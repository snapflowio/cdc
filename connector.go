package cdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snapflowio/cdc/config"
	"github.com/snapflowio/cdc/internal/pg"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/message/format"
	"github.com/snapflowio/cdc/publication"
	"github.com/snapflowio/cdc/replication"
	"github.com/snapflowio/cdc/slot"
	"github.com/snapflowio/cdc/snapshot"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config

	// Dynamic table management
	AddTable(ctx context.Context, table publication.Table) error
	RemoveTable(ctx context.Context, schema, tableName string) error
	UpdateTableReplicaIdentity(ctx context.Context, table publication.Table) error
	ListTables(ctx context.Context) (publication.Tables, error)
}

type connector struct {
	// Configuration and dependencies
	cfg          *config.Config
	listenerFunc replication.ListenerFunc

	// Connections and streams
	heartbeatConn pg.Connection
	stream        replication.Streamer
	slot          *slot.Slot
	snapshotter   *snapshot.Snapshotter

	// Channels
	cancelCh chan os.Signal
	readyCh  chan struct{}

	// Synchronization (always last)
	once         sync.Once
	cancelChOnce sync.Once
	readyChOnce  sync.Once
	heartbeatMu  sync.Mutex
	configMu     sync.RWMutex
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}
	cfg.Print()

	logger.SetLevel(cfg.Logger.LogLevel)

	if cfg.IsSnapshotOnlyMode() {
		return newSnapshotOnlyConnector(ctx, cfg, listenerFunc)
	}

	conn, err := pg.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	publicationInfo, err := initializePublication(ctx, cfg, conn)
	if err != nil {
		return nil, err
	}
	logger.Info("publication", "info", publicationInfo)

	conn.Close(ctx)

	snapshotTables, err := cfg.GetSnapshotTables(publicationInfo)
	if err != nil {
		return nil, fmt.Errorf("get snapshot tables: %w", err)
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables)
	if err != nil {
		return nil, err
	}

	stream := replication.NewStream(cfg.ReplicationDSN(), cfg, listenerFunc)

	sl := slot.NewSlot(cfg.ReplicationDSN(), cfg.DSN(), cfg.Slot, stream.(slot.XLogUpdater))

	heartbeatConn, err := pg.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("create heartbeat connection: %w", err)
	}

	return &connector{
		cfg:           &cfg,
		stream:        stream,
		slot:          sl,
		heartbeatConn: heartbeatConn,
		snapshotter:   snapshotter,
		listenerFunc:  listenerFunc,
		cancelCh:      make(chan os.Signal, 1),
		readyCh:       make(chan struct{}, 1),
	}, nil
}

func newSnapshotOnlyConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	snapshotTables, err := cfg.GetSnapshotTables(nil)
	if err != nil {
		return nil, fmt.Errorf("get snapshot tables: %w", err)
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables)
	if err != nil {
		return nil, err
	}

	logger.Info("snapshot-only mode enabled", "tables", len(snapshotTables))

	return &connector{
		cfg:          &cfg,
		snapshotter:  snapshotter,
		listenerFunc: listenerFunc,
		cancelCh:     make(chan os.Signal, 1),
		readyCh:      make(chan struct{}, 1),
	}, nil
}

func initializePublication(ctx context.Context, cfg config.Config, conn pg.Connection) (*publication.Config, error) {
	pub := publication.New(cfg.Publication, conn)
	if err := pub.SetReplicaIdentities(ctx); err != nil {
		return nil, err
	}
	return pub.Create(ctx)
}

func initializeSnapshot(ctx context.Context, cfg config.Config, tables publication.Tables) (*snapshot.Snapshotter, error) {
	if !cfg.Snapshot.Enabled {
		return nil, nil
	}
	return snapshot.New(ctx, cfg.Snapshot, tables, cfg.DSN())
}

func (c *connector) Start(ctx context.Context) {
	if c.cfg.IsSnapshotOnlyMode() {
		if !c.shouldTakeSnapshotOnly(ctx) {
			logger.Info("snapshot-only already completed, exiting")
			return
		}

		if err := c.executeSnapshotOnly(ctx); err != nil {
			logger.Error("snapshot-only execution failed", "error", err)
			return
		}
		logger.Info("snapshot-only completed successfully, exiting")
		return
	}

	if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
		if err := c.prepareSnapshotAndSlot(ctx); err != nil {
			logger.Error("snapshot preparation failed", "error", err)
			return
		}
	} else {
		logger.Info("creating replication slot for CDC")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			logger.Error("slot creation failed", "error", err)
			return
		}
		logger.Info("slot info", "info", slotInfo)
	}

	if err := c.slot.Connect(ctx); err != nil {
		logger.Error("slot connection failed", "error", err)
		return
	}

	c.CaptureSlot(ctx)

	if err := c.stream.Connect(ctx); err != nil {
		logger.Error("stream connection failed", "error", err)
		return
	}

	err := c.stream.Open(ctx)
	if err != nil {
		if errors.Is(err, replication.ErrSlotInUse) {
			logger.Info("capture failed")
			c.Start(ctx)
			return
		}
		logger.Error("postgres stream open", "error", err)
		return
	}

	logger.Info("slot captured")
	go c.runHeartbeat(ctx)

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	<-c.cancelCh
	logger.Debug("cancel channel triggered")
}

func (c *connector) shouldTakeSnapshot(ctx context.Context) bool {
	if !c.cfg.Snapshot.Enabled {
		return false
	}

	switch c.cfg.Snapshot.Mode {
	case config.SnapshotModeNever:
		return false
	case config.SnapshotModeInitial:
		job, err := c.snapshotter.LoadJob(ctx, c.cfg.Slot.Name)
		if err != nil {
			logger.Debug("failed to load snapshot job state, will take snapshot", "error", err)
			return true
		}
		return job == nil || !job.Completed
	default:
		logger.Warn("invalid snapshot mode, skipping snapshot", "mode", c.cfg.Snapshot.Mode)
		return false
	}
}

func (c *connector) prepareSnapshotAndSlot(ctx context.Context) error {
	return c.retryOperation("snapshot", 3, func(_ int) error {
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		logger.Debug("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

		err = c.snapshotter.Prepare(ctx, c.cfg.Slot.Name)
		if err != nil {
			return fmt.Errorf("prepare snapshot: %w", err)
		}
		c.stream.OpenFromSnapshotLSN()

		if err := c.executeSnapshotWithRetry(ctx); err != nil {
			if c.isSnapshotInvalidationError(err) {
				logger.Error("snapshot invalidated", "error", err)
				return err
			}
			return fmt.Errorf("execute snapshot: %w", err)
		}

		logger.Info("snapshot completed successfully")
		return nil
	})
}

func (c *connector) executeSnapshotOnly(ctx context.Context) error {
	slotName := c.getSnapshotOnlySlotName()

	logger.Info("starting snapshot-only execution", "slotName", slotName)

	err := c.snapshotter.Prepare(ctx, slotName)
	if err != nil {
		return fmt.Errorf("prepare snapshot: %w", err)
	}

	if err := c.snapshotter.Execute(ctx, c.snapshotHandler, slotName); err != nil {
		return fmt.Errorf("execute snapshot: %w", err)
	}

	logger.Info("snapshot data collection completed")
	return nil
}

func (c *connector) getSnapshotOnlySlotName() string {
	return fmt.Sprintf("snapshot_only_%s", c.cfg.Database)
}

func (c *connector) shouldTakeSnapshotOnly(ctx context.Context) bool {
	slotName := c.getSnapshotOnlySlotName()

	job, err := c.snapshotter.LoadJob(ctx, slotName)
	if err != nil {
		logger.Debug("failed to load snapshot job, will take snapshot", "error", err)
		return true
	}

	if job == nil || !job.Completed {
		return true
	}

	logger.Info("snapshot-only already completed, skipping", "slotName", slotName)
	return false
}

func (c *connector) executeSnapshotWithRetry(ctx context.Context) error {
	const (
		maxRetries        = 5
		initialDelay      = 10 * time.Second
		maxDelay          = 60 * time.Second
		backoffMultiplier = 2
	)

	retryDelay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.snapshotter.Execute(ctx, c.snapshotHandler, c.cfg.Slot.Name)
		if err == nil {
			return nil
		}

		if !c.isSnapshotInvalidationError(err) {
			return err
		}

		if attempt >= maxRetries {
			return fmt.Errorf("snapshot execution failed after maximum retries: %w", err)
		}

		c.logRetryAttempt(attempt, maxRetries, retryDelay)

		if waitErr := c.waitWithContext(ctx, retryDelay); waitErr != nil {
			return waitErr
		}

		retryDelay = c.calculateNextDelay(retryDelay, maxDelay, backoffMultiplier)
	}

	return errors.New("snapshot execution failed: unexpected exit from retry loop")
}

func (c *connector) isSnapshotInvalidationError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, snapshot.ErrSnapshotInvalidated) {
		return true
	}

	return strings.Contains(err.Error(), "snapshot invalidated")
}

func (c *connector) logRetryAttempt(attempt, maxRetries int, delay time.Duration) {
	logger.Warn("[snapshot] snapshot invalidated, coordinator likely restarted",
		"attempt", attempt,
		"maxRetries", maxRetries,
		"waitTime", delay)

	logger.Info("[snapshot] waiting for coordinator to create new snapshot",
		"retryIn", delay)
}

func (c *connector) waitWithContext(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

func (c *connector) calculateNextDelay(currentDelay, maxDelay time.Duration, multiplier int) time.Duration {
	nextDelay := currentDelay * time.Duration(multiplier)
	if nextDelay > maxDelay {
		return maxDelay
	}
	return nextDelay
}

func (c *connector) retryOperation(operationName string, maxRetries int, operation func(attempt int) error) error {
	var lastErr error
	retryDelay := 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			logger.Info("retrying operation", "operation", operationName, "attempt", attempt, "maxRetries", maxRetries)
		}

		if err := operation(attempt); err != nil {
			lastErr = err
			logger.Warn("operation failed", "operation", operationName, "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("waiting before retry", "retryDelay", retryDelay.String())
				time.Sleep(retryDelay)
			}
			continue
		}

		return nil
	}

	return fmt.Errorf("%s failed after %d retries: %w", operationName, maxRetries, lastErr)
}

func (c *connector) snapshotHandler(event *format.Snapshot) error {
	c.listenerFunc(&replication.ListenerContext{
		Message: event,
		Ack: func() error {
			return nil
		},
	})
	return nil
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Debug("[connector] closing connector")

	c.cancelChOnce.Do(func() {
		close(c.cancelCh)
	})
	c.readyChOnce.Do(func() {
		close(c.readyCh)
	})

	if c.snapshotter != nil {
		c.snapshotter.Close(ctx)
	}

	if c.slot != nil {
		c.slot.Close(ctx)
	}
	c.closeHeartbeatConn(ctx)
	if c.stream != nil {
		c.stream.Close(ctx)
	}

	logger.Info("[connector] connector closed successfully")
}

func (c *connector) GetConfig() *config.Config {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	return c.cfg
}

func (c *connector) CaptureSlot(ctx context.Context) {
	logger.Info("slot capturing...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		info, err := c.slot.Info(ctx)
		if err != nil {
			logger.Warn("slot info failed on capture slot", "error", err)
			continue
		}

		if info.Active {
			continue
		}

		logger.Debug("capture slot", "slotInfo", info)
		break
	}
}

func (c *connector) runHeartbeat(ctx context.Context) {
	interval := 100 * time.Millisecond
	logger.Debug("heartbeat loop started", "interval", interval, "table", c.cfg.Heartbeat.Table.Name)

	// Create heartbeat table if it doesn't exist
	if err := c.createHeartbeatTableIfNotExists(ctx); err != nil {
		logger.Error("failed to create heartbeat table", "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("heartbeat loop stopped", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := c.execHeartbeat(ctx); err != nil {
				logger.Error("heartbeat execution failed", "error", err)
			} else {
				logger.Debug("heartbeat query executed")
			}
		}
	}
}

func (c *connector) createHeartbeatTableIfNotExists(ctx context.Context) error {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()

	if c.heartbeatConn == nil {
		conn, err := pg.NewConnection(ctx, c.cfg.DSN())
		if err != nil {
			return fmt.Errorf("heartbeat connection establish failed: %w", err)
		}
		c.heartbeatConn = conn
	}

	schema := c.cfg.Heartbeat.Table.Schema
	if schema == "" {
		schema = c.cfg.Schema
	}
	tableName := c.cfg.Heartbeat.Table.Name

	// Create table if not exists
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id SERIAL PRIMARY KEY,
			last_heartbeat TIMESTAMPTZ DEFAULT NOW()
		)
	`, schema, tableName)

	resultReader := c.heartbeatConn.Exec(ctx, createTableQuery)
	if resultReader == nil {
		return fmt.Errorf("create heartbeat table exec returned nil resultReader")
	}

	if _, err := resultReader.ReadAll(); err != nil {
		_ = resultReader.Close()
		return fmt.Errorf("create heartbeat table failed: %w", err)
	}

	if err := resultReader.Close(); err != nil {
		return fmt.Errorf("create heartbeat table result reader close failed: %w", err)
	}

	logger.Info("heartbeat table ensured", "schema", schema, "table", tableName)

	// Insert initial row if table is empty
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s (last_heartbeat)
		SELECT NOW()
		WHERE NOT EXISTS (SELECT 1 FROM %s.%s)
	`, schema, tableName, schema, tableName)

	resultReader = c.heartbeatConn.Exec(ctx, insertQuery)
	if resultReader == nil {
		return fmt.Errorf("insert initial heartbeat row exec returned nil resultReader")
	}

	if _, err := resultReader.ReadAll(); err != nil {
		_ = resultReader.Close()
		return fmt.Errorf("insert initial heartbeat row failed: %w", err)
	}

	if err := resultReader.Close(); err != nil {
		return fmt.Errorf("insert initial heartbeat row result reader close failed: %w", err)
	}

	logger.Debug("heartbeat table initialized", "schema", schema, "table", tableName)

	return nil
}

func (c *connector) execHeartbeat(ctx context.Context) error {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()

	if c.heartbeatConn == nil {
		conn, err := pg.NewConnection(ctx, c.cfg.DSN())
		if err != nil {
			return fmt.Errorf("heartbeat connection (re)establish failed: %w", err)
		}
		c.heartbeatConn = conn
	}

	schema := c.cfg.Heartbeat.Table.Schema
	if schema == "" {
		schema = c.cfg.Schema
	}
	tableName := c.cfg.Heartbeat.Table.Name

	query := fmt.Sprintf(`
		UPDATE %s.%s
		SET last_heartbeat = NOW()
		WHERE id = (SELECT MIN(id) FROM %s.%s)
	`, schema, tableName, schema, tableName)

	resultReader := c.heartbeatConn.Exec(ctx, query)
	if resultReader == nil {
		return fmt.Errorf("heartbeat exec returned nil resultReader")
	}
	defer func() {
		if err := resultReader.Close(); err != nil {
			logger.Error("heartbeat result reader close failed", "error", err)
		}
	}()

	if _, err := resultReader.ReadAll(); err != nil {
		_ = c.heartbeatConn.Close(ctx)
		c.heartbeatConn = nil
		return fmt.Errorf("heartbeat query failed: %w", err)
	}

	return nil
}

func (c *connector) closeHeartbeatConn(ctx context.Context) {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()

	if c.heartbeatConn != nil {
		_ = c.heartbeatConn.Close(ctx)
		c.heartbeatConn = nil
	}
}

func (c *connector) AddTable(ctx context.Context, table publication.Table) error {
	if c.cfg.IsSnapshotOnlyMode() {
		return errors.New("cannot add tables in snapshot-only mode")
	}

	c.configMu.RLock()
	if table.Schema == "" || table.Schema == "public" {
		table.Schema = c.cfg.Schema
	}
	c.configMu.RUnlock()

	conn, err := pg.NewConnection(ctx, c.cfg.DSN())
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	defer conn.Close(ctx)

	pub := publication.New(c.cfg.Publication, conn)

	if err := pub.AlterTableReplicaIdentity(ctx, table); err != nil {
		return fmt.Errorf("set replica identity: %w", err)
	}

	if err := pub.AddTable(ctx, table); err != nil {
		return fmt.Errorf("add table to publication: %w", err)
	}

	c.configMu.Lock()
	c.cfg.Publication.Tables = append(c.cfg.Publication.Tables, table)
	c.configMu.Unlock()

	logger.Info("table added to CDC", "schema", table.Schema, "table", table.Name, "replicaIdentity", table.ReplicaIdentity)

	return nil
}

func (c *connector) RemoveTable(ctx context.Context, schema, tableName string) error {
	if c.cfg.IsSnapshotOnlyMode() {
		return errors.New("cannot remove tables in snapshot-only mode")
	}

	conn, err := pg.NewConnection(ctx, c.cfg.DSN())
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	defer conn.Close(ctx)

	pub := publication.New(c.cfg.Publication, conn)

	table := publication.Table{
		Schema: schema,
		Name:   tableName,
	}

	if err := pub.RemoveTable(ctx, table); err != nil {
		return fmt.Errorf("remove table from publication: %w", err)
	}

	c.configMu.Lock()
	newTables := make(publication.Tables, 0)
	for _, t := range c.cfg.Publication.Tables {
		if t.Schema != schema || t.Name != tableName {
			newTables = append(newTables, t)
		}
	}
	c.cfg.Publication.Tables = newTables
	c.configMu.Unlock()

	logger.Info("table removed from CDC", "schema", schema, "table", tableName)

	return nil
}

func (c *connector) UpdateTableReplicaIdentity(ctx context.Context, table publication.Table) error {
	if c.cfg.IsSnapshotOnlyMode() {
		return errors.New("cannot update table replica identity in snapshot-only mode")
	}

	c.configMu.RLock()
	if table.Schema == "" || table.Schema == "public" {
		table.Schema = c.cfg.Schema
	}
	c.configMu.RUnlock()

	conn, err := pg.NewConnection(ctx, c.cfg.DSN())
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	defer conn.Close(ctx)

	pub := publication.New(c.cfg.Publication, conn)

	if err := pub.AlterTableReplicaIdentity(ctx, table); err != nil {
		return fmt.Errorf("update replica identity: %w", err)
	}

	c.configMu.Lock()
	for i, t := range c.cfg.Publication.Tables {
		if t.Schema == table.Schema && t.Name == table.Name {
			c.cfg.Publication.Tables[i].ReplicaIdentity = table.ReplicaIdentity
			c.cfg.Publication.Tables[i].IndexName = table.IndexName
			break
		}
	}
	c.configMu.Unlock()

	logger.Info("table replica identity updated", "schema", table.Schema, "table", table.Name, "replicaIdentity", table.ReplicaIdentity)

	return nil
}

func (c *connector) ListTables(ctx context.Context) (publication.Tables, error) {
	conn, err := pg.NewConnection(ctx, c.cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("create connection: %w", err)
	}
	defer conn.Close(ctx)

	pub := publication.New(c.cfg.Publication, conn)

	info, err := pub.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("get publication info: %w", err)
	}

	return info.Tables, nil
}
