package snapshot

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/snapflowio/cdc/config"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
	"github.com/snapflowio/cdc/message/format"
	"github.com/snapflowio/cdc/publication"
)

var ErrSnapshotInvalidated = fmt.Errorf("snapshot invalidated by coordinator restart")

type Handler func(event *format.Snapshot) error

type Snapshotter struct {
	metadataConn       pg.Connection
	healthcheckConn    pg.Connection
	exportSnapshotConn pg.Connection
	connectionPool     *ConnectionPool
	decoderCache       *DecoderCache
	typeMap            *pgtype.Map
	orderByCache       map[string]orderByCacheEntry
	dsn                string
	tables             publication.Tables
	config             config.SnapshotConfig
	orderByMu          sync.RWMutex
}

type orderByCacheEntry struct {
	clause  string
	columns []string
}

func New(ctx context.Context, snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string) (*Snapshotter, error) {
	metadataConn, err := pg.NewConnection(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create metadata connection: %w", err)
	}

	healthcheckConn, err := pg.NewConnection(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create healthcheck connection: %w", err)
	}

	connectionPool, err := NewConnectionPool(ctx, dsn, 5)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	decoderCache := NewDecoderCache()

	return &Snapshotter{
		dsn:             dsn,
		metadataConn:    metadataConn,
		healthcheckConn: healthcheckConn,
		connectionPool:  connectionPool,
		decoderCache:    decoderCache,
		config:          snapshotConfig,
		tables:          tables,
		typeMap:         pgtype.NewMap(),
		orderByCache:    make(map[string]orderByCacheEntry),
	}, nil
}

func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] preparing", "instanceID", instanceID)

	isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return fmt.Errorf("setup job: %w", err)
	}

	if isCoordinator {
		logger.Debug("[coordinator] snapshot transaction kept OPEN - replication slot must be created NOW")
	}
	return nil
}

func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return fmt.Errorf("job not found - Prepare() must be called first")
	}

	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return fmt.Errorf("execute worker: %w", err)
	}

	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return fmt.Errorf("finalize snapshot: %w", err)
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return fmt.Errorf("check job completed: %w", err)
	}

	if !allCompleted {
		return nil
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")

	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("[snapshot] failed to mark job as completed", "error", err)
		return err
	}

	s.closeAllConnections(ctx, true)

	return handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})
}

func (s *Snapshotter) closeAllConnections(ctx context.Context, commitExport bool) {
	logger.Info("[snapshot] closing all connections")

	if s.exportSnapshotConn != nil {
		s.closeExportSnapshotConnection(ctx, commitExport)
	}

	if s.connectionPool != nil {
		s.connectionPool.Close(ctx)
		s.connectionPool = nil
	}

	if s.metadataConn != nil {
		if err := s.metadataConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing metadata connection", "error", err)
		}
		s.metadataConn = nil
	}

	if s.healthcheckConn != nil {
		if err := s.healthcheckConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing healthcheck connection", "error", err)
		}
		s.healthcheckConn = nil
	}

	logger.Info("[snapshot] all connections closed")
}

func (s *Snapshotter) closeExportSnapshotConnection(ctx context.Context, commit bool) {
	if commit {
		logger.Info("[coordinator] committing and closing snapshot export connection")
		if err := s.execSQL(ctx, s.exportSnapshotConn, "COMMIT"); err != nil {
			logger.Warn("[coordinator] failed to commit snapshot transaction, attempting rollback", "error", err)
			if rollbackErr := s.execSQL(ctx, s.exportSnapshotConn, "ROLLBACK"); rollbackErr != nil {
				logger.Error("[coordinator] failed to rollback snapshot transaction", "error", rollbackErr)
			}
		}
	} else {
		logger.Info("[coordinator] rolling back and closing snapshot export connection")
		if err := s.execSQL(ctx, s.exportSnapshotConn, "ROLLBACK"); err != nil {
			logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
		}
	}

	if err := s.exportSnapshotConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}
	s.exportSnapshotConn = nil
}

func (s *Snapshotter) Close(ctx context.Context) {
	if s == nil {
		return
	}
	logger.Debug("[snapshot] closing snapshotter (fallback cleanup)")
	s.closeAllConnections(ctx, false)
}

func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (any, error) {
	decoder := s.decoderCache.Get(dataTypeOID)
	return decoder.Decode(s.typeMap, data)
}

func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
