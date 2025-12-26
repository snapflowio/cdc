package slot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
)

var (
	ErrSlotNotExists = errors.New("slot does not exist")
	ErrNotConnected  = errors.New("slot is not connected")
	ErrSlotClosed    = errors.New("slot is closed")
)

var typeMap = pgtype.NewMap()

type XLogUpdater interface {
	UpdateXLogPos(l pg.LSN)
}

type Slot struct {
	// Configuration
	cfg       Config
	statusSQL string

	// Dependencies
	conn            pg.Connection
	replicationConn pg.Connection
	logUpdater      XLogUpdater
	ticker          *time.Ticker

	// Synchronization (always last)
	mu     sync.Mutex
	closed atomic.Bool
}

func NewSlot(replicationDSN, standardDSN string, cfg Config, updater XLogUpdater) *Slot {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn FROM pg_replication_slots WHERE slot_name = '%s';", cfg.Name)

	return &Slot{
		cfg:             cfg,
		conn:            pg.NewConnectionTemplate(standardDSN),
		replicationConn: pg.NewConnectionTemplate(replicationDSN),
		statusSQL:       query,
		ticker:          time.NewTicker(time.Millisecond * cfg.SlotActivityCheckerInterval),
		logUpdater:      updater,
	}
}

func (s *Slot) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Connect(ctx)
}

func (s *Slot) Create(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return nil, fmt.Errorf("slot connect: %w", err)
	}

	defer func() {
		_ = s.conn.Close(ctx)
	}()

	info, err := s.infoLocked(ctx)
	if err != nil {
		if !errors.Is(err, ErrSlotNotExists) || !s.cfg.CreateIfNotExists {
			return nil, fmt.Errorf("replication slot info: %w", err)
		}
	} else {
		logger.Warn("replication slot already exists")
		return info, nil
	}

	if err := s.createSlotWithReplicationConn(ctx); err != nil {
		return nil, err
	}

	logger.Info("replication slot created", "name", s.cfg.Name)

	return s.infoLocked(ctx)
}

func (s *Slot) createSlotWithReplicationConn(ctx context.Context) error {
	if err := s.replicationConn.Connect(ctx); err != nil {
		return fmt.Errorf("slot replication connect: %w", err)
	}

	defer func() {
		_ = s.replicationConn.Close(ctx)
	}()

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	resultReader := s.replicationConn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return fmt.Errorf("replication slot create result: %w", err)
	}

	if err = resultReader.Close(); err != nil {
		return fmt.Errorf("replication slot create result reader close: %w", err)
	}

	return nil
}

func (s *Slot) Info(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil, ErrSlotClosed
	}

	return s.infoLocked(ctx)
}

func (s *Slot) infoLocked(ctx context.Context) (*Info, error) {
	resultReader := s.conn.Exec(ctx, s.statusSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("replication slot info result: %w", err)
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrSlotNotExists
	}

	slotInfo, err := decodeSlotInfoResult(results[0])
	if err != nil {
		return nil, fmt.Errorf("replication slot info result decode: %w", err)
	}

	if slotInfo.Type != Logical {
		return nil, fmt.Errorf("'%s' replication slot must be logical but it is %s", slotInfo.Name, slotInfo.Type)
	}

	return slotInfo, nil
}

func (s *Slot) Close(ctx context.Context) {
	s.closed.Store(true)
	s.ticker.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
	}
}

func decodeSlotInfoResult(result *pgconn.Result) (*Info, error) {
	var slotInfo Info
	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "slot_name":
			slotInfo.Name = v.(string)
		case "slot_type":
			slotInfo.Type = Type(v.(string))
		case "active":
			slotInfo.Active = v.(bool)
		case "active_pid":
			slotInfo.ActivePID = v.(int32)
		case "restart_lsn":
			slotInfo.RestartLSN, _ = pg.ParseLSN(v.(string))
		case "confirmed_flush_lsn":
			slotInfo.ConfirmedFlushLSN, _ = pg.ParseLSN(v.(string))
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			slotInfo.CurrentLSN, _ = pg.ParseLSN(v.(string))
		}
	}

	slotInfo.RetainedWALSize = slotInfo.CurrentLSN - slotInfo.RestartLSN
	slotInfo.Lag = slotInfo.CurrentLSN - slotInfo.ConfirmedFlushLSN

	return &slotInfo, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}
