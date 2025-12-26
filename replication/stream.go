package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/snapflowio/cdc/config"
	"github.com/snapflowio/cdc/internal/slice"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
	"github.com/snapflowio/cdc/message"
	"github.com/snapflowio/cdc/message/format"
)

var (
	ErrSlotInUse    = errors.New("replication slot in use")
	ErrNotConnected = errors.New("stream is not connected")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerContext struct {
	Message any
	Ack     func() error
}

type ListenerFunc func(ctx *ListenerContext)

type Message struct {
	message  any
	walStart int64
}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pg.IdentifySystemResult
	OpenFromSnapshotLSN()
}

type stream struct {
	// Configuration and dependencies
	config       config.Config
	listenerFunc ListenerFunc

	// Connection and system info
	conn   pg.Connection
	system *pg.IdentifySystemResult

	// State
	relation            map[uint32]*format.Relation
	lastXLogPos         pg.LSN
	snapshotLSN         pg.LSN
	openFromSnapshotLSN bool

	// Channels
	messageCH chan *Message
	sinkEnd   chan struct{}

	// Synchronization (always last)
	mu          *sync.RWMutex
	sinkEndOnce sync.Once
	closed      atomic.Bool
}

func NewStream(dsn string, cfg config.Config, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         pg.NewConnectionTemplate(dsn),
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		lastXLogPos:  10,
		sinkEnd:      make(chan struct{}, 1),
		mu:           &sync.RWMutex{},
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return fmt.Errorf("stream connection: %w", err)
	}

	system, err := pg.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return fmt.Errorf("identify system: %w", err)
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrNotConnected
	}

	if err := s.setup(ctx); err != nil {
		s.sinkEnd <- struct{}{}

		var v *pgconn.PgError
		if errors.As(err, &v) && v.Code == "55006" {
			return ErrSlotInUse
		}
		return fmt.Errorf("replication setup: %w", err)
	}

	go s.sink(ctx)
	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := pg.LSN(2)
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return fmt.Errorf("fetch snapshot LSN: %w", err)
		}

		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name)
	} else {
		logger.Info("replication started from restart LSN", "slot", s.config.Slot.Name)
	}

	return nil
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	var corruptedConn bool

	for {
		msgCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Millisecond*300))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				break
			}

			if pgconn.Timeout(err) {
				err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
				if err != nil {
					logger.Error("send stand by status update", "error", err)
					break
				}

				logger.Debug("send stand by status update")
				continue
			}

			logger.Error("receive message error", "error", err)
			corruptedConn = true
			break
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			res, _ := errMsg.MarshalJSON()
			logger.Error("receive postgres wal error: " + string(res))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
			continue
		}

		var xld XLogData

		switch msg.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			continue
		case message.XLogDataByteID:
			xld, err = ParseXLogData(msg.Data[1:])
			if err != nil {
				logger.Error("parse xLog data", "error", err)
				continue
			}

			logger.Debug("wal received", "walData", string(xld.WALData), "walDataByte", slice.ConvertToInt(xld.WALData), "walStart", xld.WALStart, "walEnd", xld.ServerWALEnd, "serverTime", xld.ServerTime)

			var decodedMsg any
			decodedMsg, err = message.New(xld.WALData, xld.ServerTime, s.relation)
			if err != nil || decodedMsg == nil {
				logger.Debug("wal data message parsing error", "error", err)
				continue
			}

			s.messageCH <- &Message{
				message:  decodedMsg,
				walStart: int64(xld.WALStart),
			}
		}
	}
	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {
		s.Close(ctx)
		if corruptedConn {
			panic("corrupted connection")
		}
	}
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")

	for {
		msg, ok := <-s.messageCH
		if !ok {
			break
		}

		lCtx := &ListenerContext{
			Message: msg.message,
			Ack: func() error {
				pos := pg.LSN(msg.walStart)
				s.system.UpdateXLogPos(pos)
				logger.Debug("send stand by status update", "xLogPos", pos.String())
				return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.system.LoadXLogPos()))
			},
		}

		s.listenerFunc(lCtx)
	}
}

func (s *stream) Close(ctx context.Context) {
	s.closed.Store(true)

	<-s.sinkEnd
	s.sinkEndOnce.Do(func() {
		close(s.sinkEnd)
	})
	logger.Info("postgres message sink stopped")

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
		logger.Info("postgres connection closed")
	}
}

func (s *stream) GetSystemInfo() *pg.IdentifySystemResult {
	return s.system
}

func (s *stream) UpdateXLogPos(l pg.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pg.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

func (s *stream) fetchSnapshotLSN(ctx context.Context) (pg.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pg.LSN

	err := retry.Do(
		func() error {
			conn, err := pg.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return fmt.Errorf("create connection for snapshot LSN query: %w", err)
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed
				FROM cdc_snapshot_job
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return fmt.Errorf("execute snapshot LSN query: %w", err)
			}

			if err = resultReader.Close(); err != nil {
				return fmt.Errorf("close result reader: %w", err)
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pg.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(fmt.Errorf("parse snapshot LSN %s: %w", lsnStr, err))
			}

			return nil
		},
		retry.Attempts(0),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch snapshot LSN: %w", err)
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
}

func SendStandbyStatusUpdate(_ context.Context, conn pg.Connection, walWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.Unix()*1000000 + int64(t.Nanosecond())/1000 - microSecFromUnixEpochToY2K)
}
