package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
	"github.com/snapflowio/cdc/message/format"
)

func (s *Snapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	timeout := 5 * time.Minute
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for coordinator to initialize")
		}

		yes, status, err := s.isCoordinatorReady(ctx, slotName)
		switch {
		case err != nil:
			logger.Debug("[worker] waiting for coordinator", "status", status, "error", err)
		case yes:
			logger.Debug("[worker] coordinator ready, starting work", "status", status)
			return nil
		default:
			logger.Debug("[worker] waiting for coordinator", "status", status)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

func (s *Snapshotter) isCoordinatorReady(ctx context.Context, slotName string) (bool, string, error) {
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return false, "job not found", err
	}
	if job == nil {
		return false, "job not created yet", nil
	}

	if job.SnapshotID == "" || job.SnapshotID == "PENDING" {
		return false, "snapshot not exported yet", nil
	}

	hasChunks, err := s.hasChunksReady(ctx, slotName)
	if err != nil {
		return false, "error checking chunks", err
	}
	if !hasChunks {
		return false, "chunks not created yet", nil
	}

	return true, fmt.Sprintf("ready (job=%s, chunks=available)", job.SnapshotID), nil
}

func (s *Snapshotter) hasChunksReady(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) > 0
		FROM %s
		WHERE slot_name = '%s'
	`, chunksTableName, slotName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("check chunks ready: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	hasChunks := string(results[0].Rows[0][0]) == "t"
	return hasChunks, nil
}

func (s *Snapshotter) executeWorker(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, startTime time.Time) error {
	_ = handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeBegin,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})

	if err := s.workerProcess(ctx, slotName, instanceID, job, handler); err != nil {
		return fmt.Errorf("worker process: %w", err)
	}

	return nil
}

func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) error {
	heartbeatCtx, currentChunk := s.startHeartbeat(ctx)
	defer heartbeatCtx()

	for {
		hasMore, err := s.processNextChunk(ctx, slotName, instanceID, job, handler, currentChunk)
		if err != nil {
			return err
		}
		if !hasMore {
			logger.Debug("[worker] no more chunks available", "instanceID", instanceID)
			return nil
		}
	}
}

func (s *Snapshotter) startHeartbeat(ctx context.Context) (cancel context.CancelFunc, chunkChan chan<- int64) {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	currentChunk := make(chan int64, 1)
	go s.heartbeatWorker(heartbeatCtx, currentChunk, s.config.HeartbeatInterval)
	return cancelHeartbeat, currentChunk
}

func (s *Snapshotter) processNextChunk(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunkChan chan<- int64) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	chunk, err := s.claimNextChunk(ctx, slotName, instanceID, s.config.ClaimTimeout)
	if err != nil {
		return false, fmt.Errorf("claim next chunk: %w", err)
	}
	if chunk == nil {
		return false, nil
	}

	s.prepareChunkProcessing(instanceID, chunk, chunkChan)

	return s.executeChunkProcessing(ctx, slotName, instanceID, job, handler, chunk)
}

func (s *Snapshotter) prepareChunkProcessing(instanceID string, chunk *Chunk, chunkChan chan<- int64) {
	s.logChunkStart(instanceID, chunk)
	s.notifyHeartbeat(chunkChan, chunk.ID)
}

func (s *Snapshotter) executeChunkProcessing(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunk *Chunk) (bool, error) {
	rowsProcessed, err := s.processChunkWithTransaction(ctx, chunk, job.SnapshotID, job.SnapshotLSN, handler)
	if err != nil {
		return s.handleChunkProcessingError(ctx, instanceID, chunk, job.SnapshotID, err)
	}

	s.completeChunk(ctx, slotName, instanceID, chunk, rowsProcessed)
	return true, nil
}

func (s *Snapshotter) handleChunkProcessingError(ctx context.Context, instanceID string, chunk *Chunk, snapshotID string, err error) (bool, error) {
	if isInvalidSnapshotError(err) {
		return s.handleInvalidSnapshot(ctx, instanceID, chunk, snapshotID)
	}

	logger.Error("[worker] chunk processing failed", "chunkID", chunk.ID, "error", err)
	return true, nil
}

func (s *Snapshotter) handleInvalidSnapshot(ctx context.Context, instanceID string, chunk *Chunk, snapshotID string) (bool, error) {
	logger.Warn("[worker] invalid snapshot detected, coordinator likely restarted",
		"chunkID", chunk.ID,
		"snapshotID", snapshotID,
		"instanceID", instanceID)

	if err := s.releaseChunk(ctx, chunk.ID); err != nil {
		logger.Error("[worker] failed to release chunk after invalid snapshot",
			"chunkID", chunk.ID,
			"error", err)
	} else {
		logger.Info("[worker] chunk released back to pending", "chunkID", chunk.ID)
	}

	logger.Info("[worker] stopping worker due to invalid snapshot, will restart", "instanceID", instanceID)
	return false, ErrSnapshotInvalidated
}

func (s *Snapshotter) logChunkStart(instanceID string, chunk *Chunk) {
	args := []any{
		"instanceID", instanceID,
		"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
		"chunkIndex", chunk.ChunkIndex,
		"chunkStart", chunk.ChunkStart,
		"chunkSize", chunk.ChunkSize,
	}

	if hasRange := chunk.hasRangeBounds(); hasRange {
		args = append(args,
			"rangeStart", *chunk.RangeStart,
			"rangeEnd", *chunk.RangeEnd,
		)
	}

	logger.Debug("[worker] processing chunk", args...)
}

func (s *Snapshotter) notifyHeartbeat(chunkChan chan<- int64, chunkID int64) {
	select {
	case chunkChan <- chunkID:
	default:
	}
}

func (s *Snapshotter) completeChunk(ctx context.Context, slotName, instanceID string, chunk *Chunk, rowsProcessed int64) {
	if err := s.markChunkCompleted(ctx, slotName, chunk.ID, rowsProcessed); err != nil {
		logger.Warn("[worker] failed to mark chunk as completed", "error", err)
		return
	}

	logger.Debug("[worker] chunk completed",
		"instanceID", instanceID,
		"chunkID", chunk.ID,
		"rowsProcessed", rowsProcessed)
}

func (s *Snapshotter) processChunkWithTransaction(ctx context.Context, chunk *Chunk, snapshotID string, lsn pg.LSN, handler Handler) (int64, error) {
	var rowsProcessed int64

	err := s.retryDBOperation(ctx, func() error {
		rows, err := s.executeInTransaction(ctx, snapshotID, func(conn pg.Connection) (int64, error) {
			return s.processChunk(ctx, conn, chunk, lsn, handler)
		})
		if err != nil {
			return err
		}
		rowsProcessed = rows
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("process chunk with transaction: %w", err)
	}

	return rowsProcessed, nil
}

func (s *Snapshotter) executeInTransaction(ctx context.Context, snapshotID string, fn func(pg.Connection) (int64, error)) (int64, error) {
	chunkConn, err := s.connectionPool.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("get connection from pool: %w", err)
	}
	defer s.connectionPool.Put(chunkConn)

	tx := &snapshotTransaction{
		snapshotter: s,
		ctx:         ctx,
		snapshotID:  snapshotID,
		conn:        chunkConn,
	}

	if err := tx.begin(); err != nil {
		return 0, err
	}
	defer tx.rollbackIfNeeded()

	rows, err := fn(chunkConn)
	if err != nil {
		return 0, fmt.Errorf("execute function: %w", err)
	}

	if err := tx.commit(); err != nil {
		return 0, err
	}

	return rows, nil
}

type snapshotTransaction struct {
	ctx         context.Context
	conn        pg.Connection
	snapshotter *Snapshotter
	snapshotID  string
	committed   bool
}

func (tx *snapshotTransaction) begin() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := tx.snapshotter.setTransactionSnapshot(tx.ctx, tx.conn, tx.snapshotID); err != nil {
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
		return fmt.Errorf("set transaction snapshot: %w", err)
	}

	return nil
}

func (tx *snapshotTransaction) commit() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "COMMIT"); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	tx.committed = true
	return nil
}

func (tx *snapshotTransaction) rollbackIfNeeded() {
	if !tx.committed {
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
	}
}

func (s *Snapshotter) heartbeatWorker(ctx context.Context, currentChunk <-chan int64, interval time.Duration) {
	var activeChunkID int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case chunkID := <-currentChunk:
			activeChunkID = chunkID
		case <-ticker.C:
			if activeChunkID > 0 {
				if err := s.updateChunkHeartbeat(ctx, activeChunkID); err != nil {
					logger.Warn("[heartbeat] failed to update", "chunkID", activeChunkID, "error", err)
				} else {
					logger.Debug("[heartbeat] updated", "chunkID", activeChunkID)
				}
			}
		}
	}
}

func (s *Snapshotter) markJobAsCompleted(ctx context.Context, slotName string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET completed = true
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return fmt.Errorf("mark job as completed: %w", err)
		}

		logger.Info("[metadata] job marked as completed", "slotName", slotName)
		return nil
	})
}

func (s *Snapshotter) claimNextChunk(ctx context.Context, slotName, instanceID string, claimTimeout time.Duration) (*Chunk, error) {
	var chunk *Chunk

	err := s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := s.buildClaimChunkQuery(slotName, instanceID, now, claimTimeout)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return fmt.Errorf("claim chunk: %w", err)
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			chunk = nil
			return nil
		}

		row := results[0].Rows[0]
		if len(row) < 13 {
			return fmt.Errorf("invalid chunk row")
		}

		chunk, err = s.parseClaimedChunk(row, slotName, instanceID, now)
		return err
	})

	return chunk, err
}

func (s *Snapshotter) buildClaimChunkQuery(slotName, instanceID string, now time.Time, claimTimeout time.Duration) string {
	timeoutThreshold := now.Add(-claimTimeout)
	return fmt.Sprintf(`
		WITH available_chunk AS (
			SELECT id FROM %s
			WHERE slot_name = '%s'
			  AND (
				  status = 'pending'
				  OR (status = 'in_progress' AND heartbeat_at < '%s')
			  )
			ORDER BY chunk_index
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s c
		SET status = 'in_progress',
		    claimed_by = '%s',
		    claimed_at = '%s',
		    heartbeat_at = '%s'
		FROM available_chunk
		WHERE c.id = available_chunk.id
		RETURNING c.id, c.table_schema, c.table_name,
		          c.chunk_index, c.chunk_start, c.chunk_size, c.range_start, c.range_end,
		          c.range_start_text, c.range_end_text, c.pk_column, c.use_ctid, c.rows_processed
	`, chunksTableName,
		slotName,
		timeoutThreshold.Format(postgresTimestampFormat),
		chunksTableName,
		instanceID,
		now.Format(postgresTimestampFormat),
		now.Format(postgresTimestampFormat),
	)
}

func (s *Snapshotter) parseClaimedChunk(row [][]byte, slotName, instanceID string, now time.Time) (*Chunk, error) {
	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}

	if _, err := fmt.Sscanf(string(row[0]), "%d", &chunk.ID); err != nil {
		return nil, fmt.Errorf("parse chunk ID: %w", err)
	}
	chunk.TableSchema = string(row[1])
	chunk.TableName = string(row[2])
	if _, err := fmt.Sscanf(string(row[3]), "%d", &chunk.ChunkIndex); err != nil {
		return nil, fmt.Errorf("parse chunk index: %w", err)
	}
	if _, err := fmt.Sscanf(string(row[4]), "%d", &chunk.ChunkStart); err != nil {
		return nil, fmt.Errorf("parse chunk start: %w", err)
	}
	if _, err := fmt.Sscanf(string(row[5]), "%d", &chunk.ChunkSize); err != nil {
		return nil, fmt.Errorf("parse chunk size: %w", err)
	}
	rangeStart, err := parseNullableInt64(row[6])
	if err != nil {
		return nil, fmt.Errorf("parse range start: %w", err)
	}
	rangeEnd, err := parseNullableInt64(row[7])
	if err != nil {
		return nil, fmt.Errorf("parse range end: %w", err)
	}
	chunk.RangeStart = rangeStart
	chunk.RangeEnd = rangeEnd

	chunk.RangeStartText = parseNullableString(row[8])
	chunk.RangeEndText = parseNullableString(row[9])
	chunk.PKColumn = string(row[10])

	useCtid, err := parseNullableBool(row[11])
	if err != nil {
		return nil, fmt.Errorf("parse use_ctid: %w", err)
	}
	chunk.UseCtid = useCtid

	return chunk, nil
}

func (s *Snapshotter) updateChunkHeartbeat(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := fmt.Sprintf(`
			UPDATE %s SET heartbeat_at = '%s' WHERE id = %d
		`, chunksTableName, now.Format(postgresTimestampFormat), chunkID)

		_, err := s.execQuery(ctx, s.healthcheckConn, query)
		return err
	})
}

func (s *Snapshotter) markChunkCompleted(ctx context.Context, slotName string, chunkID, rowsProcessed int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()

		chunkQuery := fmt.Sprintf(`
			UPDATE %s
			SET status = 'completed',
			    completed_at = '%s',
			    rows_processed = %d
			WHERE id = %d
		`, chunksTableName, now.Format(postgresTimestampFormat), rowsProcessed, chunkID)

		if _, err := s.execQuery(ctx, s.metadataConn, chunkQuery); err != nil {
			return fmt.Errorf("update chunk status: %w", err)
		}

		jobQuery := fmt.Sprintf(`
			UPDATE %s
			SET completed_chunks = completed_chunks + 1
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, jobQuery); err != nil {
			return fmt.Errorf("increment completed chunks: %w", err)
		}

		return nil
	})
}

func (s *Snapshotter) releaseChunk(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET status = 'pending',
			    claimed_by = NULL,
			    claimed_at = NULL,
			    heartbeat_at = NULL
			WHERE id = %d
		`, chunksTableName, chunkID)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return fmt.Errorf("release chunk: %w", err)
		}

		return nil
	})
}
