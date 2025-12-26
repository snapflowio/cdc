package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/internal/pg"
)

type ChunkStatus string

const (
	ChunkStatusPending    ChunkStatus = "pending"
	ChunkStatusInProgress ChunkStatus = "in_progress"
	ChunkStatusCompleted  ChunkStatus = "completed"
)

type Chunk struct {
	ClaimedAt      *time.Time
	HeartbeatAt    *time.Time
	CompletedAt    *time.Time
	RangeEnd       *int64
	RangeStart     *int64
	RangeEndText   *string
	RangeStartText *string
	Status         ChunkStatus
	TableName      string
	ClaimedBy      string
	TableSchema    string
	SlotName       string
	PKColumn       string
	ID             int64
	ChunkIndex     int
	ChunkStart     int64
	ChunkSize      int64
	UseCtid        bool
}

func (c *Chunk) hasRangeBounds() bool {
	return c.RangeStart != nil && c.RangeEnd != nil
}

func (c *Chunk) hasTextRangeBounds() bool {
	return c.RangeStartText != nil && c.RangeEndText != nil
}

type Job struct {
	StartedAt       time.Time
	SlotName        string
	SnapshotID      string
	SnapshotLSN     pg.LSN
	TotalChunks     int
	CompletedChunks int
	Completed       bool
}

const (
	jobTableName    = "cdc_snapshot_job"
	chunksTableName = "cdc_snapshot_chunks"
)

func (s *Snapshotter) loadJob(ctx context.Context, slotName string) (*Job, error) {
	var job *Job

	err := s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			SELECT slot_name, snapshot_id, snapshot_lsn, started_at,
			       completed, total_chunks, completed_chunks
			FROM %s WHERE slot_name = '%s'
		`, jobTableName, slotName)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return fmt.Errorf("load job: %w", err)
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			job = nil
			return nil
		}

		row := results[0].Rows[0]
		if len(row) < 7 {
			return fmt.Errorf("invalid job row")
		}

		job = &Job{
			SlotName:   string(row[0]),
			SnapshotID: string(row[1]),
		}

		job.SnapshotLSN, err = pg.ParseLSN(string(row[2]))
		if err != nil {
			return fmt.Errorf("parse snapshot LSN: %w", err)
		}

		job.StartedAt, err = parseTimestamp(string(row[3]))
		if err != nil {
			return fmt.Errorf("parse started_at timestamp: %w", err)
		}

		job.Completed = string(row[4]) == "t" || string(row[4]) == "true"
		if _, err := fmt.Sscanf(string(row[5]), "%d", &job.TotalChunks); err != nil {
			return fmt.Errorf("parse total chunks: %w", err)
		}

		if _, err := fmt.Sscanf(string(row[6]), "%d", &job.CompletedChunks); err != nil {
			return fmt.Errorf("parse completed chunks: %w", err)
		}

		return nil
	})

	return job, err
}

func (s *Snapshotter) LoadJob(ctx context.Context, slotName string) (*Job, error) {
	return s.loadJob(ctx, slotName)
}

func (s *Snapshotter) checkJobCompleted(ctx context.Context, slotName string) (bool, error) {
	var isCompleted bool

	err := s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			SELECT
				COUNT(*) as total,
				COUNT(*) FILTER (WHERE status = 'completed') as completed
			FROM %s
			WHERE slot_name = '%s'
		`, chunksTableName, slotName)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return fmt.Errorf("check job completed: %w", err)
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			isCompleted = false
			return nil
		}

		row := results[0].Rows[0]
		var total, completed int
		if _, err := fmt.Sscanf(string(row[0]), "%d", &total); err != nil {
			return fmt.Errorf("parse total count: %w", err)
		}
		if _, err := fmt.Sscanf(string(row[1]), "%d", &completed); err != nil {
			return fmt.Errorf("parse completed count: %w", err)
		}

		isCompleted = total > 0 && total == completed
		return nil
	})

	return isCompleted, err
}
