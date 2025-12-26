package snapshot

import (
	"context"
	"fmt"
	"strings"

	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
)

func (s *Snapshotter) exportSnapshot(ctx context.Context, exportSnapshotConn pg.Connection) (string, error) {
	var snapshotID string

	err := s.retryDBOperation(ctx, func() error {
		results, err := s.execQuery(ctx, exportSnapshotConn, "SELECT pg_export_snapshot()")
		if err != nil {
			if strings.Contains(err.Error(), "permission denied") {
				return fmt.Errorf("pg_export_snapshot requires REPLICATION privilege. Run: ALTER USER your_user WITH REPLICATION")
			}

			if strings.Contains(err.Error(), "wal_level") {
				return fmt.Errorf("pg_export_snapshot requires wal_level='logical'. Set in postgresql.conf and restart")
			}

			return fmt.Errorf("export snapshot: %w", err)
		}

		if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
			return fmt.Errorf("no snapshot ID returned")
		}

		snapshotID = string(results[0].Rows[0][0])

		return nil
	})

	return snapshotID, err
}

func (s *Snapshotter) setTransactionSnapshot(ctx context.Context, conn pg.Connection, snapshotID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID)
		if err := s.execSQL(ctx, conn, query); err != nil {
			return fmt.Errorf("set transaction snapshot: %w", err)
		}

		logger.Debug("[worker] transaction snapshot set", "snapshotID", snapshotID)
		return nil
	})
}
