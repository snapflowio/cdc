package snapshot

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
	"github.com/snapflowio/cdc/message/format"
	"github.com/snapflowio/cdc/publication"
)

type primaryKeyColumn struct {
	Name     string
	DataType string
}

func (s *Snapshotter) initializeCoordinator(ctx context.Context, slotName string, currentLSN pg.LSN) error {
	logger.Debug("[coordinator] initializing job", "slotName", slotName)

	existingJob, err := s.loadJob(ctx, slotName)
	if err == nil && existingJob != nil && !existingJob.Completed {
		logger.Warn("[coordinator] incomplete job found, restarting from scratch")
		logger.Info("[coordinator] reason: stale LSN would cause data duplication")

		if err = s.cleanupJob(ctx, slotName); err != nil {
			return fmt.Errorf("cleanup incomplete job: %w", err)
		}

		logger.Info("[coordinator] cleanup complete, starting fresh")
	}

	if err := s.createMetadata(ctx, slotName, currentLSN); err != nil {
		return fmt.Errorf("create metadata: %w", err)
	}

	if err := s.exportSnapshotTransaction(ctx); err != nil {
		return fmt.Errorf("export snapshot: %w", err)
	}

	logger.Debug("[coordinator] initialization complete")
	return nil
}

func (s *Snapshotter) createMetadata(ctx context.Context, slotName string, currentLSN pg.LSN) error {
	logger.Info("[coordinator] creating metadata and chunks")

	job := &Job{
		SlotName:    slotName,
		SnapshotID:  "PENDING",
		SnapshotLSN: currentLSN,
		StartedAt:   time.Now().UTC(),
		Completed:   false,
	}

	totalChunks := 0
	for _, table := range s.tables {
		chunks := s.createTableChunks(ctx, slotName, table)

		for _, chunk := range chunks {
			if err := s.saveChunk(ctx, chunk); err != nil {
				return fmt.Errorf("save chunk: %w", err)
			}
		}

		totalChunks += len(chunks)
		logger.Info("[coordinator] chunks created",
			"table", fmt.Sprintf("%s.%s", table.Schema, table.Name),
			"chunks", len(chunks))
	}

	job.TotalChunks = totalChunks

	if err := s.saveJob(ctx, job); err != nil {
		return fmt.Errorf("save job: %w", err)
	}

	logger.Info("[coordinator] metadata committed", "totalChunks", totalChunks, "lsn", currentLSN.String())
	return nil
}

func (s *Snapshotter) exportSnapshotTransaction(ctx context.Context) error {
	exportSnapshotConn, err := pg.NewConnection(ctx, s.dsn)
	if err != nil {
		return fmt.Errorf("create pg export snapshot connection: %w", err)
	}
	s.exportSnapshotConn = exportSnapshotConn

	logger.Info("[coordinator] exporting snapshot")

	if err := s.execSQL(ctx, exportSnapshotConn, "SET idle_in_transaction_session_timeout = 0"); err != nil {
		return fmt.Errorf("set idle timeout: %w", err)
	}

	if err := s.execSQL(ctx, exportSnapshotConn, "SET statement_timeout = 0"); err != nil {
		return fmt.Errorf("set statement timeout: %w", err)
	}

	if err := s.execSQL(ctx, exportSnapshotConn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}

	go s.snapshotTransactionKeepalive(ctx, exportSnapshotConn)

	snapshotID, err := s.exportSnapshot(ctx, exportSnapshotConn)
	if err != nil {
		_ = s.execSQL(ctx, exportSnapshotConn, "ROLLBACK")
		return fmt.Errorf("export snapshot: %w", err)
	}

	logger.Info("[coordinator] snapshot exported", "snapshotID", snapshotID)

	if err := s.updateJobSnapshotID(ctx, snapshotID); err != nil {
		_ = s.execSQL(ctx, exportSnapshotConn, "ROLLBACK")
		return fmt.Errorf("update job snapshot ID: %w", err)
	}

	logger.Info("[coordinator] snapshot transaction ready for workers")
	return nil
}

func (s *Snapshotter) snapshotTransactionKeepalive(ctx context.Context, conn pg.Connection) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.execSQL(context.Background(), conn, "SELECT 1")
		}
	}
}

func (s *Snapshotter) cleanupJob(ctx context.Context, slotName string) error {
	return s.retryDBOperation(ctx, func() error {
		chunksQuery := fmt.Sprintf(`
			DELETE FROM %s
			WHERE slot_name = '%s'
		`, chunksTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, chunksQuery); err != nil {
			return fmt.Errorf("delete chunks: %w", err)
		}

		jobQuery := fmt.Sprintf(`
			DELETE FROM %s
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, jobQuery); err != nil {
			return fmt.Errorf("delete job: %w", err)
		}

		logger.Info("[metadata] job cleaned up", "slotName", slotName)
		return nil
	})
}

func (s *Snapshotter) updateJobSnapshotID(ctx context.Context, snapshotID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET snapshot_id = '%s'
			WHERE snapshot_id = 'PENDING'
		`, jobTableName, snapshotID)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return fmt.Errorf("update snapshot ID: %w", err)
		}

		logger.Debug("[metadata] snapshot ID updated", "snapshotID", snapshotID)
		return nil
	})
}

func (s *Snapshotter) setupJob(ctx context.Context, slotName, instanceID string) (bool, error) {
	if err := s.initTables(ctx); err != nil {
		return false, fmt.Errorf("initialize tables: %w", err)
	}

	lockAcquired, err := s.tryAcquireCoordinatorLock(ctx, slotName)
	if err != nil {
		return false, fmt.Errorf("acquire coordinator lock: %w", err)
	}

	if lockAcquired {
		currentLSN, err := s.getCurrentLSN(ctx)
		if err != nil {
			return false, fmt.Errorf("get current LSN: %w", err)
		}

		logger.Debug("[snapshot] elected as coordinator", "instanceID", instanceID)
		if err := s.initializeCoordinator(ctx, slotName, currentLSN); err != nil {
			return false, fmt.Errorf("initialize coordinator: %w", err)
		}
		return true, nil
	}

	logger.Debug("[snapshot] joining as worker", "instanceID", instanceID)
	if err = s.waitForCoordinator(ctx, slotName); err != nil {
		return false, fmt.Errorf("wait for coordinator: %w", err)
	}

	return false, nil
}

func (s *Snapshotter) initTables(ctx context.Context) error {
	jobTableExists, err := s.tableExists(ctx, jobTableName)
	if err != nil {
		return fmt.Errorf("check job table existence: %w", err)
	}

	if !jobTableExists {
		jobTableSQL := fmt.Sprintf(`
			CREATE TABLE %s (
				slot_name TEXT PRIMARY KEY,
				snapshot_id TEXT NOT NULL,
				snapshot_lsn TEXT NOT NULL,
				started_at TIMESTAMP NOT NULL,
				completed BOOLEAN DEFAULT FALSE,
				total_chunks INT NOT NULL DEFAULT 0,
				completed_chunks INT NOT NULL DEFAULT 0
			)
		`, jobTableName)

		if err := s.execSQL(ctx, s.metadataConn, jobTableSQL); err != nil {
			return fmt.Errorf("create job table: %w", err)
		}
		logger.Debug("[metadata] job table created")
	} else {
		logger.Debug("[metadata] job table already exists, skipping creation")
	}

	chunksTableExists, err := s.tableExists(ctx, chunksTableName)
	if err != nil {
		return fmt.Errorf("check chunks table existence: %w", err)
	}

	if !chunksTableExists {
		chunksTableSQL := fmt.Sprintf(`
			CREATE TABLE %s (
				id SERIAL PRIMARY KEY,
				slot_name TEXT NOT NULL,
				table_schema TEXT NOT NULL,
				table_name TEXT NOT NULL,
				chunk_index INT NOT NULL,
				chunk_start BIGINT NOT NULL,
				chunk_size BIGINT NOT NULL,
				range_start BIGINT,
				range_end BIGINT,
				range_start_text TEXT,
				range_end_text TEXT,
				pk_column TEXT,
				use_ctid BOOLEAN DEFAULT FALSE,
				status TEXT NOT NULL DEFAULT 'pending',
				claimed_by TEXT,
				claimed_at TIMESTAMP,
				heartbeat_at TIMESTAMP,
				completed_at TIMESTAMP,
				rows_processed BIGINT DEFAULT 0,
				UNIQUE(slot_name, table_schema, table_name, chunk_index)
			)
		`, chunksTableName)

		if err := s.execSQL(ctx, s.metadataConn, chunksTableSQL); err != nil {
			return fmt.Errorf("create chunks table: %w", err)
		}
		logger.Debug("[metadata] chunks table created")
	} else {
		logger.Debug("[metadata] chunks table already exists, skipping creation")
	}

	indexes := map[string]string{
		"idx_chunks_claim":  fmt.Sprintf("CREATE INDEX idx_chunks_claim ON %s(slot_name, status, claimed_at) WHERE status IN ('pending', 'in_progress')", chunksTableName),
		"idx_chunks_status": fmt.Sprintf("CREATE INDEX idx_chunks_status ON %s(slot_name, status)", chunksTableName),
	}

	for indexName, indexSQL := range indexes {
		indexExists, err := s.indexExists(ctx, indexName)
		if err != nil {
			return fmt.Errorf("check index %s existence: %w", indexName, err)
		}

		if !indexExists {
			if err := s.execSQL(ctx, s.metadataConn, indexSQL); err != nil {
				return fmt.Errorf("create index %s: %w", indexName, err)
			}
			logger.Debug("[metadata] index created", "index", indexName)
		} else {
			logger.Debug("[metadata] index already exists, skipping creation", "index", indexName)
		}
	}

	logger.Debug("[metadata] snapshot tables initialized")
	return nil
}

func (s *Snapshotter) getCurrentLSN(ctx context.Context) (pg.LSN, error) {
	var lsn pg.LSN

	err := s.retryDBOperation(ctx, func() error {
		results, err := s.execQuery(ctx, s.metadataConn, "SELECT pg_current_wal_lsn()")
		if err != nil {
			return fmt.Errorf("execute pg_current_wal_lsn: %w", err)
		}

		if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
			return fmt.Errorf("no LSN returned")
		}

		lsnStr := string(results[0].Rows[0][0])
		lsn, err = pg.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("parse LSN: %w", err)
		}

		return nil
	})

	return lsn, err
}

func (s *Snapshotter) processChunk(ctx context.Context, conn pg.Connection, chunk *Chunk, lsn pg.LSN, handler Handler) (int64, error) {
	table := publication.Table{
		Schema: chunk.TableSchema,
		Name:   chunk.TableName,
	}

	orderByClause, pkColumns, err := s.getOrderByClause(ctx, conn, table)
	if err != nil {
		return 0, fmt.Errorf("get order by clause: %w", err)
	}

	query := s.buildChunkQuery(chunk, orderByClause, pkColumns)

	logger.Debug("[chunk] executing query", "query", query)

	results, err := s.execQuery(ctx, conn, query)
	if err != nil {
		return 0, fmt.Errorf("execute chunk query: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, nil
	}

	result := results[0]
	rowCount := int64(len(result.Rows))

	chunkTime := time.Now().UTC()

	for i, row := range result.Rows {
		rowData := s.parseRow(result.FieldDescriptions, row)

		isLast := (i == len(result.Rows)-1) && (rowCount < chunk.ChunkSize)

		_ = handler(&format.Snapshot{
			EventType:  format.SnapshotEventTypeData,
			Table:      chunk.TableName,
			Schema:     chunk.TableSchema,
			Data:       rowData,
			ServerTime: chunkTime,
			LSN:        lsn,
			IsLast:     isLast,
		})
	}

	return rowCount, nil
}

func (s *Snapshotter) buildChunkQuery(chunk *Chunk, orderByClause string, pkColumns []string) string {
	if chunk.hasRangeBounds() && len(pkColumns) == 1 {
		pkColumn := pkColumns[0]

		return fmt.Sprintf(
			"SELECT * FROM %s.%s WHERE %s >= %d AND %s <= %d ORDER BY %s LIMIT %d",
			chunk.TableSchema,
			chunk.TableName,
			pkColumn,
			*chunk.RangeStart,
			pkColumn,
			*chunk.RangeEnd,
			orderByClause,
			chunk.ChunkSize,
		)
	}

	if chunk.hasTextRangeBounds() && chunk.PKColumn != "" {
		return fmt.Sprintf(
			"SELECT * FROM %s.%s WHERE %s >= %s AND %s <= %s ORDER BY %s LIMIT %d",
			chunk.TableSchema,
			chunk.TableName,
			chunk.PKColumn,
			pqQuote(*chunk.RangeStartText),
			chunk.PKColumn,
			pqQuote(*chunk.RangeEndText),
			chunk.PKColumn,
			chunk.ChunkSize,
		)
	}

	if chunk.UseCtid {
		return fmt.Sprintf(
			"SELECT * FROM %s.%s ORDER BY ctid LIMIT %d OFFSET %d",
			chunk.TableSchema,
			chunk.TableName,
			chunk.ChunkSize,
			chunk.ChunkStart,
		)
	}

	return fmt.Sprintf(
		"SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
		chunk.TableSchema,
		chunk.TableName,
		orderByClause,
		chunk.ChunkSize,
		chunk.ChunkStart,
	)
}

func pqQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func (s *Snapshotter) getOrderByClause(ctx context.Context, conn pg.Connection, table publication.Table) (string, []string, error) {
	if entry, ok := s.loadOrderByCache(table); ok {
		return entry.clause, cloneStringSlice(entry.columns), nil
	}

	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, conn, table)
	if err != nil {
		return "", nil, err
	}

	if len(columns) > 0 {
		var columnNames []string
		for _, column := range columns {
			columnNames = append(columnNames, column.Name)
		}
		orderBy := strings.Join(columnNames, ", ")
		logger.Debug("[chunk] using primary key for ordering", "table", table.Name, "orderBy", orderBy)
		s.storeOrderByCache(table, orderBy, columnNames)
		return orderBy, columnNames, nil
	}

	logger.Debug("[chunk] no primary key, using ctid", "table", table.Name)
	s.storeOrderByCache(table, "ctid", nil)
	return "ctid", nil, nil
}

func (s *Snapshotter) loadOrderByCache(table publication.Table) (orderByCacheEntry, bool) {
	key := s.orderByCacheKey(table)

	s.orderByMu.RLock()
	entry, ok := s.orderByCache[key]
	s.orderByMu.RUnlock()

	if !ok {
		return orderByCacheEntry{}, false
	}

	return orderByCacheEntry{
		clause:  entry.clause,
		columns: cloneStringSlice(entry.columns),
	}, true
}

func (s *Snapshotter) storeOrderByCache(table publication.Table, clause string, columns []string) {
	key := s.orderByCacheKey(table)

	s.orderByMu.Lock()
	s.orderByCache[key] = orderByCacheEntry{
		clause:  clause,
		columns: cloneStringSlice(columns),
	}
	s.orderByMu.Unlock()
}

func (s *Snapshotter) orderByCacheKey(table publication.Table) string {
	return fmt.Sprintf("%s.%s", table.Schema, table.Name)
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func (s *Snapshotter) createTableChunks(ctx context.Context, slotName string, table publication.Table) []*Chunk {
	pkColumn, ok, err := s.getSingleIntegerPrimaryKey(ctx, table)
	if err != nil {
		logger.Warn("[chunk] failed to inspect primary key", "table", table.Name, "error", err)
	}

	if ok {
		if rangeChunks := s.createRangeChunks(ctx, slotName, table, pkColumn); len(rangeChunks) > 0 {
			return rangeChunks
		}
		logger.Warn("[chunk] range chunking unavailable, falling back to text PK", "table", table.Name)
	}

	textPKColumn, textOk, err := s.getSingleTextPrimaryKey(ctx, table)
	if err != nil {
		logger.Warn("[chunk] failed to inspect text primary key", "table", table.Name, "error", err)
	}

	if textOk {
		if textRangeChunks := s.createTextRangeChunks(ctx, slotName, table, textPKColumn); len(textRangeChunks) > 0 {
			return textRangeChunks
		}
		logger.Warn("[chunk] text range chunking unavailable, falling back to ctid", "table", table.Name)
	}

	return s.createCtidChunks(ctx, slotName, table)
}

func (s *Snapshotter) createRangeChunks(ctx context.Context, slotName string, table publication.Table, pkColumn string) []*Chunk {
	minValue, maxValue, ok, err := s.getPrimaryKeyBounds(ctx, table, pkColumn)
	if err != nil {
		logger.Warn("[chunk] failed to read primary key bounds", "table", table.Name, "error", err)
		return nil
	}

	if !ok {
		return []*Chunk{
			{
				SlotName:    slotName,
				TableSchema: table.Schema,
				TableName:   table.Name,
				ChunkIndex:  0,
				ChunkStart:  0,
				ChunkSize:   s.config.ChunkSize,
				Status:      ChunkStatusPending,
			},
		}
	}

	chunkSize := s.config.ChunkSize
	totalRange := (maxValue - minValue) + 1
	numChunks := (totalRange + chunkSize - 1) / chunkSize
	chunks := make([]*Chunk, 0, numChunks)

	for i := range numChunks {
		rangeStart := minValue + (i * chunkSize)
		rangeEnd := rangeStart + chunkSize - 1
		if rangeEnd > maxValue {
			rangeEnd = maxValue
		}

		startValue := rangeStart
		endValue := rangeEnd

		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
			RangeStart:  &startValue,
			RangeEnd:    &endValue,
		}
		chunks = append(chunks, chunk)
	}

	logger.Debug("[chunk] range chunks created",
		"table", table.Name,
		"chunkSize", chunkSize,
		"numChunks", numChunks,
		"rangeMin", minValue,
		"rangeMax", maxValue,
	)
	return chunks
}

func (s *Snapshotter) createOffsetChunks(ctx context.Context, slotName string, table publication.Table) []*Chunk {
	rowCount, err := s.getTableRowCount(ctx, table.Schema, table.Name)
	if err != nil {
		logger.Warn("[chunk] failed to estimate row count, using single chunk", "table", table.Name, "error", err)
		rowCount = 0
	}

	chunkSize := s.config.ChunkSize

	if rowCount == 0 {
		return []*Chunk{
			{
				SlotName:    slotName,
				TableSchema: table.Schema,
				TableName:   table.Name,
				ChunkIndex:  0,
				ChunkStart:  0,
				ChunkSize:   chunkSize,
				Status:      ChunkStatusPending,
			},
		}
	}

	numChunks := (rowCount + chunkSize - 1) / chunkSize
	chunks := make([]*Chunk, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
		}
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] offset chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", numChunks)
	return chunks
}

func (s *Snapshotter) createTextRangeChunks(ctx context.Context, slotName string, table publication.Table, pkColumn string) []*Chunk {
	rowCount, err := s.getTableRowCount(ctx, table.Schema, table.Name)
	if err != nil || rowCount == 0 {
		logger.Warn("[chunk] failed to get row count for text range chunking", "table", table.Name, "error", err)
		return nil
	}

	chunkSize := s.config.ChunkSize
	numChunks := (rowCount + chunkSize - 1) / chunkSize
	if numChunks < 2 {
		numChunks = 2
	}
	if numChunks > 1000 {
		numChunks = 1000
	}

	samplePercent := 10.0
	if rowCount > 1000000 {
		samplePercent = 1.0
	} else if rowCount > 100000 {
		samplePercent = 5.0
	}

	query := fmt.Sprintf(`
		WITH sampled AS (
			SELECT %s
			FROM %s.%s
			TABLESAMPLE BERNOULLI(%.1f)
		),
		boundaries AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as bucket
			FROM sampled
		)
		SELECT bucket, MIN(%s) as range_start, MAX(%s) as range_end
		FROM boundaries
		GROUP BY bucket
		ORDER BY bucket
	`, pkColumn, table.Schema, table.Name, samplePercent, pkColumn, numChunks, pkColumn, pkColumn, pkColumn)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		logger.Warn("[chunk] text range query failed", "table", table.Name, "error", err)
		return nil
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		logger.Warn("[chunk] no text range boundaries returned", "table", table.Name)
		return nil
	}

	chunks := make([]*Chunk, 0, len(results[0].Rows))
	for i, row := range results[0].Rows {
		if len(row) < 3 {
			continue
		}

		rangeStart := string(row[1])
		rangeEnd := string(row[2])

		chunk := &Chunk{
			SlotName:       slotName,
			TableSchema:    table.Schema,
			TableName:      table.Name,
			ChunkIndex:     i,
			ChunkStart:     0,
			ChunkSize:      chunkSize,
			Status:         ChunkStatusPending,
			RangeStartText: &rangeStart,
			RangeEndText:   &rangeEnd,
			PKColumn:       pkColumn,
		}
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] text range chunks created",
		"table", table.Name,
		"pkColumn", pkColumn,
		"numChunks", len(chunks),
		"samplePercent", samplePercent,
	)
	return chunks
}

func (s *Snapshotter) createCtidChunks(ctx context.Context, slotName string, table publication.Table) []*Chunk {
	rowCount, err := s.getTableRowCount(ctx, table.Schema, table.Name)
	if err != nil {
		logger.Warn("[chunk] failed to estimate row count, using single chunk", "table", table.Name, "error", err)
		rowCount = 0
	}

	chunkSize := s.config.ChunkSize

	if rowCount == 0 {
		return []*Chunk{
			{
				SlotName:    slotName,
				TableSchema: table.Schema,
				TableName:   table.Name,
				ChunkIndex:  0,
				ChunkStart:  0,
				ChunkSize:   chunkSize,
				Status:      ChunkStatusPending,
				UseCtid:     true,
			},
		}
	}

	numChunks := (rowCount + chunkSize - 1) / chunkSize
	chunks := make([]*Chunk, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
			UseCtid:     true,
		}
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] ctid chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", numChunks)
	return chunks
}

func (s *Snapshotter) getPrimaryKeyColumnsDetailed(ctx context.Context, conn pg.Connection, table publication.Table) ([]primaryKeyColumn, error) {
	query := fmt.Sprintf(`
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = '%s.%s'::regclass AND i.indisprimary
		ORDER BY a.attnum
	`, table.Schema, table.Name)

	results, err := s.execQuery(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("query primary key: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}

	var columns []primaryKeyColumn
	for _, row := range results[0].Rows {
		if len(row) < 2 {
			continue
		}
		columns = append(columns, primaryKeyColumn{
			Name:     string(row[0]),
			DataType: strings.ToLower(string(row[1])),
		})
	}
	return columns, nil
}

func (s *Snapshotter) getSingleIntegerPrimaryKey(ctx context.Context, table publication.Table) (string, bool, error) {
	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, s.metadataConn, table)
	if err != nil {
		return "", false, err
	}

	if len(columns) != 1 {
		return "", false, nil
	}

	if !isIntegerType(columns[0].DataType) {
		return "", false, nil
	}

	return columns[0].Name, true, nil
}

func (s *Snapshotter) getSingleTextPrimaryKey(ctx context.Context, table publication.Table) (string, bool, error) {
	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, s.metadataConn, table)
	if err != nil {
		return "", false, err
	}

	if len(columns) != 1 {
		return "", false, nil
	}

	if !isTextType(columns[0].DataType) {
		return "", false, nil
	}

	return columns[0].Name, true, nil
}

func isIntegerType(dataType string) bool {
	switch dataType {
	case "smallint", "integer", "bigint", "int2", "int4", "int8":
		return true
	default:
		return false
	}
}

func isTextType(dataType string) bool {
	switch dataType {
	case "text", "varchar", "character varying", "char", "character", "bpchar", "uuid":
		return true
	default:
		return strings.HasPrefix(dataType, "varchar(") ||
			strings.HasPrefix(dataType, "char(") ||
			strings.HasPrefix(dataType, "character varying(") ||
			strings.HasPrefix(dataType, "character(")
	}
}

func (s *Snapshotter) getPrimaryKeyBounds(ctx context.Context, table publication.Table, pkColumn string) (int64, int64, bool, error) {
	query := fmt.Sprintf(`
		SELECT MIN(%s)::bigint AS min_value, MAX(%s)::bigint AS max_value
		FROM %s.%s
	`, pkColumn, pkColumn, table.Schema, table.Name)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return 0, 0, false, fmt.Errorf("query primary key bounds: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, 0, false, nil
	}

	row := results[0].Rows[0]
	if len(row) < 2 || row[0] == nil || row[1] == nil {
		return 0, 0, false, nil
	}

	minValue, err := strconv.ParseInt(string(row[0]), 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse min value: %w", err)
	}

	maxValue, err := strconv.ParseInt(string(row[1]), 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse max value: %w", err)
	}

	return minValue, maxValue, true, nil
}

func (s *Snapshotter) parseRow(fields []pgconn.FieldDescription, row [][]byte) map[string]any {
	rowData := make(map[string]any, len(fields))

	for i, field := range fields {
		if i >= len(row) {
			break
		}

		columnName := field.Name
		columnValue := row[i]

		if columnValue == nil {
			rowData[columnName] = nil
			continue
		}

		val, err := s.decodeColumnData(columnValue, field.DataTypeOID)
		if err != nil {
			logger.Debug("[chunk] failed to decode column, using string", "column", columnName, "error", err)
			rowData[columnName] = string(columnValue)
			continue
		}

		rowData[columnName] = val
	}

	return rowData
}

func (s *Snapshotter) saveChunk(ctx context.Context, chunk *Chunk) error {
	return s.retryDBOperation(ctx, func() error {
		rangeStart := "NULL"
		if chunk.RangeStart != nil {
			rangeStart = fmt.Sprintf("%d", *chunk.RangeStart)
		}

		rangeEnd := "NULL"
		if chunk.RangeEnd != nil {
			rangeEnd = fmt.Sprintf("%d", *chunk.RangeEnd)
		}

		rangeStartText := "NULL"
		if chunk.RangeStartText != nil {
			rangeStartText = pqQuote(*chunk.RangeStartText)
		}

		rangeEndText := "NULL"
		if chunk.RangeEndText != nil {
			rangeEndText = pqQuote(*chunk.RangeEndText)
		}

		pkColumn := "NULL"
		if chunk.PKColumn != "" {
			pkColumn = pqQuote(chunk.PKColumn)
		}

		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, table_schema, table_name, chunk_index,
				chunk_start, chunk_size, range_start, range_end,
				range_start_text, range_end_text, pk_column, use_ctid, status
			) VALUES ('%s', '%s', '%s', %d, %d, %d, %s, %s, %s, %s, %s, %t, '%s')
		`, chunksTableName,
			chunk.SlotName,
			chunk.TableSchema,
			chunk.TableName,
			chunk.ChunkIndex,
			chunk.ChunkStart,
			chunk.ChunkSize,
			rangeStart,
			rangeEnd,
			rangeStartText,
			rangeEndText,
			pkColumn,
			chunk.UseCtid,
			string(chunk.Status),
		)

		_, err := s.execQuery(ctx, s.metadataConn, query)
		return err
	})
}

func (s *Snapshotter) getTableRowCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return 0, fmt.Errorf("table row count: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return 0, nil
	}

	countStr := string(results[0].Rows[0][0])
	if countStr == "" || countStr == "-1" {
		return 0, nil
	}

	var count int64
	if _, err := fmt.Sscanf(countStr, "%d", &count); err != nil {
		return 0, fmt.Errorf("parse row count: %w", err)
	}

	return count, nil
}

func (s *Snapshotter) saveJob(ctx context.Context, job *Job) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, snapshot_id, snapshot_lsn, started_at,
				completed, total_chunks, completed_chunks
			) VALUES ('%s', '%s', '%s', '%s', %t, %d, %d)
		`, jobTableName,
			job.SlotName,
			job.SnapshotID,
			job.SnapshotLSN.String(),
			job.StartedAt.Format(postgresTimestampFormat),
			job.Completed,
			job.TotalChunks,
			job.CompletedChunks,
		)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return fmt.Errorf("create job: %w", err)
		}

		logger.Debug("[metadata] job created", "slotName", job.SlotName, "snapshotID", job.SnapshotID)
		return nil
	})
}

func (s *Snapshotter) tryAcquireCoordinatorLock(ctx context.Context, slotName string) (bool, error) {
	lockID := hashString(slotName)

	query := fmt.Sprintf("SELECT pg_try_advisory_lock(%d)", lockID)
	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("acquire coordinator lock: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("no lock result returned")
	}

	acquired := string(results[0].Rows[0][0]) == "t"
	return acquired, nil
}

func hashString(s string) int64 {
	var hash int64
	for i := 0; i < len(s); i++ {
		hash = hash*31 + int64(s[i])
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

func (s *Snapshotter) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public'
			AND table_name = '%s'
		)
	`, tableName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("query table existence: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("no result returned from table existence check")
	}

	exists := string(results[0].Rows[0][0]) == "t"
	return exists, nil
}

func (s *Snapshotter) indexExists(ctx context.Context, indexName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = 'public'
			AND indexname = '%s'
		)
	`, indexName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("query index existence: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("no result returned from index existence check")
	}

	exists := string(results[0].Rows[0][0]) == "t"
	return exists, nil
}
