package publication

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/snapflowio/cdc/logger"
)

const (
	ReplicaIdentityDefault = "DEFAULT"
	ReplicaIdentityFull    = "FULL"
	ReplicaIdentityIndex   = "INDEX"
	ReplicaIdentityNothing = "NOTHING"
)

var (
	ErrTablesNotExist      = errors.New("table does not exist")
	ReplicaIdentityOptions = []string{ReplicaIdentityDefault, ReplicaIdentityFull, ReplicaIdentityIndex, ReplicaIdentityNothing}
	ReplicaIdentityMap     = map[string]string{
		"d": ReplicaIdentityDefault,
		"f": ReplicaIdentityFull,
		"i": ReplicaIdentityIndex,
		"n": ReplicaIdentityNothing,
	}
)

func (p *Publication) SetReplicaIdentities(ctx context.Context) error {
	if !p.cfg.CreateIfNotExists {
		return nil
	}

	if len(p.cfg.Tables) == 0 {
		return nil
	}

	tables, err := p.GetReplicaIdentities(ctx)
	if err != nil {
		return err
	}

	diff := p.cfg.Tables.Diff(tables)

	for _, d := range diff {
		if err = p.AlterTableReplicaIdentity(ctx, d); err != nil {
			return err
		}
	}

	return nil
}

func (p *Publication) AlterTableReplicaIdentity(ctx context.Context, t Table) error {
	var query string
	if t.ReplicaIdentity == ReplicaIdentityIndex {
		query = fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY USING INDEX %s;", t.Schema, t.Name, t.IndexName)
	} else {
		query = fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY %s;", t.Schema, t.Name, t.ReplicaIdentity)
	}

	resultReader := p.conn.Exec(ctx, query)
	_, err := resultReader.ReadAll()
	if err != nil {
		return fmt.Errorf("table replica identity update result: %w", err)
	}

	if err = resultReader.Close(); err != nil {
		return fmt.Errorf("table replica identity update result reader close: %w", err)
	}

	if t.ReplicaIdentity == ReplicaIdentityIndex {
		logger.Info("table replica identity updated", "name", t.Name, "replica_identity", t.ReplicaIdentity, "index", t.IndexName)
	} else {
		logger.Info("table replica identity updated", "name", t.Name, "replica_identity", t.ReplicaIdentity)
	}

	return nil
}

func (p *Publication) GetReplicaIdentities(ctx context.Context) ([]Table, error) {
	tableNames := make([]string, len(p.cfg.Tables))

	for i, t := range p.cfg.Tables {
		if t.Schema == "" && !strings.Contains(t.Name, ".") {
			tableNames[i] = "'" + t.Name + "'"
		} else {
			tableNames[i] = "'" + t.Schema + "." + t.Name + "'"
		}
	}

	query := fmt.Sprintf(`
		SELECT
			c.relname AS table_name,
			n.nspname AS schema_name,
			c.relreplident AS replica_identity,
			CASE
				WHEN c.relreplident = 'i' THEN i.indexrelid::regclass::text
				ELSE NULL
			END AS index_name
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_index i ON c.oid = i.indrelid AND i.indisreplident = true
		WHERE concat(n.nspname, '.', c.relname) IN (%s)
	`, strings.Join(tableNames, ", "))

	logger.Debug("executing query: " + query)

	resultReader := p.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("replica identities result: %w", err)
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrTablesNotExist
	}

	if err = resultReader.Close(); err != nil {
		return nil, fmt.Errorf("replica identities result reader close: %w", err)
	}

	replicaIdentities, err := decodeReplicaIdentitiesResult(results)
	if err != nil {
		return nil, fmt.Errorf("replica identities result decode: %w", err)
	}

	return replicaIdentities, nil
}

func decodeReplicaIdentitiesResult(results []*pgconn.Result) ([]Table, error) {
	var res []Table

	for _, result := range results {
		for i := range len(result.Rows) {
			var t Table
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "table_name":
					t.Name = v.(string)
				case "schema_name":
					t.Schema = v.(string)
				case "replica_identity":
					t.ReplicaIdentity = ReplicaIdentityMap[string(v.(int32))]
				case "index_name":
					if str, ok := v.(string); ok {
						t.IndexName = str
					}
				}
			}
			res = append(res, t)
		}
	}

	return res, nil
}
