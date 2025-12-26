package publication

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
)

var ErrPublicationNotExists = errors.New("publication does not exist")

var typeMap = pgtype.NewMap()

type Publication struct {
	conn pg.Connection
	cfg  Config
}

func New(cfg Config, conn pg.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (p *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := p.Info(ctx)
	if err != nil {
		if !errors.Is(err, ErrPublicationNotExists) || !p.cfg.CreateIfNotExists {
			return nil, fmt.Errorf("publication info: %w", err)
		}
	} else {
		logger.Warn("publication already exists")
		return info, nil
	}

	resultReader := p.conn.Exec(ctx, p.cfg.createQuery())
	_, err = resultReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("publication create result: %w", err)
	}

	if err = resultReader.Close(); err != nil {
		return nil, fmt.Errorf("publication create result reader close: %w", err)
	}

	logger.Info("publication created", "name", p.cfg.Name)

	return &p.cfg, nil
}

func (p *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := p.conn.Exec(ctx, p.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		var v *pgconn.PgError
		if errors.As(err, &v) && v.Code == "42703" {
			return nil, ErrPublicationNotExists
		}

		return nil, fmt.Errorf("publication info result: %w", err)
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrPublicationNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, fmt.Errorf("publication info result reader close: %w", err)
	}

	publicationInfo, err := decodePublicationInfoResult(results[0])
	if err != nil {
		return nil, fmt.Errorf("publication info result decode: %w", err)
	}

	return publicationInfo, nil
}

func decodePublicationInfoResult(result *pgconn.Result) (*Config, error) {
	var publicationConfig Config
	var tables []string

	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "pubname":
			publicationConfig.Name = v.(string)
		case "pubinsert":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "INSERT")
			}
		case "pubupdate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "UPDATE")
			}
		case "pubdelete":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "DELETE")
			}
		case "pubtruncate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "TRUNCATE")
			}
		case "pubtables":
			for _, val := range v.([]any) {
				tables = append(tables, val.(string))
			}
		}
	}

	for _, tableName := range tables {
		st := strings.Split(tableName, ".")
		publicationConfig.Tables = append(publicationConfig.Tables, Table{
			Name:   st[1],
			Schema: st[0],
		})
	}

	return &publicationConfig, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}

func (p *Publication) AddTable(ctx context.Context, table Table) error {
	query := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s.%s;",
		p.cfg.Name,
		table.Schema,
		table.Name,
	)

	resultReader := p.conn.Exec(ctx, query)
	_, err := resultReader.ReadAll()
	if err != nil {
		return fmt.Errorf("add table to publication: %w", err)
	}

	if err = resultReader.Close(); err != nil {
		return fmt.Errorf("add table result reader close: %w", err)
	}

	logger.Info("table added to publication", "publication", p.cfg.Name, "table", table.Schema+"."+table.Name)

	return nil
}

func (p *Publication) RemoveTable(ctx context.Context, table Table) error {
	query := fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s.%s;",
		p.cfg.Name,
		table.Schema,
		table.Name,
	)

	resultReader := p.conn.Exec(ctx, query)
	_, err := resultReader.ReadAll()
	if err != nil {
		return fmt.Errorf("remove table from publication: %w", err)
	}

	if err = resultReader.Close(); err != nil {
		return fmt.Errorf("remove table result reader close: %w", err)
	}

	logger.Info("table removed from publication", "publication", p.cfg.Name, "table", table.Schema+"."+table.Name)

	return nil
}
