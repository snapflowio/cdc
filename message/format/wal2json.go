package format

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type WAL2JSONColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	TypeOID uint32 `json:"typeoid"`
	Value   any    `json:"value"`
}

type WAL2JSONPrimaryKey struct {
	Name string `json:"name"`
}

type WAL2JSONMessage struct {
	Action  string               `json:"action"`
	Schema  string               `json:"schema"`
	Table   string               `json:"table"`
	Columns []WAL2JSONColumn     `json:"columns"`
	PK      []WAL2JSONPrimaryKey `json:"pk"`
}

// ToMap converts WAL2JSONMessage to map[string]any for easy JSON marshaling
func (w *WAL2JSONMessage) ToMap() map[string]any {
	return map[string]any{
		"action":  w.Action,
		"schema":  w.Schema,
		"table":   w.Table,
		"columns": w.Columns,
		"pk":      w.PK,
	}
}

// ToJSON converts WAL2JSONMessage to JSON bytes
func (w *WAL2JSONMessage) ToJSON() ([]byte, error) {
	return json.Marshal(w.ToMap())
}

// buildWAL2JSON builds a wal2json message from relation and decoded data
func buildWAL2JSON(action string, rel *Relation, decoded map[string]any) (*WAL2JSONMessage, error) {
	if rel == nil {
		return nil, fmt.Errorf("relation is nil")
	}

	columns := make([]WAL2JSONColumn, 0, len(rel.Columns))
	pk := make([]WAL2JSONPrimaryKey, 0)

	for _, colMeta := range rel.Columns {
		value := decoded[colMeta.Name]

		// Convert values based on PostgreSQL type
		value = formatValue(value, colMeta.DataType)

		columns = append(columns, WAL2JSONColumn{
			Name:    colMeta.Name,
			Type:    getTypeName(colMeta.DataType),
			TypeOID: colMeta.DataType,
			Value:   value,
		})

		// Check if this is a primary key column (flag & 1 == 1)
		if colMeta.Flags&1 == 1 {
			pk = append(pk, WAL2JSONPrimaryKey{Name: colMeta.Name})
		}
	}

	return &WAL2JSONMessage{
		Action:  action,
		Schema:  rel.Namespace,
		Table:   rel.Name,
		Columns: columns,
		PK:      pk,
	}, nil
}

// formatValue converts a value based on its PostgreSQL type OID
func formatValue(value any, typeOID uint32) any {
	if value == nil {
		return nil
	}

	// Handle []byte values based on type
	if bytesVal, ok := value.([]byte); ok {
		switch typeOID {
		case 2950: // UUID
			if len(bytesVal) == 16 {
				// Format as UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
				return fmt.Sprintf("%x-%x-%x-%x-%x",
					bytesVal[0:4],
					bytesVal[4:6],
					bytesVal[6:8],
					bytesVal[8:10],
					bytesVal[10:16])
			}
		case 17: // bytea
			// Format as hex string
			return "\\x" + hex.EncodeToString(bytesVal)
		default:
			// For other types, convert to string (text, varchar, etc.)
			return string(bytesVal)
		}
	}

	return value
}

// getTypeName returns a PostgreSQL type name for common OIDs
func getTypeName(oid uint32) string {
	switch oid {
	case 16:
		return "bool"
	case 17:
		return "bytea"
	case 18:
		return "char"
	case 19:
		return "name"
	case 20:
		return "int8"
	case 21:
		return "int2"
	case 23:
		return "int4"
	case 25:
		return "text"
	case 700:
		return "float4"
	case 701:
		return "float8"
	case 1043:
		return "varchar"
	case 1082:
		return "date"
	case 1083:
		return "time"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamptz"
	case 2950:
		return "uuid"
	case 3802:
		return "jsonb"
	default:
		return "unknown"
	}
}
