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

func formatValue(value any, typeOID uint32) any {
	if value == nil {
		return nil
	}

	// Handle UUID type (OID 2950)
	if typeOID == 2950 {
		return formatUUID(value)
	}

	// Handle []byte values based on type
	if bytesVal, ok := value.([]byte); ok {
		switch typeOID {
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

func formatUUID(value any) string {
	var uuidBytes [16]byte

	switch v := value.(type) {
	case [16]byte:
		uuidBytes = v
	case []byte:
		if len(v) == 16 {
			copy(uuidBytes[:], v)
		} else if len(v) == 36 {
			return string(v)
		} else {
			return fmt.Sprintf("%v", value)
		}
	case string:
		return v
	default:
		if stringer, ok := value.(fmt.Stringer); ok {
			return stringer.String()
		}

		return fmt.Sprintf("%v", value)
	}

	var buf [36]byte
	encodeUUIDHex(buf[:], uuidBytes)
	return string(buf[:])
}

func encodeUUIDHex(dst []byte, uuid [16]byte) {
	const hexDigits = "0123456789abcdef"

	for i, b := range uuid {
		dst[i*2] = hexDigits[b>>4]
		dst[i*2+1] = hexDigits[b&0x0f]
	}

	// Insert hyphens at positions: 8, 13, 18, 23
	// Work backwards to avoid overwriting
	copy(dst[24:36], dst[20:32]) // Last 12 chars (6 bytes)
	dst[23] = '-'
	copy(dst[19:23], dst[16:20]) // 4 chars (2 bytes)
	dst[18] = '-'
	copy(dst[14:18], dst[12:16]) // 4 chars (2 bytes)
	dst[13] = '-'
	copy(dst[9:13], dst[8:12]) // 4 chars (2 bytes)
	dst[8] = '-'
	// First 8 chars already in place
}

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
