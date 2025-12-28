package format

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/message/tuple"
)

const (
	UpdateTupleTypeKey = 'K'
	UpdateTupleTypeOld = 'O'
	UpdateTupleTypeNew = 'N'
)

type Update struct {
	MessageTime    time.Time
	NewTupleData   *tuple.Data
	OldTupleData   *tuple.Data
	WAL2JSON       *WAL2JSONMessage
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
	OldTupleType   uint8
}

func NewUpdate(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Update, error) {
	msg := &Update{
		MessageTime: serverTime,
	}

	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, fmt.Errorf("relation not found")
	}

	msg.TableNamespace = rel.Namespace
	msg.TableName = rel.Name

	var err error

	// Decode new tuple data (UPDATE always has new data)
	newDecoded, err := msg.NewTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	// Build wal2json structure with new data
	msg.WAL2JSON, err = buildWAL2JSON("U", rel, newDecoded)
	if err != nil {
		return nil, fmt.Errorf("build wal2json: %w", err)
	}

	return msg, nil
}

func (m *Update) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return fmt.Errorf("streamed transaction update message length must be at least 11 bytes, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return fmt.Errorf("update message length must be at least 7 bytes, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	switch m.OldTupleType {
	case UpdateTupleTypeKey, UpdateTupleTypeOld:
		m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
		if err != nil {
			return fmt.Errorf("update message old tuple data: %w", err)
		}
		skipByte = m.OldTupleData.SkipByte
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, err = tuple.NewData(data, UpdateTupleTypeNew, skipByte)
		if err != nil {
			return fmt.Errorf("update message new tuple data: %w", err)
		}

		if m.OldTupleData != nil {
			for i, col := range m.NewTupleData.Columns {
				if col.DataType == tuple.DataTypeToast {
					m.NewTupleData.Columns[i] = m.OldTupleData.Columns[i]
				}
			}
		}
	default:
		return fmt.Errorf("update message undefined tuple type")
	}

	return nil
}
