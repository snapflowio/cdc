package format

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/message/tuple"
)

const (
	InsertTupleDataType = 'N'
)

type Insert struct {
	MessageTime    time.Time
	TupleData      *tuple.Data
	WAL2JSON       *WAL2JSONMessage
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
}

func NewInsert(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Insert, error) {
	msg := &Insert{
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

	// Decode tuple data
	decoded, err := msg.TupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	// Build wal2json structure
	msg.WAL2JSON, err = buildWAL2JSON("I", rel, decoded)
	if err != nil {
		return nil, fmt.Errorf("build wal2json: %w", err)
	}

	return msg, nil
}

func (m *Insert) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 13 {
			return fmt.Errorf("streamed transaction insert message length must be at least 13 bytes, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 9 {
		return fmt.Errorf("insert message length must be at least 9 bytes, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var err error

	m.TupleData, err = tuple.NewData(data, InsertTupleDataType, skipByte)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}

	return nil
}
