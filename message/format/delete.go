package format

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/message/tuple"
)

type Delete struct {
	MessageTime    time.Time
	OldTupleData   *tuple.Data
	WAL2JSON       *WAL2JSONMessage
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
	OldTupleType   uint8
}

func NewDelete(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Delete, error) {
	msg := &Delete{
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

	// Decode old tuple data
	oldDecoded, err := msg.OldTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	// Build wal2json structure
	msg.WAL2JSON, err = buildWAL2JSON("D", rel, oldDecoded)
	if err != nil {
		return nil, fmt.Errorf("build wal2json: %w", err)
	}

	return msg, nil
}

func (m *Delete) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return fmt.Errorf("streamed transaction delete message length must be at least 11 bytes, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return fmt.Errorf("delete message length must be at least 7 bytes, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
	if err != nil {
		return fmt.Errorf("delete message old tuple data: %w", err)
	}

	return nil
}
