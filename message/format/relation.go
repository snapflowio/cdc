package format

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/snapflowio/cdc/message/tuple"
)

type Relation struct {
	Namespace     string
	Name          string
	Columns       []tuple.RelationColumn
	OID           uint32
	XID           uint32
	ColumnNumbers uint16
	ReplicaID     uint8
}

func NewRelation(data []byte, streamedTransaction bool) (*Relation, error) {
	msg := &Relation{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Relation) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 12 {
			return fmt.Errorf("streamed transaction relation message length must be at least 12 bytes, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 8 {
		return fmt.Errorf("relation message length must be at least 8 bytes, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var usedByteCount int
	m.Namespace, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return fmt.Errorf("relation message namespace decode error")
	}

	skipByte += usedByteCount

	m.Name, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return fmt.Errorf("relation message name decode error")
	}

	skipByte += usedByteCount

	m.ReplicaID = data[skipByte]
	skipByte++

	m.ColumnNumbers = binary.BigEndian.Uint16(data[skipByte:])
	skipByte += 2

	m.Columns = make([]tuple.RelationColumn, m.ColumnNumbers)
	for i := range m.Columns {
		col := tuple.RelationColumn{}
		col.Flags = data[skipByte]
		skipByte++

		col.Name, usedByteCount = decodeString(data[skipByte:])
		if usedByteCount < 0 {
			return fmt.Errorf("relation message columns[%d].name decode error", i)
		}

		skipByte += usedByteCount

		col.DataType = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4

		col.TypeModifier = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4

		m.Columns[i] = col
	}

	return nil
}

func decodeString(data []byte) (string, int) {
	end := bytes.IndexByte(data, byte(0))
	if end == -1 {
		return "", -1
	}

	return string(data[:end]), end + 1
}
