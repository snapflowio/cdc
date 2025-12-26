package message

import (
	"fmt"
	"time"

	"github.com/snapflowio/cdc/message/format"
)

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrByteNotSupported = fmt.Errorf("message byte not supported")

type Type uint8

var streamedTransaction bool

func New(data []byte, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
	switch Type(data[0]) {
	case InsertByte:
		return format.NewInsert(data, streamedTransaction, relation, serverTime)
	case UpdateByte:
		return format.NewUpdate(data, streamedTransaction, relation, serverTime)
	case DeleteByte:
		return format.NewDelete(data, streamedTransaction, relation, serverTime)
	case StreamStopByte, StreamAbortByte, StreamCommitByte:
		streamedTransaction = false
		return nil, nil
	case RelationByte:
		msg, err := format.NewRelation(data, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}

		return msg, err
	case StreamStartByte:
		streamedTransaction = true

		return nil, nil
	default:
		return nil, fmt.Errorf("%w: %c", ErrByteNotSupported, data[0])
	}
}
