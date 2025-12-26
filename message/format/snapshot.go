package format

import (
	"time"

	"github.com/snapflowio/cdc/internal/pg"
)

type SnapshotEventType string

const (
	SnapshotEventTypeBegin SnapshotEventType = "BEGIN"
	SnapshotEventTypeData  SnapshotEventType = "DATA"
	SnapshotEventTypeEnd   SnapshotEventType = "END"
)

type Snapshot struct {
	ServerTime time.Time
	Data       map[string]any
	EventType  SnapshotEventType
	Table      string
	Schema     string
	LSN        pg.LSN
	TotalRows  int64
	IsLast     bool
}
