package replication

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/snapflowio/cdc/internal/pg"
)

var microSecFromUnixEpochToY2K = int64(946684800)

type XLogData struct {
	ServerTime   time.Time
	WALData      []byte
	WALStart     pg.LSN
	ServerWALEnd pg.LSN
}

func ParseXLogData(buf []byte) (XLogData, error) {
	var xld XLogData
	if len(buf) < 24 {
		return xld, fmt.Errorf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	xld.WALStart = pg.LSN(binary.BigEndian.Uint64(buf))
	xld.ServerWALEnd = pg.LSN(binary.BigEndian.Uint64(buf[8:]))
	xld.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:])))
	xld.WALData = buf[24:]

	return xld, nil
}

func pgTimeToTime(microSecSinceY2K int64) time.Time {
	return time.Unix(microSecFromUnixEpochToY2K+(microSecSinceY2K/1_000_000), (microSecSinceY2K%1_000_000)*1_000).UTC()
}
