package slot

import "github.com/snapflowio/cdc/internal/pg"

const (
	Logical  Type = "logical"
	Physical Type = "physical"
)

type Type string

type Info struct {
	Name              string `json:"name"`
	Type              Type   `json:"type"`
	WalStatus         string `json:"walStatus"`
	RestartLSN        pg.LSN `json:"restartLSN"`
	ConfirmedFlushLSN pg.LSN `json:"confirmedFlushLSN"`
	CurrentLSN        pg.LSN `json:"currentLSN"`
	RetainedWALSize   pg.LSN `json:"retainedWALSize"`
	Lag               pg.LSN `json:"lag"`
	ActivePID         int32  `json:"activePID"`
	Active            bool   `json:"active"`
}
