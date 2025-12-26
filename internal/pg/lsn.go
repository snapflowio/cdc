package pg

import "fmt"

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

func ParseLSN(s string) (LSN, error) {
	var upperHalf, lowerHalf uint64

	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0, fmt.Errorf("lsn parse: %w", err)
	}

	if nparsed != 2 {
		return 0, fmt.Errorf("lsn parse: invalid format: %s", s)
	}

	return LSN((upperHalf << 32) + lowerHalf), nil
}
