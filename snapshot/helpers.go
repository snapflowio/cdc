package snapshot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
)

const (
	postgresTimestampFormat       = "2006-01-02 15:04:05"
	postgresTimestampFormatMicros = "2006-01-02 15:04:05.999999"
)

func (s *Snapshotter) execSQL(ctx context.Context, conn pg.Connection, sql string) error {
	resultReader := conn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return err
	}

	return resultReader.Close()
}

func (s *Snapshotter) execQuery(ctx context.Context, conn pg.Connection, query string) ([]*pgconn.Result, error) {
	resultReader := conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if err = resultReader.Close(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *Snapshotter) retryDBOperation(ctx context.Context, operation func() error) error {
	maxRetries := 3
	retryDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryDelay * time.Duration(1<<uint(attempt-1))
			logger.Debug("[retry] database operation", "attempt", attempt, "delay", delay)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := operation()
		if err == nil {
			return nil
		}

		if isTransientError(err) {
			lastErr = err
			continue
		}

		return err
	}

	return fmt.Errorf("database operation failed after retries: %w", lastErr)
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "55006", "55P03", "57P03", "58000", "58030":
			return true
		}
	}

	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}

	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.ECONNREFUSED) ||
			errors.Is(netErr.Err, syscall.ECONNRESET) ||
			errors.Is(netErr.Err, syscall.EPIPE) {
			return true
		}
	}

	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection lost") {
		return true
	}

	return false
}

func isInvalidSnapshotError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "22023" && strings.Contains(strings.ToLower(pgErr.Message), "invalid snapshot identifier") {
			return true
		}
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "invalid snapshot identifier")
}

func parseNullableInt64(value []byte) (*int64, error) {
	if value == nil {
		return nil, nil
	}

	str := string(value)
	if str == "" {
		return nil, nil
	}

	parsed, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, err
	}

	return &parsed, nil
}

func parseNullableString(value []byte) *string {
	if value == nil {
		return nil
	}

	str := string(value)
	if str == "" {
		return nil
	}

	return &str
}

func parseNullableBool(value []byte) (bool, error) {
	if value == nil {
		return false, nil
	}

	str := string(value)
	if str == "" || str == "f" || str == "false" {
		return false, nil
	}

	if str == "t" || str == "true" {
		return true, nil
	}

	return strconv.ParseBool(str)
}

func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		postgresTimestampFormatMicros,
		postgresTimestampFormat,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}
