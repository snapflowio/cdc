package snapshot

import (
	"context"
	"fmt"
	"sync"

	"github.com/snapflowio/cdc/logger"
	"github.com/snapflowio/cdc/internal/pg"
)

type ConnectionPool struct {
	pool  chan pg.Connection
	dsn   string
	conns []pg.Connection
	mu    sync.Mutex
}

func NewConnectionPool(ctx context.Context, dsn string, size int) (*ConnectionPool, error) {
	if size <= 0 {
		size = 5
	}

	p := &ConnectionPool{
		dsn:   dsn,
		pool:  make(chan pg.Connection, size),
		conns: make([]pg.Connection, 0, size),
	}

	logger.Info("[snapshot-connection-pool] creating connection pool", "size", size)
	for i := 0; i < size; i++ {
		conn, err := pg.NewConnection(ctx, dsn)
		if err != nil {
			p.Close(ctx)
			return nil, fmt.Errorf("create pool connection: %w", err)
		}

		p.conns = append(p.conns, conn)
		p.pool <- conn
	}

	logger.Info("[snapshot-connection-pool] connection pool ready", "size", size)
	return p, nil
}

func (p *ConnectionPool) Get(ctx context.Context) (pg.Connection, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *ConnectionPool) Put(conn pg.Connection) {
	select {
	case p.pool <- conn:
	default:
		logger.Warn("[snapshot-connection-pool] pool is full, connection not returned")
	}
}

func (p *ConnectionPool) Close(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Info("[snapshot-connection-pool] closing all connections", "count", len(p.conns))

	for _, conn := range p.conns {
		if err := conn.Close(ctx); err != nil {
			logger.Warn("[snapshot-connection-pool] error closing connection", "error", err)
		}
	}

	close(p.pool)
	p.conns = nil
	logger.Info("[snapshot-connection-pool] connection pool closed")
}
