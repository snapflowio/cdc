package snapshot

import (
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

type TypeDecoder struct {
	oid uint32
}

func (d *TypeDecoder) Decode(typeMap *pgtype.Map, data []byte) (any, error) {
	if dt, ok := typeMap.TypeForOID(d.oid); ok {
		return dt.Codec.DecodeValue(typeMap, d.oid, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}

type DecoderCache struct {
	cache map[uint32]*TypeDecoder
	mu    sync.RWMutex
}

func NewDecoderCache() *DecoderCache {
	return &DecoderCache{
		cache: make(map[uint32]*TypeDecoder, 50),
	}
}

func (c *DecoderCache) Get(oid uint32) *TypeDecoder {
	c.mu.RLock()
	decoder, exists := c.cache[oid]
	c.mu.RUnlock()

	if exists {
		return decoder
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if decoder, exists := c.cache[oid]; exists {
		return decoder
	}

	decoder = &TypeDecoder{oid: oid}
	c.cache[oid] = decoder

	return decoder
}

func (c *DecoderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}
