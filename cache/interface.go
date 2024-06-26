package cache

import (
	"context"
	"time"
)

type Client interface {
	Do(ctx context.Context, cmd string, args ...string)
}

type LocalCache interface {
	Set(k, v []byte, ttl time.Duration) error
	Get(k []byte) (interface{}, bool)
}
