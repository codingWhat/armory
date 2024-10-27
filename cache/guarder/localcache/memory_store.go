package localcache

import "time"

type MemoryStore interface {
	Set(key string, val interface{}, ttl time.Duration) error
	Get(key string) (interface{}, error)
}
