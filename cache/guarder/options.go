package guarder

import (
	"github.com/codingWhat/armory/cache/guarder/localcache"
	"time"
)

type Options struct {
	EnableLocalCache bool
	LocalCache       localcache.MemoryStore

	LocalCacheTTL  time.Duration
	RemoteCacheTTL time.Duration
}

type Option func(ops *Options)

func WithEnableLocalCache() Option {
	return func(ops *Options) {
		ops.EnableLocalCache = true
	}
}
func WithLocalCache(lc localcache.MemoryStore) Option {
	return func(ops *Options) {
		ops.LocalCache = lc
	}
}
func WithLocalCacheTTL(ttl time.Duration) Option {
	return func(ops *Options) {
		ops.LocalCacheTTL = ttl
	}
}
func WithRemoteCacheTTL(ttl time.Duration) Option {
	return func(ops *Options) {
		ops.RemoteCacheTTL = ttl
	}
}
