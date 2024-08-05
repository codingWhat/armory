package localcache

import "time"

func WithCapacity(size int) Option {
	return func(c *cache) {
		if size <= 0 {
			return
		}
		c.size = size
	}
}

func WithSetTimout(t time.Duration) Option {
	return func(c *cache) {
		c.setTimeout = t
		c.isSync = true
	}
}

type Option func(*cache)
