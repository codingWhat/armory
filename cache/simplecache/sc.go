package simplecache

import (
	cache2 "github.com/codingWhat/armory/cache"
	"sync"
)

func New() cache2.Cache {
	return &cache{
		data: make(map[string]any),
	}
}

type cache struct {
	mu   sync.RWMutex //
	data map[string]any
}

func (c *cache) Set(k string, v any) {
	c.mu.Lock()
	c.data[k] = v
	c.mu.Unlock()
}

func (c *cache) Get(k string) any {
	c.mu.RLock()
	v := c.data[k]
	c.mu.RUnlock()
	return v
}
func (c *cache) del(k string) {
	c.mu.Lock()
	delete(c.data, k)
	c.mu.Unlock()
}
