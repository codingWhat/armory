package simplecache

import "sync"

type cache struct {
	mu   *sync.RWMutex
	data map[string]interface{}
}

func (c *cache) set(k string, v interface{}) {
	c.mu.Lock()
	c.data[k] = v
	c.mu.Unlock()
}

func (c *cache) get(k string) (interface{}, bool) {
	c.mu.RLock()
	v, ok := c.data[k]
	c.mu.RUnlock()
	return v, ok
}
func (c *cache) del(k string) {
	c.mu.Lock()
	delete(c.data, k)
	c.mu.Unlock()
}
