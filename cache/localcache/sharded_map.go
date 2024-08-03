package localcache

import (
	"github.com/cespare/xxhash/v2"
	"sync"
)

type safeMap struct {
	mu   sync.RWMutex
	data map[string]any
}

func newSafeMap() *safeMap {
	return &safeMap{
		mu:   sync.RWMutex{},
		data: make(map[string]any),
	}
}

func (sm *safeMap) set(k string, v any) {
	sm.mu.Lock()
	sm.data[k] = v
	sm.mu.Unlock()
}
func (sm *safeMap) get(k string) (any, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	v, ok := sm.data[k]
	return v, ok
}

func (sm *safeMap) del(k string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.data, k)
}

func (sm *safeMap) clear() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data = make(map[string]any)
}

type store interface {
	set(k string, v any)
	get(k string) (any, bool)
	del(k string)
	clear()
}

const shardsNum uint64 = 256

type shardedMap struct {
	shards []*safeMap
}

func (s *shardedMap) set(k string, v any) {
	s.shards[xxhash.Sum64String(k)%shardsNum].set(k, v)
}

func (s *shardedMap) get(k string) (any, bool) {
	return s.shards[xxhash.Sum64String(k)%shardsNum].get(k)
}

func (s *shardedMap) del(k string) {
	s.shards[xxhash.Sum64String(k)%shardsNum].del(k)
}

func (s *shardedMap) clear(k string) {
	for _, shard := range s.shards {
		shard.clear()
	}
}

func newShardedMap() *shardedMap {
	sm := &shardedMap{
		shards: make([]*safeMap, shardsNum),
	}
	for i := range sm.shards {
		sm.shards[i] = newSafeMap()
	}
	return sm
}