package localcache

import (
	"github.com/pkg/errors"
	"sync"
	"time"
)

type SimpleMemoryStore struct {
	store  map[string]interface{}
	mutex  sync.Mutex
	timers map[string]*time.Timer
}

func NewSimpleMemoryStore() *SimpleMemoryStore {
	return &SimpleMemoryStore{
		store:  make(map[string]interface{}),
		timers: make(map[string]*time.Timer),
	}
}

func (s *SimpleMemoryStore) Set(key string, val interface{}, ttl time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If there's an existing timer, stop it
	if timer, found := s.timers[key]; found {
		timer.Stop()
		delete(s.timers, key)
	}

	s.store[key] = val

	// Set a new timer for TTL
	if ttl > 0 {
		s.timers[key] = time.AfterFunc(ttl, func() {
			s.mutex.Lock()
			delete(s.store, key)
			delete(s.timers, key)
			s.mutex.Unlock()
		})
	}

	return nil
}

func (s *SimpleMemoryStore) Get(key string) (interface{}, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	val, found := s.store[key]
	if !found {
		return nil, errors.New("key not found")
	}
	return val, nil
}
