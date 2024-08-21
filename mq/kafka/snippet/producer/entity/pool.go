package entity

import (
	"github.com/IBM/sarama"
	"sync"
	"sync/atomic"
)

var _PI = &producerInstances{
	async: newAsyncInstances(),
	sync:  newSyncInstances(),
}

type producerInstances struct {
	async *asyncInstances
	sync  *syncInstances

	isClosed atomic.Bool
}

func (pi *producerInstances) close() {
	if pi.isClosed.Load() {
		return
	}

	pi.isClosed.Store(true)
	pi.sync.close()
	pi.async.close()
}

type asyncInstances struct {
	mu        sync.RWMutex
	instances map[string]sarama.AsyncProducer
}

func newAsyncInstances() *asyncInstances {
	return &asyncInstances{
		instances: make(map[string]sarama.AsyncProducer),
	}
}

func newSyncInstances() *syncInstances {
	return &syncInstances{
		instances: make(map[string]sarama.SyncProducer),
	}
}

func (a *asyncInstances) set(name string, p sarama.AsyncProducer) {
	a.mu.Lock()
	a.instances[name] = p
	a.mu.Unlock()
}
func (a *asyncInstances) get(name string) (sarama.AsyncProducer, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.instances[name]
	return p, ok
}

func (a *asyncInstances) close() {
	a.mu.Lock()
	for _, ins := range a.instances {
		_ = ins.Close()
	}
	a.mu.Unlock()
}

type syncInstances struct {
	mu        sync.RWMutex
	instances map[string]sarama.SyncProducer
}

func (s *syncInstances) set(name string, p sarama.SyncProducer) {
	s.mu.Lock()
	s.instances[name] = p
	s.mu.Unlock()
}
func (s *syncInstances) get(name string) (sarama.SyncProducer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.instances[name]
	return p, ok
}

func (s *syncInstances) close() {
	s.mu.Lock()
	for _, ins := range s.instances {
		_ = ins.Close()
	}
	s.mu.Unlock()
}
