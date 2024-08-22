package entity

import (
	"github.com/IBM/sarama"
	"sync"
)

// ProducerPool 生产者池子
type ProducerPool interface {
	SetProducer(conf *ProducerConfig) error //会覆盖
	GetAsyncProducer(name string) (*Producer, error)
	GetSyncProducer(name string) (*Producer, error)
	Close()
}

var _pI ProducerPool

// NewProducerInstancesPool 自定义使用
func NewProducerInstancesPool() ProducerPool {
	return &producerInstances{
		async: newAsyncInstances(),
		sync:  newSyncInstances(),
	}
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
	if pO, ok := a.instances[name]; ok {
		_ = pO.Close()
	}
	a.instances[name] = p
	a.mu.Unlock()
}

func (a *asyncInstances) add(name string, p sarama.AsyncProducer) {
	a.mu.Lock()
	if _, ok := a.instances[name]; !ok {
		a.instances[name] = p
	}
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
	if pO, ok := s.instances[name]; ok {
		_ = pO.Close()
	}
	s.instances[name] = p
	s.mu.Unlock()
}

func (s *syncInstances) add(name string, p sarama.SyncProducer) {
	s.mu.Lock()
	if _, ok := s.instances[name]; !ok {
		s.instances[name] = p
	}
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
