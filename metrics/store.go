package metrics

import (
	"fmt"
	"sync"
)

type Store interface {
	Add(m Metrics, name string) error
	Get(name string) Metrics
	Names() []string
}

type memoryStore struct {
	metrics map[string]Metrics
	*sync.RWMutex
}

func NewMemoryStore() Store {
	return &memoryStore{
		metrics: map[string]Metrics{},
		RWMutex: &sync.RWMutex{},
	}
}

func (s *memoryStore) Add(m Metrics, name string) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.metrics[name]; ok {
		return fmt.Errorf("metrics for %s already stored", name)
	}
	s.metrics[name] = m
	return nil
}

func (s *memoryStore) Get(name string) Metrics {
	s.RLock()
	defer s.RUnlock()
	return s.metrics[name]
}

func (s *memoryStore) Names() []string {
	s.RLock()
	names := make([]string, 0, len(s.metrics))
	for k := range s.metrics {
		names = append(names, k)
	}
	s.RUnlock()
	return names
}
