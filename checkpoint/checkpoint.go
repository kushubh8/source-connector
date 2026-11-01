package checkpoint

import (
	"errors"
	"sync"
)

var ErrBackwards = errors.New("checkpoint cannot move backwards")

type Store interface {
	Load(feedID string) int64
	Save(feedID string, position int64) error
}

// MemoryStore is intentionally in-memory, matching the assessment constraints.
// Reuse the same instance across connector recreation to simulate restart recovery.
type MemoryStore struct {
	mu        sync.RWMutex
	positions map[string]int64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{positions: make(map[string]int64)}
}

func (s *MemoryStore) Load(feedID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[feedID]
}

func (s *MemoryStore) Save(feedID string, position int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if position < s.positions[feedID] {
		return ErrBackwards
	}
	s.positions[feedID] = position
	return nil
}
