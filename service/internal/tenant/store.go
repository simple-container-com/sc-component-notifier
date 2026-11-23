// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package tenant

import (
	"context"
	"sync"
)

// Store is the tenant repository interface. Two implementations:
// BlobStore (production, in store_blob.go) and InMemoryStore (tests).
//
// There is no Mongo implementation, deliberately. The scaffold this repo
// was copied from had one, and it was removed rather than carried: a
// notifier tracks a handful of tenants and a few events a day, which
// does not justify a paid cluster, and sc-component-observatory runs the
// same shape on object storage alone. `storage: <resource-name>` stays
// the operator's ONLY storage knob.
type Store interface {
	Upsert(ctx context.Context, record Record) error
	List(ctx context.Context) ([]Record, error)
	Get(ctx context.Context, stackName string) (Record, bool, error)
	SoftDelete(ctx context.Context, stackName string) error
	Close(ctx context.Context) error
}

// -------------------- In-memory implementation (tests) --------------------

// InMemoryStore is for tests — a map behind a mutex. No persistence.
type InMemoryStore struct {
	mu   sync.RWMutex
	rows map[string]Record
}

// NewInMemoryStore returns an empty store, for tests.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{rows: map[string]Record{}}
}

func (s *InMemoryStore) Upsert(_ context.Context, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[record.StackName] = record
	return nil
}

func (s *InMemoryStore) List(_ context.Context) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Record, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	return out, nil
}

func (s *InMemoryStore) Get(_ context.Context, stackName string) (Record, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rows[stackName]
	return r, ok, nil
}

func (s *InMemoryStore) SoftDelete(_ context.Context, stackName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rows[stackName]; ok {
		r.SoftDeleted = true
		s.rows[stackName] = r
	}
	return nil
}

func (s *InMemoryStore) Close(_ context.Context) error { return nil }
