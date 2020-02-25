package storage

import (
	"github.com/geobeau/laniakea/pkg/memtable"
	"github.com/geobeau/laniakea/pkg/mvcc"
	"github.com/geobeau/laniakea/pkg/wal"
)

// Storage is the backend of the KV/store
type Storage struct {
	memtable memtable.RollingMemtable
	wal      wal.Wal
}

// NewStorage return a new Storage struct
func NewStorage() Storage {
	memtable := memtable.NewRollingMemtable()

	storage := Storage{memtable: memtable, wal: wal.Wal{Memtable: &memtable}}
	storage.wal.Start()
	return storage
}

// Set a key from storage
func (s *Storage) Set(elem mvcc.Element) bool {
	success := s.memtable.Set(elem)
	if success {
		s.wal.Append(elem)
	}
	return success
}

// Get a key from storage
func (s *Storage) Get(key string) (mvcc.Element, bool) {
	return s.memtable.Get(key)
}

// Delete a key from storage
func (s *Storage) Delete(key string) {
	elem := mvcc.NewTombstone(key)
	s.Set(elem)
}

// FlushActive persist to disk the active memtable
func (s *Storage) FlushActive() {
	s.memtable.FlushActive()
}
