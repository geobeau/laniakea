package memtable

import (
	"github.com/geobeau/laniakea/memtable/skiplist"
	"sync"
)

// RollingMemtable contains the dataset in memory
// It is flushed to disk at regular interval
type RollingMemtable struct {
	mutex       *sync.RWMutex
	activeTable memtable
	flushQueue  []memtable
}

// NewRollingMemtable create a new rollingMemtable
func NewRollingMemtable() RollingMemtable {
	return RollingMemtable{
		mutex:       &sync.RWMutex{},
		activeTable: newMemtable(),
	}
}

// Get a key from the memtables
func (m *RollingMemtable) Get(string) ([]byte, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.activeTable.Get(string)
}

// Set a key to the active memtable
func (m *RollingMemtable) Set(string, []byte) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

}

// Delete a key from the active memtable
func (m *RollingMemtable) Delete(string) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

}

type memtable struct {
	skiplist *skiplist.SkipList
}

func newMemtable() memtable {
	return memtable{skiplist.New()}
}

func (m *memtable) get(string) ([]byte, error) {
	m.skiplist.Get()
}

func (m *memtable) set(string, []byte) error {
	m.skiplist.Set()
}

// Delete a key from the active memtable
func (m *memtable) delete(string) error {
	m.skiplist.Remove()
}
