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
func (m *RollingMemtable) Get(key string) Element {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.activeTable.get(key)
}

// Set a key to the active memtable
func (m *RollingMemtable) Set(key string, value Element) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.activeTable.set(key, value)
}

// Delete a key from the active memtable
func (m *RollingMemtable) Delete(key string) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	m.activeTable.delete(key)
}

type memtable struct {
	skiplist *skiplist.SkipList
}

// Element is an element containing the value of an object and flags
type Element struct {
	tombstone bool
	Value     []byte
}

func newMemtable() memtable {
	return memtable{skiplist.New()}
}

func (m *memtable) get(key string) Element {
	elem := m.skiplist.Get(key)
	return elem.Value().(Element)
}

func (m *memtable) set(key string, value Element) {
	m.skiplist.Set(key, value)
}

func (m *memtable) delete(key string) {
	// TODO: Implement tombstone
	elem := Element{tombstone: true}
	m.skiplist.Set(key, elem)
}
