package memtable

import (
	"container/list"
	"sync"

	"github.com/geobeau/laniakea/memtable/skiplist"
)

// RollingMemtable contains the dataset in memory
// It is flushed to disk at regular interval
type RollingMemtable struct {
	mutex       *sync.RWMutex
	activeTable *memtable
	flushQueue  *list.List
}

// NewRollingMemtable create a new rollingMemtable
func NewRollingMemtable() RollingMemtable {
	return RollingMemtable{
		mutex:       &sync.RWMutex{},
		activeTable: newMemtable(),
		flushQueue:  list.New(),
	}
}

func (m *RollingMemtable) flushActive() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.flushQueue.PushFront(m.activeTable)
	m.activeTable = newMemtable()
}

// Get a key from the memtables
func (m *RollingMemtable) Get(key string) (Element, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	elem, found := m.activeTable.get(key)
	if found {
		return elem, found
	}
	for e := m.flushQueue.Front(); e != nil; e = e.Next() {
		table := e.Value.(memtable)
		table.get(key)
		if found {
			return elem, found
		}
	}
	return Element{}, false
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

func newMemtable() *memtable {
	return &memtable{skiplist.New()}
}

func (m *memtable) get(key string) (Element, bool) {
	elem := m.skiplist.Get(key)
	if elem == nil {
		return Element{}, false
	}
	return elem.Value().(Element), true
}

func (m *memtable) set(key string, value Element) {
	m.skiplist.Set(key, value)
}

func (m *memtable) delete(key string) {
	// TODO: Implement tombstone
	elem := Element{tombstone: true}
	m.skiplist.Set(key, elem)
}
