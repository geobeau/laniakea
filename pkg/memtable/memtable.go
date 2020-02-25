package memtable

import (
	"container/list"
	"sync"

	"github.com/geobeau/laniakea/pkg/memtable/skiplist"
	"github.com/geobeau/laniakea/pkg/mvcc"
	"github.com/geobeau/laniakea/pkg/sstable"
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

// FlushActive add the active memtable and add it to the flush queue
func (m *RollingMemtable) FlushActive() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.flushQueue.PushFront(m.activeTable)
	oldTable := m.activeTable
	m.activeTable = newMemtable()

	// TODO: Should async
	oldTable.flushToSSTable()
}

// Get a key from the memtables
func (m *RollingMemtable) Get(key string) (mvcc.Element, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	elem, found := m.activeTable.get(key)
	if found {
		return elem, !elem.Tombstone
	}
	for e := m.flushQueue.Front(); e != nil; e = e.Next() {
		table := e.Value.(*memtable)
		elem, found = table.get(key)
		if found {
			return elem, !elem.Tombstone
		}
	}
	return mvcc.Element{}, false
}

// Set a key to the active memtable
func (m *RollingMemtable) Set(elem mvcc.Element) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.activeTable.set(elem)
}

type memtable struct {
	skiplist *skiplist.SkipList
}

func newMemtable() *memtable {
	return &memtable{skiplist.New()}
}

func (m *memtable) get(key string) (mvcc.Element, bool) {
	data := m.skiplist.Get(key)
	if data == nil {
		return mvcc.Element{}, false
	}
	elemStack := data.Value().(*mvcc.ElemStack)
	elem := elemStack.GetLatest()
	// If we find a tombstoned value, return that we didn't find it
	// TODO: should be improved
	return elem, true
}

func (m *memtable) set(elem mvcc.Element) bool {
	data := m.skiplist.Get(elem.Key)

	if data == nil {
		newStack := mvcc.NewElemStack(elem)
		m.skiplist.Set(elem.Key, &newStack)
		return true
	}
	stack := data.Value().(*mvcc.ElemStack)
	return stack.Push(elem)
}

func (m *memtable) flushToSSTable() {
	sstable.FlushToSSTable(newMemtableReader(m))
}

// MemtableReader Read read all elements of a table in order
type memtableReader struct {
	cur *skiplist.Element
}

func newMemtableReader(m *memtable) *memtableReader {
	return &memtableReader{cur: m.skiplist.Front()}
}

func (mr *memtableReader) ReadNext() *mvcc.ElemStack {
	elemStack := mr.cur.Value().(*mvcc.ElemStack)
	mr.cur = mr.cur.Next()
	return elemStack
}
