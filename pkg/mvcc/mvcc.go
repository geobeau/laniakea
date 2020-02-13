package mvcc

import "sync"

// Element is a MVCC element composed of a key/value, an hybridTime timestamp
// and a tombstone flag
type Element struct {
	Timestamp HybridTimestamp
	Key       string
	Value     []byte
	Tombstone bool
}

// NewElement return a new element with a timestamp at now()
func NewElement(key string, value []byte) Element {
	return Element{Clock.now(), key, value, false}
}

// NewTombstone return a tombstone for a key
func NewTombstone(key string) Element {
	return Element{Clock.now(), key, nil, true}
}

// ElemStack is a stack of element
// it stores the multiple version of an element
type ElemStack struct {
	stack []Element
	mutex *sync.RWMutex
}

// NewElemStack returns a ElemStack with one Element
func NewElemStack(elem Element) ElemStack {
	return ElemStack{stack: []Element{elem}, mutex: &sync.RWMutex{}}
}

// GetWithTimestamp Get the newest element before a specific timestamp
func (es *ElemStack) GetWithTimestamp(ht HybridTimestamp) (Element, bool) {
	es.mutex.RLock()
	defer es.mutex.RUnlock()
	if len(es.stack) == 0 {
		return Element{}, false
	}

	// If the element that we search is before the oldest element of the stack
	// We return false
	if !ht.after(es.stack[0].Timestamp) {
		return Element{}, false
	}

	// TODO: Binary search is possible since records should be appended in order
	for i := len(es.stack) - 1; i >= 0; i-- {
		elem := es.stack[i]
		if !elem.Timestamp.after(ht) {
			return elem, true
		}
	}
	return Element{}, false
}

// GetLatest get the latest element of the stack
func (es *ElemStack) GetLatest() Element {
	es.mutex.RLock()
	defer es.mutex.RUnlock()
	return es.stack[len(es.stack)-1]
}

// Push a new element to the stack
// The new element needs to be more recent that the latest
func (es *ElemStack) Push(elem Element) bool {
	es.mutex.Lock()
	defer es.mutex.Unlock()
	if len(es.stack) > 0 {
		latest := es.stack[len(es.stack)-1]
		if latest.Timestamp.after(elem.Timestamp) {
			return false
		}
	}

	es.stack = append(es.stack, elem)
	return true
}
