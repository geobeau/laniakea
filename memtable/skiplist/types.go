package skiplist

import (
	"math/rand"
	"sync"
)

type elementNode struct {
	next []*Element
}

// Element represent the key-value pair that is inserted in the skiplist
type Element struct {
	elementNode
	key   string
	value interface{}
}

// Key allows retrieval of the key for a given Element
func (e *Element) Key() string {
	return e.key
}

// Value allows retrieval of the value for a given Element
func (e *Element) Value() interface{} {
	return e.value
}

// Next returns the following Element or nil if we're at the end of the list.
// Only operates on the bottom level of the skip list (a fully linked list).
func (e *Element) Next() *Element {
	return e.next[0]
}

// SkipList is the implementation of a skiplist
type SkipList struct {
	elementNode
	maxLevel       int
	Length         int
	randSource     rand.Source
	probability    float64
	probTable      []float64
	mutex          sync.RWMutex
	prevNodesCache []*elementNode
}
