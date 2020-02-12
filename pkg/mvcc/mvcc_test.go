package mvcc

import (
	"reflect"
	"testing"
)

func init() {
	Clock.Start()
}

func TestConstructorIsValid(t *testing.T) {
	elem := NewElement("test", []byte("data"))
	if elem.Timestamp.wall <= 0 {
		t.Error("Timestamp is not properly initialized")
	}
	if elem.Key != "test" || string(elem.Value) != "data" {
		t.Error("Key/Value are different than given")
	}
	if elem.Tombstone == true {
		t.Error("Tombstone is set to true")
	}
}

func TestTombstoneReturnTombstone(t *testing.T) {
	elem := NewTombstone("test")
	if elem.Timestamp.wall <= 0 {
		t.Error("Timestamp is not properly initialized")
	}
	if elem.Key != "test" {
		t.Error("Key/Value are different than given")
	}
	if elem.Tombstone == false {
		t.Error("Tombstone doesn't have tombstone flag")
	}
}

func TestNewElemStackCorrectlyInitializeStack(t *testing.T) {
	elem := NewElement("test", []byte("val"))
	stack := NewElemStack(elem)
	stack.mutex.Lock()
	stack.mutex.Unlock()
	latest := stack.GetLatest()
	if !reflect.DeepEqual(elem, latest) {
		t.Error("Stack is not properly initialized")
	}
}

func TestPushIsFailingIfOlderThanLatest(t *testing.T) {
	elem := NewElement("test", []byte("val"))
	elem2 := NewElement("test", []byte("val"))
	stack := NewElemStack(elem2)
	success := stack.Push(elem)
	if success {
		t.Error("Push should fail is the timestamp is older than the latest")
	}
}

func TestPushSucceedIfNewerThanLast(t *testing.T) {
	elem := NewElement("test", []byte("val"))
	elem2 := NewElement("test", []byte("val"))
	stack := NewElemStack(elem)
	success := stack.Push(elem2)
	latest := stack.GetLatest()
	if !success || !reflect.DeepEqual(elem2, latest) {
		t.Error("Push should fail is the timestamp is older than the latest")
	}
}

func TestGetWithTimestampReturnCorrectElem(t *testing.T) {
	elem1 := NewElement("test", []byte("val"))
	elem2 := NewElement("test", []byte("val"))
	elem3 := NewElement("test", []byte("val"))
	stack := NewElemStack(elem1)
	stack.Push(elem2)
	stack.Push(elem3)
	elem, found := stack.GetWithTimestamp(elem2.Timestamp)

	if !found || !reflect.DeepEqual(elem2, elem) {
		t.Error("GetWithTimestamp return wrong element")
	}
}

func TestGetWithTimestampReturnFalseIfOlder(t *testing.T) {
	elem1 := NewElement("test", []byte("val"))
	elem2 := NewElement("test", []byte("val"))
	elem3 := NewElement("test", []byte("val"))
	stack := NewElemStack(elem1)
	stack.Push(elem2)
	stack.Push(elem3)
	ts := elem1.Timestamp
	ts.wall -= 100
	_, found := stack.GetWithTimestamp(ts)

	if found {
		t.Error("GetWithTimestamp should return false if requested elem is older than oldest")
	}
}

func TestGetWithTimestampReturnFalseIfEmpty(t *testing.T) {
	elem1 := NewElement("test", []byte("val"))
	stack := NewElemStack(elem1)
	stack.stack = []Element{}
	ts := elem1.Timestamp
	_, found := stack.GetWithTimestamp(ts)

	if found {
		t.Error("GetWithTimestamp should return false if stack is empty")
	}
}
