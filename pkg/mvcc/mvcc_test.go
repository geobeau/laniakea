package mvcc

import (
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
