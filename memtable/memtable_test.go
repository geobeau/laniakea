package memtable

import (
	"testing"

	"github.com/geobeau/laniakea/mvcc"
)

func init() {
	mvcc.Clock.Start()
}

func TestCRUD(t *testing.T) {
	memstore := NewRollingMemtable()

	memstore.Set(mvcc.Element{Key: "key1", Value: []byte("testval1")})
	memstore.Set(mvcc.Element{Key: "key2", Value: []byte("testval2")})
	memstore.Set(mvcc.Element{Key: "key3", Value: []byte("testval3")})
	memstore.Set(mvcc.Element{Key: "key4", Value: []byte("testval4")})
	memstore.Set(mvcc.Element{Key: "key5", Value: []byte("testval5")})
	memstore.Set(mvcc.Element{Key: "key6", Value: []byte("testval6")})

	memstore.Delete("key1")
	memstore.Delete("key3")
	memstore.Delete("key6")

	memstore.Set(mvcc.Element{Key: "key4", Value: []byte("testval40")})
	memstore.Set(mvcc.Element{Key: "key5", Value: []byte("testval50")})
	memstore.Set(mvcc.Element{Key: "key6", Value: []byte("testval60")})

	_, found := memstore.Get("key7")
	if found != false {
		t.Error("Get doesn't report not found on non existing key")
	}

	_, found = memstore.Get("key1")
	if found != false {
		t.Error("Get doesn't report not found on non deleted key")
	}

	elem, found := memstore.Get("key2")
	if found != true || string(elem.Value) != "testval2" {
		t.Errorf("Get doesn't find the correct key: found: %t, vals: %s != %s", found, string(elem.Value), "testval2")
	}

	elem, found = memstore.Get("key4")
	if found != true || string(elem.Value) != "testval40" {
		t.Error("Get doesn't return the last correct data")
	}

	elem, found = memstore.Get("key6")
	if found != true || string(elem.Value) != "testval60" {
		t.Error("Get doesn't return the last correct data for a previously deleted key")
	}
}

func TestGetIsDoneInMultipleMemtable(t *testing.T) {
	memstore := NewRollingMemtable()

	memstore.Set(mvcc.Element{Key: "key1", Value: []byte("testval1")})
	memstore.Set(mvcc.Element{Key: "key2", Value: []byte("testval2")})

	elem, found := memstore.Get("key1")
	if found != true || string(elem.Value) != "testval1" {
		t.Error("Key is not found before the flush")
	}

	memstore.FlushActive()

	elem, found = memstore.Get("key1")
	if found != true || string(elem.Value) != "testval1" {
		t.Error("Key is not found after the flush")
	}

	memstore.Set(mvcc.Element{Key: "key2", Value: []byte("testval20")})
	elem, found = memstore.Get("key2")
	if found != true || string(elem.Value) != "testval20" {
		t.Error("New key in active table is not found")
	}

	memstore.FlushActive()

	elem, found = memstore.Get("key2")
	if found != true || string(elem.Value) != "testval20" {
		t.Error("Key in last flushed table is not correct (possible ordering issue)")
	}
}
