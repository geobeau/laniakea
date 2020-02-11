package main

import (
	"github.com/geobeau/laniakea/memtable"
	"github.com/geobeau/laniakea/mvcc"
)

func main() {
	mvcc.Clock.Start()
	bt := memtable.NewRollingMemtable()
	bt.Set(mvcc.NewElement("test", []byte("hello world")))
	bt.Get("test")
}
