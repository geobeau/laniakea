package main

import (
	"github.com/geobeau/laniakea/memtable"
	"github.com/geobeau/laniakea/mvcc"
)

func main() {
	mvcc.Clock.Start()
	bt := memtable.NewRollingMemtable()
	bt.Set("test", memtable.Element{})
	bt.Get("test")
}
