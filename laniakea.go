package main

import (
	"github.com/geobeau/laniakea/pkg/memtable"
	"github.com/geobeau/laniakea/pkg/mvcc"
)

func main() {
	mvcc.Clock.Start()
	bt := memtable.NewRollingMemtable()
	bt.Set(mvcc.NewElement("test", []byte("hello world")))
	bt.Get("test")
}
