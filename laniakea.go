package main

import (
	"github.com/geobeau/laniakea/memtable"
)

func main() {
	bt := memtable.NewRollingMemtable()
	bt.Set("test", nil)
	bt.Get("test")
}
