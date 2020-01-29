package main

import (
	"github.com/geobeau/laniakea/storage/btree"
)

func main() {
	bt := btree.Btree{}
	bt.Set("test", "data")
	bt.Get("test")
}
