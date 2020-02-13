package main

import (
	"github.com/geobeau/laniakea/pkg/mvcc"
	"github.com/geobeau/laniakea/pkg/storage"
	"time"
)

func main() {
	mvcc.Clock.Start()
	sto := storage.NewStorage()
	sto.Set(mvcc.NewElement("test", []byte("hello world")))
	sto.Get("test")
	time.Sleep(100 * time.Second)
}
