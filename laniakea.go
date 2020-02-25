package main

import (
	"time"

	"github.com/geobeau/laniakea/pkg/mvcc"
	"github.com/geobeau/laniakea/pkg/storage"
)

func main() {
	mvcc.Clock.Start()
	sto := storage.NewStorage()
	sto.Set(mvcc.NewElement("test", []byte("hello world")))
	sto.Get("test")
	sto.FlushActive()
	time.Sleep(100 * time.Second)
}
