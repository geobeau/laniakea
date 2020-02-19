package sstable

import (
	"log"
	"os"

	"github.com/geobeau/laniakea/pkg/mvcc"
)

type SSTableBuilder struct {
	dataFile    *os.File
	indexFile   *os.File
	dataCursor  int64
	indexCursor int64
}

// ElemStackReader read a stream of elementStacks
type ElemStackReader interface {
	readNext() mvcc.ElemStack
}

func FlushToSSTable() {
	builder := SSTableBuilder{}
	builder.prepareFiles()
}

func (s *SSTableBuilder) prepareFiles() {
	fileName := "data/sstable.data"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}
	s.dataFile = file
	s.dataCursor = 0

	fileName = "data/sstable.index"
	file, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}
	s.dataFile = file
	s.dataCursor = 0
}
