package wal

import (
	"encoding/binary"
	"log"
	"os"

	"github.com/geobeau/laniakea/pkg/mvcc"
	proto "github.com/golang/protobuf/proto"
)

// Wal a Write Ahead Log receiver
type Wal struct {
	receiverChan chan mvcc.Element
	controlChan  chan bool
	file         *os.File
}

// Start the WAL service
func (w *Wal) Start() {
	log.Println("Starting WAL service")
	w.receiverChan = make(chan mvcc.Element, 10)
	w.controlChan = make(chan bool, 10)
	w.prepareFile()
	log.Println(string(w.read().Value))
	go w.appender()
}

// Append an Element to the WAL
func (w *Wal) Append(elem mvcc.Element) {
	w.receiverChan <- elem
}

func (w *Wal) appender() {
	log.Println("WAL Service started")
	var elem mvcc.Element
	for {
		select {
		case elem = <-w.receiverChan:
			log.Println("Received something", elem)
			serializedData, err := serialize(elem)
			if err != nil {
				log.Fatalln(err)
			}
			serializedSize := make([]byte, 4)
			binary.BigEndian.PutUint32(serializedSize, uint32(len(serializedData)))

			w.file.Write(append(serializedSize, serializedData...))
		case <-w.controlChan:
			log.Println("Terminating properly")
			return
		}
	}
}

func (w *Wal) prepareFile() {
	fileName := "data/wal"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}
	w.file = file
}

func (w *Wal) read() mvcc.Element {
	serializedSize := make([]byte, 4)
	w.file.ReadAt(serializedSize, 0)
	size := binary.BigEndian.Uint32(serializedSize)
	serializedElem := make([]byte, size)
	w.file.ReadAt(serializedElem, 4)
	entry := &Entry{}
	proto.Unmarshal(serializedElem, entry)
	return mvcc.Element{
		Timestamp: mvcc.HybridTimestamp{Wall: entry.GetTs().GetWall(), Logical: entry.GetTs().GetLogical()},
		Tombstone: entry.GetTombstone(),
		Key:       entry.GetKey(),
		Value:     entry.GetVal(),
	}
}

func serialize(elem mvcc.Element) ([]byte, error) {
	protoEntry := &Entry{
		Ts:        &HybridTimestamp{Wall: elem.Timestamp.Wall, Logical: elem.Timestamp.Logical},
		Tombstone: elem.Tombstone,
		Key:       elem.Key,
		Val:       elem.Value,
	}
	return proto.Marshal(protoEntry)
}
