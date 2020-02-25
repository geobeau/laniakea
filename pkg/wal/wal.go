package wal

import (
	"encoding/binary"
	"io"
	"log"
	"os"

	"github.com/geobeau/laniakea/pkg/memtable"
	"github.com/geobeau/laniakea/pkg/mvcc"
	proto "github.com/golang/protobuf/proto"
)

// Wal a Write Ahead Log receiver
type Wal struct {
	receiverChan chan mvcc.Element
	controlChan  chan bool
	file         *os.File
	cursor       int64
	Memtable     *memtable.RollingMemtable
}

// Start the WAL service
func (w *Wal) Start() {
	log.Println("Starting WAL service")
	w.receiverChan = make(chan mvcc.Element, 10)
	w.controlChan = make(chan bool, 10)
	w.prepareFile()
	w.recoverFromWal()
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
	w.cursor = 0
}

func (w *Wal) recoverFromWal() {
	log.Println("Recovering from WAL")
	var err error
	var elem mvcc.Element

	for {
		elem, err = w.readNext()
		if err == io.EOF {
			return
		} else if err != nil {
			log.Fatalln("Error while recovering from WAL: ", err)
		}
		w.Memtable.Set(elem)
	}
}

func (w *Wal) readNext() (mvcc.Element, error) {
	serializedSize := make([]byte, 4)
	_, err := w.file.ReadAt(serializedSize, w.cursor)
	if err != nil {
		return mvcc.Element{}, err
	}

	size := binary.BigEndian.Uint32(serializedSize)
	serializedElem := make([]byte, size)
	_, err = w.file.ReadAt(serializedElem, w.cursor+4)
	if err != nil {
		return mvcc.Element{}, err
	}

	entry := &mvcc.ProtoElement{}
	err = proto.Unmarshal(serializedElem, entry)
	if err != nil {
		return mvcc.Element{}, err
	}

	elem := mvcc.Element{
		Timestamp: mvcc.HybridTimestamp{Wall: entry.GetTs().GetWall(), Logical: entry.GetTs().GetLogical()},
		Tombstone: entry.GetTombstone(),
		Key:       entry.GetKey(),
		Value:     entry.GetVal(),
	}

	w.cursor += int64(4 + size)
	return elem, nil
}

func serialize(elem mvcc.Element) ([]byte, error) {
	protoEntry := &mvcc.ProtoElement{
		Ts:        &mvcc.ProtoHybridTimestamp{Wall: elem.Timestamp.Wall, Logical: elem.Timestamp.Logical},
		Tombstone: elem.Tombstone,
		Key:       elem.Key,
		Val:       elem.Value,
	}
	return proto.Marshal(protoEntry)
}
