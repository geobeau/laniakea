package mvcc

// Element is a MVCC element composed of a key/value, an hybridTime timestamp
// and a tombstone flag
type Element struct {
	Timestamp hybridTimestamp
	Key       string
	Value     []byte
	Tombstone bool
}

// NewElement return a new element with a timestamp at now()
func NewElement(key string, value []byte) Element {
	return Element{Clock.now(), key, value, false}
}
