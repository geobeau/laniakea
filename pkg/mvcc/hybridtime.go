package mvcc

import (
	"sync"
	"time"
)

type hybridtime struct {
	lastWall    int64
	nextLogical int64
	mutex       *sync.Mutex
}

type HybridTimestamp struct {
	Wall    int64
	Logical int64
}

// Clock is the global state of the clock
var Clock *hybridtime

func (ht *hybridtime) Start() {
	Clock = &hybridtime{0, 0, &sync.Mutex{}}
}

func (ht *hybridtime) now() HybridTimestamp {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	curPhysical := time.Now().UnixNano()
	if ht.lastWall < curPhysical {
		ht.lastWall = curPhysical
		ht.nextLogical = 1
		return HybridTimestamp{Wall: curPhysical, Logical: 0}
	}
	ht.nextLogical++
	return HybridTimestamp{Wall: ht.lastWall, Logical: ht.nextLogical - 1}
}

func (ht *hybridtime) update(ts HybridTimestamp) {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	if ht.lastWall < ts.Wall {
		ht.lastWall = ts.Wall
		ht.nextLogical = ts.Logical + 1
		return
	}
	return
}

func (ht *HybridTimestamp) after(ts HybridTimestamp) bool {
	if ht.Wall > ts.Wall {
		return true
	} else if ht.Wall < ts.Wall {
		return false
	}
	// If both Wall are equal we compare the Logical part
	if ht.Logical > ts.Logical {
		return true
	}
	return false
}
