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

type hybridTimestamp struct {
	wall    int64
	logical int64
}

// Clock is the global state of the clock
var Clock *hybridtime

func (ht *hybridtime) Start() {
	Clock = &hybridtime{0, 0, &sync.Mutex{}}
}

func (ht *hybridtime) now() hybridTimestamp {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	curPhysical := time.Now().UnixNano()
	if ht.lastWall < curPhysical {
		ht.lastWall = curPhysical
		ht.nextLogical = 1
		return hybridTimestamp{wall: curPhysical, logical: 0}
	}
	ht.nextLogical++
	return hybridTimestamp{wall: ht.lastWall, logical: ht.nextLogical - 1}
}

func (ht *hybridtime) update(ts hybridTimestamp) {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	if ht.lastWall < ts.wall {
		ht.lastWall = ts.wall
		ht.nextLogical = ts.logical + 1
		return
	}
	return
}

func (ht *hybridTimestamp) after(ts hybridTimestamp) bool {
	if ht.wall > ts.wall {
		return true
	} else if ht.wall < ts.wall {
		return false
	}
	// If both wall are equal we compare the logical part
	if ht.logical > ts.logical {
		return true
	}
	return false
}
