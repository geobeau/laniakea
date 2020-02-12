package mvcc

import (
	"testing"
)

func init() {
	Clock.Start()
}

func TestClockIsMonotonicAfterUpdate(t *testing.T) {
	ts := Clock.now()
	oldWall := Clock.lastWall
	ts.wall = 0
	Clock.update(ts)
	if Clock.lastWall != oldWall {
		t.Error("It's possible to change the value of the clock with a past timestamp")
	}
}

func TestClockIsUpdatable(t *testing.T) {
	ts := Clock.now()
	oldWall := Clock.lastWall
	ts.wall = oldWall + 1000*1000*1000*1000*1000
	ts.logical = 10
	Clock.update(ts)
	if Clock.lastWall != ts.wall {
		t.Error("Wall clock is not properly updated")
	}
	if Clock.nextLogical != int64(11) {
		t.Error("Logical clock is not properly updated")
	}
}

func TestClockConcurrency(t *testing.T) {
	workerNum := 100
	chanResult := make(chan bool)
	worker := func() {
		oldCurtime := Clock.now()
		for i := 0; i < 1000; i++ {
			curtime := Clock.now()
			if oldCurtime.after(curtime) {
				chanResult <- false
				break
			}
		}
		chanResult <- true
	}

	for i := 0; i < workerNum; i++ {
		go worker()
	}
	for i := 0; i < workerNum; i++ {
		if <-chanResult == false {
			t.Error("Concurrency issues, clock went into past")
		}
	}
}

func TestAfter(t *testing.T) {
	ht1 := hybridTimestamp{1, 1}
	ht2 := hybridTimestamp{1, 2}
	ht3 := hybridTimestamp{2, 0}
	if ht1.after(ht2) {
		t.Error("After return true if it's before (logical clock)")
	}
	if ht2.after(ht3) {
		t.Error("After return true if it's before (wall clock)")
	}
	if !ht2.after(ht1) {
		t.Error("After return false if it's after (logical clock)")
	}
	if !ht3.after(ht2) {
		t.Error("After return false if it's after (wall clock)")
	}
}
