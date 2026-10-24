package upstream

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// leave() loads the drain signal after its count reached zero; a pick that
// succeeds and a Drain that starts in between make that stale leave close the
// new drain signal while the new lease is still in flight.
func TestDrainNeverClosesEarly(t *testing.T) {
	p := New[int](Config{})
	defer p.Close()
	if err := p.Put(Target[int]{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	var holding [4]atomic.Int32
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := range holding {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				l, err := p.Pick()
				if err != nil {
					runtime.Gosched()
					continue
				}
				holding[g].Store(1)
				holding[g].Store(0)
				l.Release(Success)
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	early := 0
	iters := 0
	for time.Now().Before(deadline) && early == 0 {
		iters++
		ch, _ := p.Drain("a")
		<-ch.Done()
		for g := range holding {
			if holding[g].Load() == 1 {
				early++
			}
		}
		p.Resume("a")
	}
	stop.Store(true)
	wg.Wait()
	if early > 0 {
		t.Fatalf("drain channel closed while a lease was still held (after %d drains)", iters)
	}
	t.Logf("%d drains, no early close observed", iters)
}
