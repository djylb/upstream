package leakcheck

import (
	"strings"
	"testing"
	"time"
)

// No VerifyTestMain here: TestFindMarksUnreachable leaks on purpose.

// running returns the ids of the goroutines running now: the tests may run
// in any order, and TestFindMarksUnreachable leaves one behind.
func running() map[uint64]bool {
	ids := map[uint64]bool{}
	for _, g := range goroutines() {
		ids[g.id] = true
	}
	return ids
}

func TestFind(t *testing.T) {
	base := running()
	if err := find(0, base); err != nil {
		t.Fatalf("find at rest: %v", err)
	}
	stop := make(chan struct{})
	go func() { <-stop }()
	err := find(10*time.Millisecond, base)
	if err == nil || !strings.Contains(err.Error(), "TestFind") {
		t.Fatalf("find with a running goroutine = %v, want it listed", err)
	}
	close(stop)
	if err := find(settleWait, base); err != nil {
		t.Fatalf("find after stop: %v", err)
	}
}

func TestFindMarksUnreachable(t *testing.T) {
	go func() {
		ch := make(chan int)
		<-ch // no other goroutine can reach ch: leaked for good
	}()
	err := find(10*time.Millisecond, nil)
	if err == nil {
		t.Fatal("find with a leaked goroutine = nil")
	}
	if !strings.Contains(err.Error(), "goroutineleak profile") {
		t.Skipf("runtime without the goroutineleak profile:\n%v", err)
	}
}

func TestParse(t *testing.T) {
	g, ok := parse("goroutine 7 [chan receive]:\nexample.com/p.f(0x1)\n\t/p/f.go:3 +0x1")
	if !ok || g.id != 7 || g.top != "example.com/p.f" {
		t.Fatalf("parse = %+v, %v", g, ok)
	}
	if _, ok := parse("not a goroutine"); ok {
		t.Fatal("parse accepted garbage")
	}
}
