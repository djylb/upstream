package health

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

// TestScheduleOneTimer checks that targets on different intervals share
// one timer, are each checked on their own interval, and that removing
// them all stops the timer.
func TestScheduleOneTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		const n = 12
		for i := range n {
			tg := h.sw.target(strconv.Itoa(i))
			tg.Options.Interval = time.Duration(i+1) * time.Second
			h.c.Put(strconv.Itoa(i), tg)
		}
		synctest.Wait()
		if p := h.clk.Pending(); p != 1 {
			t.Fatalf("%d timers for %d targets, want 1", p, n)
		}
		const secs = 30
		h.tick(secs)
		for i := range n {
			want := 1 + secs/(i+1)
			if got := h.sw.count(strconv.Itoa(i)); got != want {
				t.Errorf("target %d (every %ds): %d checks, want %d", i, i+1, got, want)
			}
		}
		if p := h.clk.Pending(); p != 1 {
			t.Fatalf("%d timers after %ds, want 1", p, secs)
		}
		for i := range n {
			h.c.Remove(strconv.Itoa(i))
		}
		synctest.Wait()
		if p := h.clk.Pending(); p != 0 {
			t.Fatalf("%d timers with no targets", p)
		}
	})
}

// countingClock counts the timers made.
type countingClock struct {
	*fakeclock.Clock
	timers atomic.Int64
}

func (c *countingClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.timers.Add(1)
	return c.Clock.AfterFunc(d, f)
}

// TestScheduleKeepsEarlierTimer checks that scheduling a check due after
// the armed timer makes no new timer: many targets on one interval cost
// one timer per round, not one per target.
func TestScheduleKeepsEarlierTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := &countingClock{Clock: fakeclock.New()}
		sw := newSwitchable()
		c := New[string, string](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Second, Jitter: -1}})
		defer c.Close()
		const n = 50
		for i := range n {
			c.Put(strconv.Itoa(i), sw.target(strconv.Itoa(i)))
		}
		synctest.Wait()
		before := clk.timers.Load()
		const rounds = 5
		for range rounds {
			clk.Advance(time.Second)
			synctest.Wait()
		}
		if got := clk.timers.Load() - before; got > 2*rounds {
			t.Fatalf("%d timers for %d rounds of %d targets", got, rounds, n)
		}
		if got := sw.count("7"); got != 1+rounds {
			t.Fatalf("%d checks, want %d", got, 1+rounds)
		}
	})
}

func TestSingleSlotLeavesWaitingTargetsScheduled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		probe := func(ctx context.Context, _ Target) (Observation, error) {
			<-ctx.Done()
			return Observation{}, ctx.Err()
		}
		c := New[int, int](t.Context(), Config[int]{Clock: clk, Concurrency: 1})
		defer c.Close()
		for i := range 512 {
			c.Put(i, Target{Probe: probe})
		}
		synctest.Wait()
		if len(c.sched) != 511 || c.active != 1 {
			t.Fatalf("scheduled %d active %d", len(c.sched), c.active)
		}
		c.Close()
		<-c.Done()
	})
}

func TestSyncPreservesJitteredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		target := h.sw.target("a")
		target.Options.Jitter = time.Second
		set := map[string]Target{"a": target}
		h.c.SyncGroup("g", set)
		synctest.Wait()
		e := h.c.entries["a"]
		revision, due := e.timer, h.c.sched[e.slot-1].due
		h.c.SyncGroup("g", set)
		synctest.Wait()
		if revision != e.timer || !due.Equal(h.c.sched[e.slot-1].due) {
			t.Fatal("unchanged target rescheduled")
		}
	})
}
