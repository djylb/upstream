package health

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
)

// Two owned goroutines (two probes) calling Close at the same time each wait
// for the other to end: len(owned) stays 2 > min(owned[self],1).
func TestConcurrentCloseFromTwoProbes(t *testing.T) {
	var c *Checker[string, string]
	var barrier sync.WaitGroup
	barrier.Add(2)
	done := make(chan struct{}, 2)
	probe := func(ctx context.Context, _ Target) (Observation, error) {
		barrier.Done()
		barrier.Wait()
		c.Close()
		done <- struct{}{}
		return Observation{}, nil
	}
	c = New[string, string](context.Background(), Config[string]{Concurrency: 4})
	c.Set("a", Target{Probe: probe, Address: "a"})
	c.Set("b", Target{Probe: probe, Address: "b"})
	for range 2 {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("deadlock: two probes calling Close wait for each other")
		}
	}
	waitDone(t, c)
}

func waitDone(t *testing.T, c *Checker[string, string]) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed")
	}
}

// A callback and a probe calling Close at the same time.
func TestCloseFromCallbackAndProbe(t *testing.T) {
	var c *Checker[string, string]
	inProbe := make(chan struct{})
	done := make(chan struct{}, 2)
	probe := func(ctx context.Context, tg Target) (Observation, error) {
		if tg.Address == "slow" {
			close(inProbe)
			time.Sleep(100 * time.Millisecond) // let the callback reach Close
			c.Close()
			done <- struct{}{}
		}
		return Observation{}, nil
	}
	c = New[string, string](context.Background(), Config[string]{Concurrency: 4, OnChange: func(r Report[string]) {
		if r.Key == "fast" {
			<-inProbe
			c.Close()
			done <- struct{}{}
		}
	}})
	c.Set("slow", Target{Probe: probe, Address: "slow"})
	c.Set("fast", Target{Probe: probe, Address: "fast"})
	for range 2 {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("deadlock: callback and probe calling Close wait for each other")
		}
	}
	waitDone(t, c)
}

// Per-target Interval: the documented defaults (UnhealthyInterval =
// Interval, Timeout = min(5s, Interval), Jitter = Interval/10) are taken from
// the Config's resolved values instead.
func TestPerTargetIntervalDefaults(t *testing.T) {
	c := New[string, string](context.Background(), Config[string]{Options: Options{Interval: 10 * time.Second, Disabled: true}})
	defer c.Close()
	c.Set("a", Target{Address: "x:1", Options: Options{Interval: time.Second}})
	c.mu.Lock()
	o := c.optionsLocked(c.entries["a"])
	c.mu.Unlock()
	if o.UnhealthyInterval != time.Second {
		t.Errorf("UnhealthyInterval = %v, documented default is Interval (1s)", o.UnhealthyInterval)
	}
	if o.Jitter > 100*time.Millisecond {
		t.Errorf("Jitter = %v, documented default is Interval/10 (100ms)", o.Jitter)
	}
	if o.Timeout > time.Second {
		t.Errorf("Timeout = %v, documented default is min(5s, Interval) (1s)", o.Timeout)
	}
}

func TestDoneAndWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		probe := func(ctx context.Context, _ Target) (Observation, error) {
			<-gate // ignores ctx on purpose: Done must wait for it
			return Observation{}, nil
		}
		c := New[string, string](t.Context(), Config[string]{Clock: fakeclock.New()})
		c.Set("a", Target{Probe: probe})
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := c.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait on a running checker: %v", err)
		}
		c.Close() // returns at once although the probe hangs
		select {
		case <-c.Done():
			t.Fatal("Done closed while a probe was running")
		default:
		}
		close(gate)
		if err := c.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

// TestConcurrentCloseEverywhere closes the checker from several probes,
// callbacks and outside goroutines at once.
func TestConcurrentCloseEverywhere(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *Checker[string, string]
		var arrived atomic.Int32
		all := make(chan struct{})
		probe := func(ctx context.Context, _ Target) (Observation, error) {
			if arrived.Add(1) == 4 {
				close(all)
			}
			select { // an outside Close may cancel probes before all arrive
			case <-all:
			case <-ctx.Done():
			}
			c.Close()
			return Observation{}, nil
		}
		c = New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), OnResult: func(Report[string]) { c.Close() }})
		for _, k := range []string{"a", "b", "c", "d"} {
			c.Set(k, Target{Probe: probe})
		}
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(c.Close)
		}
		wg.Wait()
		<-c.Done()
	})
}

func TestWatchEndsWithPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "a"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		clk := fakeclock.New()
		c := Watch(t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: clk})
		synctest.Wait()
		p.Close()
		<-c.Done()
		if c.Len() != 0 || clk.Pending() != 0 {
			t.Fatalf("checker of a closed pool: %d targets, %d timers", c.Len(), clk.Pending())
		}
	})
}

func TestWatchMatchesGenerations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "x"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		c := Watch(t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: fakeclock.New()})
		defer c.Close()
		synctest.Wait()
		old, _ := c.Status("a")
		// Remove and re-add with the same address: a new generation.
		p.Remove("a")
		if err := p.Put(upstream.Target[string]{ID: "a", Value: "x"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		cur, _ := c.Status("a")
		if cur.Generation == old.Generation {
			t.Fatal("re-added member kept its generation")
		}
		c.mu.Lock()
		sink := c.sink
		c.mu.Unlock()
		sink(Report[string]{Result: Result{Health: down, Changed: true}, Key: "a", Target: old.Target, Generation: old.Generation})
		if b, _ := p.Backend("a"); b.Status().Health == down {
			t.Fatal("a report of the old generation reached the re-added member")
		}
	})
}
