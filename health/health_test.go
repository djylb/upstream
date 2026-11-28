package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/djylb/upstream"
)

var errDown = errors.New("down")

// switchable is a probe whose result per address the test sets.
type switchable struct {
	mu      sync.Mutex
	fail    map[string]bool
	latency map[string]time.Duration
	calls   map[string]int
}

func newSwitchable() *switchable {
	return &switchable{fail: map[string]bool{}, latency: map[string]time.Duration{}, calls: map[string]int{}}
}

func (s *switchable) set(addr string, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[addr] = fail
}

func (s *switchable) count(addr string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[addr]
}

func (s *switchable) probe(_ context.Context, t Target) (Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[t.Address]++
	obs := Observation{Addr: "resolved-" + t.Address, Latency: s.latency[t.Address]}
	if s.fail[t.Address] {
		return obs, errDown
	}
	return obs, nil
}

func (s *switchable) target(addr string) Target { return Target{Probe: s.probe, Address: addr} }

type reports struct {
	mu   sync.Mutex
	list []Report[string]
}

func (r *reports) add(rep Report[string]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, rep)
}

// changes returns the health of every edge so far.
func (r *reports) changes() []upstream.Health {
	r.mu.Lock()
	defer r.mu.Unlock()
	var c []upstream.Health
	for _, rep := range r.list {
		if rep.Changed() {
			c = append(c, rep.Health)
		}
	}
	return c
}

func (r *reports) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.list)
}

const (
	up      = upstream.HealthUp
	down    = upstream.HealthDown
	unknown = upstream.HealthUnknown
)

type harness struct {
	clk *timeshim.Fake
	sw  *switchable
	rep *reports
	c   *Checker[string, string]
}

// newHarness starts a checker with a fake clock and a 1s interval without
// jitter.
func newHarness(t *testing.T, cfg Config[string]) *harness {
	t.Helper()
	h := &harness{clk: timeshim.NewFake(time.Unix(0, 0)), sw: newSwitchable(), rep: new(reports)}
	cfg.Clock = h.clk
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	if cfg.Jitter == 0 {
		cfg.Jitter = -1
	}
	if cfg.OnResult == nil {
		cfg.OnResult = h.rep.add
	}
	h.c = New[string, string](t.Context(), cfg)
	t.Cleanup(h.c.Close)
	return h
}

// runDue drives zero-delay timers and gives their probes and callbacks
// time to finish or block, without advancing beyond the current time.
func runDue(clk *timeshim.Fake) {
	for {
		synctest.Wait()
		at, ok := clk.Next()
		if !ok || at.After(clk.Now()) {
			return
		}
		clk.Advance(0)
	}
}

func (h *harness) tick(n int) {
	for range n {
		h.clk.Advance(time.Second)
		runDue(h.clk)
	}
}

func TestRiseAndFall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Rise: 2, Fall: 3}})
		h.c.Put("a", h.sw.target("a"))
		if s, _ := h.c.Status("a"); s.Health != unknown && s.Checks != 0 {
			t.Fatalf("status before the first check: %+v", s)
		}
		runDue(h.clk)
		// The first check decides at once.
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up}) {
			t.Fatalf("changes %v after the first check", got)
		}
		h.sw.set("a", true)
		h.tick(2)
		if got := h.rep.changes(); len(got) != 1 {
			t.Fatalf("went down after 2 failures, fall is 3: %v", got)
		}
		h.tick(1)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down}) {
			t.Fatalf("changes %v after 3 failures", got)
		}
		h.tick(20) // many more failures, no more transitions
		if got := h.rep.changes(); len(got) != 2 {
			t.Fatalf("repeated down reports: %v", got)
		}
		// Pass, fail, pass is not two consecutive passes.
		for _, fail := range []bool{false, true, false} {
			h.sw.set("a", fail)
			h.tick(1)
		}
		if got := h.rep.changes(); len(got) != 2 {
			t.Fatalf("came up without 2 consecutive passes: %v", got)
		}
		h.tick(1)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down, up}) {
			t.Fatalf("changes %v after 2 passes", got)
		}
		s, ok := h.c.Status("a")
		if !ok || s.Health != up || s.Last.Err != nil || !s.Last.Time.Equal(h.clk.Now()) || s.Checks != 28 || s.Failures != 24 || !s.Checking {
			t.Fatalf("status %+v", s)
		}
		if s.Last.Addr != "resolved-a" || s.Last.Reason != ReasonNone {
			t.Fatalf("last result %+v", s.Last)
		}
		if _, ok := h.c.Status("zz"); ok {
			t.Fatal("status of an unknown key")
		}
	})
}

func TestFirstFailureDecides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		h.sw.set("a", true)
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{down}) {
			t.Fatalf("changes %v", got)
		}
		if s, _ := h.c.Status("a"); s.Last.Reason != ReasonOther || !errors.Is(s.Last.Err, errDown) {
			t.Fatalf("last %+v", s.Last)
		}
	})
}

func TestUncheckedNetworks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		h.c.Put("dns", Target{Network: "udp", Address: "192.0.2.1:53"})
		h.c.Put("custom", Target{Network: "udp", Address: "x", Probe: h.sw.probe})
		runDue(h.clk)
		s, _ := h.c.Status("dns")
		if s.Checking || s.Health != unknown || s.Checks != 0 {
			t.Fatalf("UDP target without a probe: %+v", s)
		}
		if s, _ := h.c.Status("custom"); !s.Checking || s.Health != up {
			t.Fatalf("UDP target with a probe: %+v", s)
		}
		if h.clk.Pending() != 1 {
			t.Fatalf("%d timers, want only the custom check's", h.clk.Pending())
		}
	})
}

func TestIntervals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Interval: 10 * time.Second, UnhealthyInterval: time.Second}})
		h.sw.set("down", true)
		h.c.Put("up", h.sw.target("up"))
		h.c.Put("down", h.sw.target("down"))
		runDue(h.clk)
		h.tick(10)
		if up, down := h.sw.count("up"), h.sw.count("down"); up != 2 || down != 11 {
			t.Fatalf("checks: up %d, down %d; want 2 and 11", up, down)
		}
		// Per-target options override the defaults.
		h.c.Put("up", Target{Probe: h.sw.probe, Address: "up", Options: Options{Interval: time.Second}})
		h.tick(3)
		if got := h.sw.count("up"); got != 5 {
			t.Fatalf("checks after shortening the interval: %d, want 5", got)
		}
	})

	// The default jitter is a tenth of the interval.
	c := New[string, string](t.Context(), Config[string]{Options: Options{Interval: 10 * time.Second}})
	defer c.Close()
	e := &entry[string, string]{lastEnd: time.Time{}, checks: 1, target: Target{Address: "x"}}
	if j := c.optionsLocked(e).Jitter; j != time.Second {
		t.Fatalf("default jitter %v", j)
	}
	if d := c.delayLocked(e, time.Time{}); d < 9*time.Second || d > 11*time.Second {
		t.Fatalf("jittered interval from zero time %v", d)
	}
}

func TestRuntimeOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Interval: time.Hour}})
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)

		// A shorter interval applies to the pending check at once.
		cfg := Config[string]{Clock: h.clk, OnResult: h.rep.add, Options: Options{Interval: time.Second, Jitter: -1}}
		h.c.SetConfig(cfg)
		h.tick(1)
		if got := h.sw.count("a"); got != 2 {
			t.Fatalf("checks after SetConfig: %d, want 2", got)
		}

		// Disabling one target forgets its health and stops its checks.
		h.c.Put("a", Target{Probe: h.sw.probe, Address: "a", Options: Options{Disabled: true}})
		runDue(h.clk)
		h.tick(5)
		s, _ := h.c.Status("a")
		if h.sw.count("a") != 2 || s.Checking || s.Health != unknown || h.clk.Pending() != 0 {
			t.Fatalf("disabled target: %d checks, %+v, %d timers", h.sw.count("a"), s, h.clk.Pending())
		}
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, unknown}) {
			t.Fatalf("changes %v", got)
		}
		// Enabling it checks at once.
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		if got := h.sw.count("a"); got != 3 {
			t.Fatalf("checks after enabling: %d", got)
		}

		// Disabling globally stops every target.
		h.c.Put("b", h.sw.target("b"))
		runDue(h.clk)
		cfg.Disabled = true
		h.c.SetConfig(cfg)
		runDue(h.clk)
		h.tick(3)
		if h.sw.count("a") != 3 || h.sw.count("b") != 1 || h.clk.Pending() != 0 {
			t.Fatalf("globally disabled: a %d, b %d, %d timers", h.sw.count("a"), h.sw.count("b"), h.clk.Pending())
		}
		cfg.Disabled = false
		h.c.SetConfig(cfg)
		runDue(h.clk)
		if h.sw.count("a") != 4 || h.sw.count("b") != 2 {
			t.Fatal("re-enabling did not check at once")
		}
	})
}

func TestAddressChangeStartsAfresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		h.c.AddToGroup("tunnel", "k", h.sw.target("old"))
		runDue(h.clk)
		h.c.AddToGroup("tunnel", "k", h.sw.target("new"))
		runDue(h.clk)
		s, _ := h.c.Status("k")
		if s.Checks != 1 || s.Target.Address != "new" || s.Health != up {
			t.Fatalf("status %+v", s)
		}
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, unknown, up}) {
			t.Fatalf("changes %v", got)
		}
		if gs := h.c.GroupsOf("k"); !slices.Equal(gs, []string{"tunnel"}) {
			t.Fatalf("groups %v", gs)
		}
	})
}

func TestGroupsShareTargets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var removed []string
		var mu sync.Mutex
		h := newHarness(t, Config[string]{OnRemove: func(k string, _ uint64) {
			mu.Lock()
			removed = append(removed, k)
			mu.Unlock()
		}})
		h.c.AddToGroup("t1", "10.0.0.1:80", h.sw.target("10.0.0.1:80"))
		h.c.AddToGroup("t1", "10.0.0.2:80", h.sw.target("10.0.0.2:80"))
		h.c.AddToGroup("t2", "10.0.0.1:80", h.sw.target("10.0.0.1:80"))
		runDue(h.clk)
		h.tick(2)
		if got := h.sw.count("10.0.0.1:80"); got != 3 {
			t.Fatalf("shared target checked %d times, want once per interval (3)", got)
		}
		keys := h.c.GroupKeys("t1")
		slices.Sort(keys)
		groups := h.c.Groups()
		slices.Sort(groups)
		if !slices.Equal(keys, []string{"10.0.0.1:80", "10.0.0.2:80"}) || !slices.Equal(groups, []string{"t1", "t2"}) {
			t.Fatalf("keys %v groups %v", keys, groups)
		}

		if n := h.c.RemoveGroup("t1"); n != 2 {
			t.Fatalf("RemoveGroup returned %d", n)
		}
		if h.c.Len() != 1 || h.c.RemoveGroup("t1") != 0 {
			t.Fatalf("Len %d after removing t1", h.c.Len())
		}
		h.tick(1)
		if h.sw.count("10.0.0.1:80") != 4 || h.sw.count("10.0.0.2:80") != 3 {
			t.Fatal("shared target stopped with its first group, or the other did not stop")
		}
		if !h.c.RemoveFromGroup("t2", "10.0.0.1:80") || h.c.RemoveFromGroup("t2", "10.0.0.1:80") {
			t.Fatal("RemoveFrom")
		}
		runDue(h.clk) // OnRemove runs on the dispatcher
		if h.c.Len() != 0 || len(h.c.Groups()) != 0 || h.clk.Pending() != 0 {
			t.Fatalf("left %d keys, %d groups, %d timers", h.c.Len(), len(h.c.Groups()), h.clk.Pending())
		}
		mu.Lock()
		slices.Sort(removed)
		got := slices.Clone(removed)
		mu.Unlock()
		if !slices.Equal(got, []string{"10.0.0.1:80", "10.0.0.2:80"}) {
			t.Fatalf("removed %v", got)
		}

		// A key held directly survives its groups.
		h.c.Put("d", h.sw.target("d"))
		h.c.AddToGroup("g", "d", h.sw.target("d"))
		h.c.RemoveGroup("g")
		if h.c.Len() != 1 || !h.c.Remove("d") || h.c.Remove("d") {
			t.Fatal("direct hold")
		}
	})
}

func TestSyncGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		h.c.SyncGroup("t", map[string]Target{"a": h.sw.target("a"), "b": h.sw.target("b")})
		h.c.AddToGroup("other", "a", h.sw.target("a"))
		runDue(h.clk)
		h.tick(1)
		h.c.SyncGroup("t", map[string]Target{"b": h.sw.target("b"), "c": h.sw.target("c")})
		runDue(h.clk)
		keys := h.c.GroupKeys("t")
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"b", "c"}) {
			t.Fatalf("keys %v", keys)
		}
		if s, _ := h.c.Status("b"); s.Checks != 2 || s.Health != up {
			t.Fatalf("unchanged target lost its state: %+v", s)
		}
		if _, ok := h.c.Status("a"); !ok {
			t.Fatal("a is still held by another group")
		}
		h.c.SyncGroup("t", nil)
		h.c.RemoveGroup("other")
		if h.c.Len() != 0 || h.clk.Pending() != 0 {
			t.Fatalf("%d keys and %d timers left", h.c.Len(), h.clk.Pending())
		}
	})
}

func TestRemoveCancelsCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rep reports
		started := make(chan struct{}, 1)
		var canceled atomic.Bool
		hang := func(ctx context.Context, _ Target) (Observation, error) {
			started <- struct{}{}
			<-ctx.Done()
			canceled.Store(true)
			return Observation{}, ctx.Err()
		}
		sw := newSwitchable()
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: rep.add, Concurrency: 1, Options: Options{Timeout: time.Hour}})
		defer c.Close()
		c.Put("hang", Target{Probe: hang})
		clk.Advance(0)
		<-started
		c.Put("next", Target{Probe: sw.probe, Address: "next"}) // waits for the only slot
		runDue(clk)
		if sw.count("next") != 0 {
			t.Fatal("concurrency bound not applied")
		}
		c.Remove("hang")
		runDue(clk)
		if !canceled.Load() {
			t.Fatal("check of a removed target not canceled")
		}
		if sw.count("next") != 1 {
			t.Fatal("slot of the removed target not released")
		}
		if rep.len() != 1 {
			t.Fatalf("%d reports, want only next's", rep.len())
		}
		// Adding the key again starts afresh.
		c.Put("hang", Target{Probe: sw.probe, Address: "hang"})
		runDue(clk)
		if s, _ := c.Status("hang"); s.Checks != 1 || s.Health != up {
			t.Fatalf("re-added: %+v", s)
		}
	})
}

// TestSlowTargetDoesNotDelayOthers hangs one target's checks and verifies
// that the other targets keep their schedule.
func TestSlowTargetDoesNotDelayOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Concurrency: 2, Options: Options{Timeout: time.Hour}})
		release := make(chan struct{})
		slow := func(ctx context.Context, _ Target) (Observation, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Observation{}, nil
		}
		h.c.Put("slow", Target{Probe: slow})
		h.c.Put("x", h.sw.target("x"))
		h.c.Put("y", h.sw.target("y"))
		runDue(h.clk)
		h.tick(5)
		if x, y := h.sw.count("x"), h.sw.count("y"); x != 6 || y != 6 {
			t.Fatalf("fast checks %d and %d, want 6 each", x, y)
		}
		close(release)
	})
}

func TestConcurrencyBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running, peak atomic.Int32
		gate := make(chan struct{})
		probe := func(context.Context, Target) (Observation, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-gate
			running.Add(-1)
			return Observation{}, nil
		}
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := New[string, string](t.Context(), Config[string]{Clock: clk, Concurrency: 3})
		for _, id := range []string{"a", "b", "c", "d", "e", "f", "g"} {
			c.Put(id, Target{Probe: probe})
		}
		runDue(clk)
		if peak.Load() != 3 || running.Load() != 3 {
			t.Fatalf("peak %d, running %d; want 3", peak.Load(), running.Load())
		}
		close(gate)
		runDue(clk)
		if peak.Load() != 3 {
			t.Fatalf("peak %d after the gate opened", peak.Load())
		}
		c.Close()
	})
}

func TestLatencyStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{LatencyWindow: 3})
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		for _, ms := range []int{40, 10, 30, 20} {
			h.sw.mu.Lock()
			h.sw.latency["a"] = time.Duration(ms) * time.Millisecond
			h.sw.mu.Unlock()
			h.tick(1)
		}
		h.sw.set("a", true) // failures do not count
		h.tick(1)
		s, _ := h.c.Status("a")
		// Samples: 0 (first check, zero latency means elapsed = 0), 40, 10, 30, 20.
		want := LatencyStats{Last: 20 * time.Millisecond, Min: 10 * time.Millisecond, Max: 30 * time.Millisecond, Samples: 3}
		avg := s.Stats.Average
		s.Stats.Average = 0
		if s.Stats != want {
			t.Fatalf("latency %+v, want %+v", s.Stats, want)
		}
		if avg <= 0 || avg >= 40*time.Millisecond {
			t.Fatalf("average %v", avg)
		}
	})
}

func TestCloseAndContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rep reports
		started := make(chan struct{})
		probe := func(ctx context.Context, _ Target) (Observation, error) {
			close(started)
			<-ctx.Done()
			return Observation{}, ctx.Err()
		}
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: rep.add, Options: Options{Timeout: time.Hour}})
		c.Put("a", Target{Probe: probe})
		clk.Advance(0)
		<-started
		c.Close()
		c.Close()
		<-c.Done() // the probe saw its context end and returned
		if rep.len() != 0 {
			t.Fatal("canceled check reported")
		}
		c.Put("b", Target{Probe: probe})
		if len(c.Statuses()) != 0 {
			t.Fatal("Set after Close")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		ctx, cancel := context.WithCancel(t.Context())
		sw := newSwitchable()
		c := New[string, string](ctx, Config[string]{Clock: clk})
		c.Put("a", sw.target("a"))
		runDue(clk)
		cancel()
		runDue(clk)
		if clk.Pending() != 0 || len(c.Statuses()) != 0 {
			t.Fatal("checker survives its context")
		}
		c.Close()
	})
}

// TestConcurrentChurn adds, syncs and removes targets and groups and
// changes the configuration from many goroutines while checks run, then
// removes everything and checks that no timer or goroutine is left.
func TestConcurrentChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		probe := func(ctx context.Context, t Target) (Observation, error) {
			select {
			case <-ctx.Done():
				return Observation{}, ctx.Err()
			case <-time.After(time.Duration(len(t.Address)) * time.Millisecond):
			}
			if len(t.Address)%3 == 0 {
				return Observation{}, errDown
			}
			return Observation{}, nil
		}
		var c *Checker[string, int]
		c = New[string, int](t.Context(), Config[string]{
			Clock:       clk,
			Concurrency: 4,
			Options:     Options{Interval: time.Second, Timeout: 50 * time.Millisecond},
			OnResult: func(Report[string]) {
				_ = len(c.Statuses()) // callbacks may call the Checker
			},
		})
		key := func(i int) string { return fmt.Sprintf("10.0.%d.%d:80", i%7, i%13) }
		var wg sync.WaitGroup
		for w := range 6 {
			wg.Go(func() {
				for i := range 300 {
					g := (i + w) % 5
					k := key(i * (w + 1))
					switch i % 6 {
					case 0:
						c.AddToGroup(g, k, Target{Probe: probe, Address: k})
					case 1:
						c.SyncGroup(g, map[string]Target{k: {Probe: probe, Address: k}, key(i + 1): {Probe: probe, Address: key(i + 1)}})
					case 2:
						c.RemoveFromGroup(g, k)
					case 3:
						c.RemoveGroup(g)
					case 4:
						c.SetConfig(Config[string]{Options: Options{Interval: time.Duration(i%3+1) * time.Second, Disabled: i%17 == 0}, Concurrency: 4})
					case 5:
						_ = c.Statuses()
					}
				}
			})
		}
		wg.Go(func() {
			for range 50 {
				clk.Advance(time.Second)
				time.Sleep(time.Millisecond)
			}
		})
		wg.Wait()
		for g := range 5 {
			c.RemoveGroup(g)
		}
		runDue(clk)
		if len(c.Statuses()) != 0 || clk.Pending() != 0 {
			t.Fatalf("%d keys and %d timers after removing every group", len(c.Statuses()), clk.Pending())
		}
		c.Close()
	})
}

func TestWatchPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "1"}, {ID: "b", Value: "2"}}); err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var states []string
		p.Subscribe(func(ev upstream.Event[string]) {
			if ev.Kind == upstream.EventStateChanged {
				mu.Lock()
				states = append(states, ev.Backend.ID()+":"+ev.To.String())
				mu.Unlock()
			}
		})
		sw := newSwitchable()
		sw.set("2", true)
		target := func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} }
		c := mustWatch(t.Context(), t, p, target, Config[string]{Clock: clk, Options: Options{Interval: time.Second, Fall: 2, Jitter: -1}})
		runDue(clk)
		b, _ := p.Backend("b")
		if b.State() != upstream.StateUnhealthy || !errors.Is(b.Status().LastCheck.Err, errDown) || b.Status().Health != down {
			t.Fatalf("b is %v, status %+v", b.State(), b.Status())
		}
		if a, _ := p.Backend("a"); a.Status().Health != up {
			t.Fatalf("a: %+v", a.Status())
		}
		for range 5 {
			clk.Advance(time.Second)
			runDue(clk)
		}
		mu.Lock()
		got := slices.Clone(states)
		mu.Unlock()
		if !slices.Equal(got, []string{"b:unhealthy"}) {
			t.Fatalf("state events %v", got)
		}

		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "1"}, {ID: "c", Value: "3"}}); err != nil {
			t.Fatal(err)
		}
		runDue(clk)
		if len(c.Statuses()) != 2 || sw.count("3") != 1 {
			t.Fatalf("checker has %d targets, c checked %d times", len(c.Statuses()), sw.count("3"))
		}
		before := sw.count("2")
		clk.Advance(time.Second)
		runDue(clk)
		if sw.count("2") != before {
			t.Fatal("removed backend still checked")
		}
		c.Close()
		if err := p.Put(upstream.Target[string]{ID: "d", Value: "4"}); err != nil {
			t.Fatal(err)
		}
		runDue(clk)
		if sw.count("4") != 0 {
			t.Fatal("closed checker still follows the pool")
		}
	})
}

func TestWatchClosedContext(t *testing.T) {
	p := upstream.New[string](upstream.Config{})
	defer p.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := Watch(ctx, p, nil, Config[string]{Clock: timeshim.NewFake(time.Unix(0, 0))}); !errors.Is(err, context.Canceled) || w != nil {
		t.Fatalf("Watch with a done context = %v, %v", w, err)
	}
	p.Close()
	if _, err := Watch(t.Context(), p, nil, Config[string]{Clock: timeshim.NewFake(time.Unix(0, 0))}); !errors.Is(err, upstream.ErrClosed) {
		t.Fatalf("Watch of a closed pool = %v", err)
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		err  error
		want Reason
	}{
		{nil, ReasonNone},
		{&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "x.invalid", IsNotFound: true}}, ReasonDNS},
		{fmt.Errorf("check: %w", context.DeadlineExceeded), ReasonTimeout},
		{os.ErrDeadlineExceeded, ReasonTimeout},
		{fmt.Errorf("status 500: %w", ErrBadResponse), ReasonResponse},
		{errors.New("connection refused"), ReasonOther}, // text is not parsed
	}
	tests = append(tests, errnoCases...)
	for _, tt := range tests {
		if got := Classify(tt.err); got != tt.want {
			t.Errorf("Classify(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
	for r := ReasonNone; r <= ReasonOther+1; r++ {
		if r.String() == "" {
			t.Errorf("Reason(%d) has no name", r)
		}
	}
}

func TestTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan int, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if err != nil {
				continue // a connection reset before it was taken, as on Windows
			}
			// The check must send nothing: the first read sees the
			// end of the stream (or a reset).
			n, _ := io.Copy(io.Discard, conn)
			received <- int(n)
			_ = conn.Close()
		}
	}()
	defer func() {
		_ = ln.Close()
		<-done
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	target := Target{Address: ln.Addr().String()}
	for _, o := range []TCPOptions{{}, {Reset: true}} {
		obs, err := TCP(o)(ctx, target)
		if err != nil {
			t.Fatalf("check of a listening port (%+v): %v", o, err)
		}
		// Zero is possible where the clock is coarse, as on Windows.
		if obs.Addr != ln.Addr().String() || obs.Latency < 0 {
			t.Fatalf("observation %+v", obs)
		}
		select {
		case n := <-received:
			if n != 0 {
				t.Fatalf("the check sent %d bytes", n)
			}
		case <-time.After(5 * time.Second):
			// A connection reset at once may never reach Accept, as on
			// Windows; one closed normally always does.
			if !o.Reset {
				t.Fatal("the listener never saw the check's connection")
			}
		}
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	<-done
	if runtime.GOOS != "js" && runtime.GOOS != "wasip1" { // the simulated network differs
		if _, err := TCP(TCPOptions{})(ctx, Target{Address: addr}); Classify(err) != ReasonRefused {
			t.Fatalf("check of a closed port: %v (%v)", err, Classify(err))
		}
	}

	dialErr := errors.New("dial")
	custom := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) { return nil, dialErr }})
	if _, err := custom(ctx, Target{Address: "x"}); !errors.Is(err, dialErr) {
		t.Fatalf("custom dial: %v", err)
	}
}

func TestDisableCancelsCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		var rep reports
		gate := make(chan struct{})
		var canceled atomic.Bool
		probe := func(ctx context.Context, _ Target) (Observation, error) {
			select {
			case <-gate:
				return Observation{}, nil
			case <-ctx.Done():
				canceled.Store(true)
				return Observation{}, ctx.Err()
			}
		}
		c := New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: rep.add, Options: Options{Timeout: time.Hour}})
		defer c.Close()
		c.Put("a", Target{Probe: probe})
		close(gate)
		runDue(clk) // first check: up
		gate = make(chan struct{})
		clk.Advance(time.Hour)
		runDue(clk) // second check hangs
		c.Put("a", Target{Probe: probe, Options: Options{Disabled: true}})
		runDue(clk)
		s, _ := c.Status("a")
		if !canceled.Load() || s.Health != unknown || s.Checking || clk.Pending() != 0 {
			t.Fatalf("canceled %v, status %+v, %d timers", canceled.Load(), s, clk.Pending())
		}
		if got := rep.changes(); !slices.Equal(got, []upstream.Health{up, unknown}) {
			t.Fatalf("changes %v", got)
		}
	})
}

func TestWatchIgnoresStaleAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "old"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		sw.set("old", true)
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := mustWatch(t.Context(), t, p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: clk})
		defer c.Close()
		runDue(clk)
		// Reports about the old address do not apply to the new one.
		c.c.mu.Lock()
		sink := c.c.sinks[0].fn
		c.c.mu.Unlock()
		if err := p.Put(upstream.Target[string]{ID: "a", Value: "new"}); err != nil {
			t.Fatal(err)
		}
		runDue(clk)
		sink(Report[string]{Result: Result{Health: down, Previous: up}, Key: "a", Target: Target{Probe: sw.probe, Address: "old"}})
		if b, _ := p.Backend("a"); b.Status().Health != up {
			t.Fatalf("stale report applied: %+v", b.Status())
		}
	})
}

func TestCloseFromCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *Checker[string, string]
		closed := make(chan struct{})
		clk := timeshim.NewFake(time.Unix(0, 0))
		c = New[string, string](t.Context(), Config[string]{Clock: clk, OnChange: func(Report[string]) {
			c.Close() // must not wait for this callback
			close(closed)
		}})
		sw := newSwitchable()
		c.Put("a", sw.target("a"))
		clk.Advance(0)
		<-closed
		c.Close() // from outside too: does not wait
		<-c.Done()
		if len(c.Statuses()) != 0 {
			t.Fatal("targets left after Close")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		var c *Checker[string, string]
		probe := func(context.Context, Target) (Observation, error) {
			c.Close() // from a probe
			return Observation{}, nil
		}
		var rep reports
		clk := timeshim.NewFake(time.Unix(0, 0))
		c = New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: rep.add})
		c.Put("a", Target{Probe: probe})
		clk.Advance(0)
		<-c.Done()
		if rep.len() != 0 {
			t.Fatal("result of a check that closed the checker was reported")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		// Removal methods from a callback.
		var c *Checker[string, string]
		sw := newSwitchable()
		clk := timeshim.NewFake(time.Unix(0, 0))
		c = New[string, string](t.Context(), Config[string]{Clock: clk, OnChange: func(r Report[string]) {
			c.RemoveGroup("g")
			c.SyncGroup("h", map[string]Target{"z": sw.target("z")})
			c.Remove(r.Key)
		}})
		defer c.Close()
		c.AddToGroup("g", "a", sw.target("a"))
		runDue(clk)
		if _, ok := c.Status("a"); ok || len(c.GroupKeys("g")) != 0 {
			t.Fatal("removal from a callback did not apply")
		}
	})
}

func TestNoCallbackAfterRemove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		sw := newSwitchable()
		block := make(chan struct{})
		var mu sync.Mutex
		var removedAt bool
		var late, xReports int
		var removedGen uint64
		c := New[string, string](t.Context(), Config[string]{
			Clock:   clk,
			Options: Options{Interval: time.Second, Jitter: -1},
			OnResult: func(r Report[string]) {
				if r.Key == "block" && r.Health == up && r.Changed() {
					<-block
				}
				mu.Lock()
				defer mu.Unlock()
				if r.Key == "x" {
					xReports++
					if removedAt {
						late++
					}
				}
			},
			OnRemove: func(k string, gen uint64) {
				mu.Lock()
				defer mu.Unlock()
				if k == "x" {
					removedGen = gen
				}
			},
		})
		defer c.Close()
		c.Put("block", sw.target("block"))
		runDue(clk) // the dispatcher is stuck in block's first callback
		c.Put("x", sw.target("x"))
		c.Put("y", sw.target("y"))
		for range 50 {
			clk.Advance(time.Second)
			runDue(clk)
		}
		gen := func() uint64 { s, _ := c.Status("x"); return s.Generation }()
		c.Remove("x")
		mu.Lock()
		removedAt = true
		mu.Unlock()
		close(block)
		runDue(clk)
		mu.Lock()
		defer mu.Unlock()
		if late != 0 || xReports != 0 {
			t.Fatalf("%d callbacks for x after Remove (%d in all)", late, xReports)
		}
		if removedGen != gen || gen == 0 {
			t.Fatalf("OnRemove generation %d, want %d", removedGen, gen)
		}
		c.Put("x", sw.target("x"))
		if s, _ := c.Status("x"); s.Generation == gen {
			t.Fatal("re-added key kept its generation")
		}
	})
}

func TestBlockedCallbackCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		sw := newSwitchable()
		block := make(chan struct{})
		var n atomic.Int32
		c := New[string, string](t.Context(), Config[string]{
			Clock:   clk,
			Options: Options{Interval: time.Second, Jitter: -1},
			OnResult: func(r Report[string]) {
				if r.Key == "block" && r.Changed() {
					<-block
				}
				if r.Key == "y" {
					n.Add(1)
				}
			},
		})
		defer c.Close()
		c.Put("block", sw.target("block"))
		runDue(clk)
		c.Put("y", sw.target("y"))
		runDue(clk)
		for range 100 {
			clk.Advance(time.Second)
			runDue(clk)
		}
		c.mu.Lock()
		queued := len(c.queue)
		c.mu.Unlock()
		if queued > 4 {
			t.Fatalf("%d callbacks queued behind a blocked one", queued)
		}
		close(block)
		runDue(clk)
		if s, _ := c.Status("y"); n.Load() > 3 || s.Checks != 101 {
			t.Fatalf("y: %d reports delivered, %d checks", n.Load(), s.Checks)
		}
	})
}

func TestWatchCloseFromPoolSubscriber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "a"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		sw.set("a", true)
		// The subscriber must not block: it may run on the goroutine that
		// calls Watch, which delivers the events it causes.
		var cp atomic.Pointer[Watcher]
		var once sync.Once
		closed := make(chan struct{})
		p.Subscribe(func(ev upstream.Event[string]) {
			if w := cp.Load(); w != nil && ev.Kind == upstream.EventStateChanged {
				once.Do(func() { w.Close(); close(closed) }) // on the dispatcher, through ReportCheck
			}
		})
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := mustWatch(t.Context(), t, p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: clk, Options: Options{Rise: 1}})
		runDue(clk)
		cp.Store(c)
		sw.set("a", false)
		c.CheckNow("a") // a state change after the pointer is set
		runDue(clk)
		select {
		case <-closed:
		default:
			t.Fatal("the subscriber did not close the watcher")
		}
		c.Close()
		if len(c.Statuses()) != 0 {
			t.Fatal("checker still running")
		}
	})
}

// Two owned goroutines (two probes) calling Close at the same time each wait
// for the other to end: len(owned) stays 2 > min(owned[self],1).
func TestConcurrentCloseFromTwoProbes(t *testing.T) {
	var c *Checker[string, string]
	var barrier sync.WaitGroup
	barrier.Add(2)
	done := make(chan struct{}, 2)
	probe := func(_ context.Context, _ Target) (Observation, error) {
		barrier.Done()
		barrier.Wait()
		c.Close()
		done <- struct{}{}
		return Observation{}, nil
	}
	c = New[string, string](context.Background(), Config[string]{Concurrency: 4})
	c.Put("a", Target{Probe: probe, Address: "a"})
	c.Put("b", Target{Probe: probe, Address: "b"})
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
	inCallback := make(chan struct{})
	done := make(chan struct{}, 2)
	probe := func(_ context.Context, tg Target) (Observation, error) {
		if tg.Address == "slow" {
			close(inProbe)
			<-inCallback
			c.Close()
			done <- struct{}{}
		}
		return Observation{}, nil
	}
	c = New[string, string](context.Background(), Config[string]{Concurrency: 4, OnChange: func(r Report[string]) {
		if r.Key == "fast" {
			<-inProbe
			close(inCallback)
			c.Close()
			done <- struct{}{}
		}
	}})
	c.Put("slow", Target{Probe: probe, Address: "slow"})
	c.Put("fast", Target{Probe: probe, Address: "fast"})
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
	c.Put("a", Target{Address: "x:1", Options: Options{Interval: time.Second}})
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
		probe := func(_ context.Context, _ Target) (Observation, error) {
			<-gate // ignores ctx on purpose: Done must wait for it
			return Observation{}, nil
		}
		clk := timeshim.NewFake(time.Unix(0, 0))
		c := New[string, string](t.Context(), Config[string]{Clock: clk})
		c.Put("a", Target{Probe: probe})
		runDue(clk)
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
		clk := timeshim.NewFake(time.Unix(0, 0))
		c = New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: func(Report[string]) { c.Close() }})
		for _, k := range []string{"a", "b", "c", "d"} {
			c.Put(k, Target{Probe: probe})
		}
		clk.Advance(0)
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(c.Close)
		}
		wg.Wait()
		<-c.Done()
	})
}

// TestAddressChangeEndsGeneration checks that OnRemove reports the end of
// the old generation when an address change starts a new one, before any
// report of the new one.
func TestAddressChangeEndsGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var log []string
		h := newHarness(t, Config[string]{
			OnResult: func(r Report[string]) {
				mu.Lock()
				defer mu.Unlock()
				log = append(log, fmt.Sprintf("result %d %v", r.Generation, r.Health))
			},
			OnRemove: func(key string, gen uint64) {
				mu.Lock()
				defer mu.Unlock()
				log = append(log, fmt.Sprintf("remove %s %d", key, gen))
			},
		})
		h.c.Put("k", h.sw.target("a"))
		runDue(h.clk)
		h.c.Put("k", Target{Probe: h.sw.probe, Network: "tcp", Address: "a"}) // the same network
		h.c.Put("k", h.sw.target("b"))
		runDue(h.clk)
		mu.Lock()
		defer mu.Unlock()
		want := []string{"result 1 up", "remove k 1", "result 2 unknown", "result 2 up"}
		if !slices.Equal(log, want) {
			t.Fatalf("callbacks %v, want %v", log, want)
		}
	})
}

// TestTCPCustomDialClosesConnWithError checks that a connection a custom
// Dial returns together with an error is closed.
func TestTCPCustomDialClosesConnWithError(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	probe := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) { return client, errDown }})
	if _, err := probe(t.Context(), Target{Address: "x"}); !errors.Is(err, errDown) {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte{0}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("connection left open: %v", err)
	}
}

// TestTCPNoAddress checks that the TCP check fails a target without an
// address with ErrNoAddress instead of dialing "".
func TestTCPNoAddress(t *testing.T) {
	dial := func(context.Context, string, string) (net.Conn, error) {
		t.Error("dialed an empty address")
		return nil, errDown
	}
	for _, o := range []TCPOptions{{}, {Dial: dial}} {
		if _, err := TCP(o)(context.Background(), Target{}); !errors.Is(err, upstream.ErrNoAddress) {
			t.Fatalf("check = %v, want ErrNoAddress", err)
		}
	}
}

// The Checker's Clock is its only time source: the default TCP check
// measures latency on it, whatever TCPOptions.Clock says, and the check
// timeout runs on it.
func TestCheckerClockIsOnlyTimeSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(0, 0))
		dial := func(context.Context, string, string) (net.Conn, error) {
			clk.Advance(3 * time.Second)
			a, b := net.Pipe()
			_ = b.Close()
			return a, nil
		}
		results := make(chan Report[string], 4)
		c := New[string, string](t.Context(), Config[string]{
			Clock:    clk,
			TCP:      TCPOptions{Dial: dial, Clock: timeshim.Real},
			Options:  Options{Interval: time.Hour, Timeout: time.Minute},
			OnResult: func(r Report[string]) { results <- r },
		})
		defer c.Close()
		c.Put("tcp", Target{Address: "tcp.invalid:80"})
		runDue(clk)
		if r := <-results; r.Err != nil || r.Latency != 3*time.Second {
			t.Fatalf("TCP check: err %v, latency %v, want nil, 3s", r.Err, r.Latency)
		}
		c.Remove("tcp")
		runDue(clk)

		deadlines := make(chan time.Time, 1)
		hang := func(ctx context.Context, _ Target) (Observation, error) {
			d, ok := ctx.Deadline()
			if !ok {
				t.Error("probe context has no deadline")
			}
			deadlines <- d
			<-ctx.Done()
			return Observation{}, errors.New("canceled")
		}
		want := clk.Now().Add(time.Minute)
		c.Put("hang", Target{Probe: hang})
		runDue(clk)
		if d := <-deadlines; !d.Equal(want) {
			t.Fatalf("probe deadline %v, want %v", d, want)
		}
		clk.Advance(time.Minute - time.Nanosecond)
		runDue(clk)
		if len(results) != 0 {
			t.Fatal("check timed out early")
		}
		clk.Advance(time.Nanosecond)
		runDue(clk)
		if r := <-results; r.Reason != ReasonTimeout || !errors.Is(r.Err, context.DeadlineExceeded) {
			t.Fatalf("timed out check: %v (%v), want ReasonTimeout", r.Err, r.Reason)
		}
	})
}

func TestTCPLatencyUsesClock(t *testing.T) {
	clk := timeshim.NewFake(time.Unix(0, 0))
	dial := func(context.Context, string, string) (net.Conn, error) {
		clk.Advance(2 * time.Second)
		a, b := net.Pipe()
		_ = b.Close()
		return a, nil
	}
	obs, err := TCP(TCPOptions{Dial: dial, Clock: clk})(context.Background(), Target{Address: "x.invalid:1"})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if obs.Latency != 2*time.Second {
		t.Fatalf("latency %v, want 2s", obs.Latency)
	}
}

func TestProbeSeesDeadline(t *testing.T) {
	deadlines := make(chan time.Time, 1)
	probe := func(ctx context.Context, _ Target) (Observation, error) {
		d, ok := ctx.Deadline()
		if !ok {
			t.Error("probe context has no deadline")
		}
		deadlines <- d
		return Observation{}, nil
	}
	before := time.Now()
	c := New[string, string](t.Context(), Config[string]{
		Options: Options{Interval: time.Hour, Timeout: time.Minute},
	})
	defer c.Close()
	c.Put("p", Target{Probe: probe})
	d := <-deadlines
	if d.Before(before.Add(time.Minute)) || d.After(time.Now().Add(time.Minute)) {
		t.Fatalf("probe deadline %v, want about a minute from %v", d, before)
	}
}

func TestTouchPostponesChecksWhileUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Interval: 10 * time.Second, UnhealthyInterval: time.Second, Fall: 1, Rise: 1}})
		if h.c.Touch("a") {
			t.Fatal("Touch of a missing key")
		}
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		if got := h.sw.count("a"); got != 1 {
			t.Fatalf("first check: %d", got)
		}
		// Traffic every 5s keeps the target from being probed.
		for range 6 {
			h.tick(5)
			if !h.c.Touch("a") {
				t.Fatal("Touch of an up target")
			}
		}
		if got := h.sw.count("a"); got != 1 {
			t.Fatalf("checks during traffic: %d, want 1", got)
		}
		// Idle again: the check comes an interval after the last traffic.
		h.tick(9)
		if got := h.sw.count("a"); got != 1 {
			t.Fatalf("checks before the interval passed: %d", got)
		}
		h.tick(1)
		if got := h.sw.count("a"); got != 2 {
			t.Fatalf("checks after the interval: %d, want 2", got)
		}
		// A down target is not postponed.
		h.sw.set("a", true)
		h.tick(10)
		if h.c.Touch("a") {
			t.Fatal("Touch of a down target")
		}
		before := h.sw.count("a")
		h.tick(3)
		if got := h.sw.count("a"); got != before+3 {
			t.Fatalf("checks of a down target: %d, want %d", got, before+3)
		}
	})
}

func TestRemoveDuringOnResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *Checker[string, string]
		var removed bool
		clk := timeshim.NewFake(time.Unix(0, 0))
		c = New[string, string](t.Context(), Config[string]{Clock: clk,
			OnResult: func(r Report[string]) { removed = c.Remove(r.Key) },
			OnChange: func(Report[string]) { t.Error("change callback after remove") }})
		defer c.Close()
		c.Put("a", Target{Probe: newSwitchable().probe})
		runDue(clk)
		if !removed {
			t.Fatal("OnResult did not remove the target")
		}
	})
}

func TestAttachmentReplayPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Sync([]upstream.Target[string]{{ID: "a"}, {ID: "b"}}); err != nil {
			t.Fatal(err)
		}
		c := New[string, string](t.Context(), Config[string]{Clock: timeshim.NewFake(time.Unix(0, 0))})
		func() {
			defer func() {
				if recover() != "target" {
					t.Fatal("missing panic")
				}
			}()
			calls := 0
			_, _ = Attach(c, "group", p, func(*upstream.Backend[string]) Target {
				calls++
				if calls == 1 {
					return Target{Network: "udp", Address: "a"}
				}
				panic("target")
			})
		}()
		defer c.Close()
		if err := p.Put(upstream.Target[string]{ID: "c"}); err != nil {
			t.Fatal(err)
		}
		if len(c.GroupKeys("group")) != 0 || c.Len() != 0 {
			t.Fatal("registration survived")
		}
	})
}

func TestTCPRejectsUDP(t *testing.T) {
	probe := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("UDP dial")
		return nil, errDown
	}})
	if _, err := probe(t.Context(), Target{Network: "udp", Address: "a"}); err == nil {
		t.Fatal("UDP check passed")
	}
}

func TestLateProbeResults(t *testing.T) {
	for _, probeErr := range []error{nil, &net.DNSError{Err: "missing"}, os.ErrNotExist} {
		synctest.Test(t, func(t *testing.T) {
			clk := timeshim.NewFake(time.Unix(0, 0))
			c := New[string, string](t.Context(), Config[string]{Clock: clk, Options: Options{Timeout: time.Second}})
			defer c.Close()
			c.Put("a", Target{Probe: func(ctx context.Context, _ Target) (Observation, error) {
				<-ctx.Done()
				return Observation{}, probeErr
			}})
			runDue(clk)
			clk.Advance(time.Second)
			runDue(clk)
			s, _ := c.Status("a")
			if s.Last.Reason != ReasonTimeout || !errors.Is(s.Last.Err, context.DeadlineExceeded) || probeErr != nil && !errors.Is(s.Last.Err, probeErr) {
				t.Fatalf("late result %+v", s.Last)
			}
		})
	}
}

func TestWatchSkipsReplacedEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		put := func(v string) {
			if err := p.Put(upstream.Target[string]{ID: "a", Value: v}); err != nil {
				t.Fatal(err)
			}
		}
		defer p.Subscribe(func(e upstream.Event[string]) {
			if e.Kind == upstream.EventAdded && e.Backend.Value() == "old" {
				p.Remove("a")
				put("new")
			}
		})()
		calls := 0
		w, err := Watch(t.Context(), p, func(b *upstream.Backend[string]) Target {
			calls++
			if b.Value() != "new" {
				t.Error("stale target")
			}
			return Target{Network: "udp"}
		}, Config[string]{Clock: timeshim.NewFake(time.Unix(0, 0))})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		put("old")
		if calls != 1 {
			t.Fatalf("target calls %d", calls)
		}
	})
}

type panicAddrConn struct {
	net.Conn
	closed int
}

func (*panicAddrConn) RemoteAddr() net.Addr { panic("address") }
func (c *panicAddrConn) Close() error       { c.closed++; return nil }

func TestTCPClosesOnPanic(t *testing.T) {
	c := new(panicAddrConn)
	probe := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) { return c, nil }})
	defer func() {
		if recover() != "address" || c.closed != 1 {
			t.Error("connection cleanup failed")
		}
	}()
	_, _ = probe(t.Context(), Target{Address: "a"})
}

type admissionClock struct {
	*timeshim.Fake
	beforeProbe func()
}

func (c *admissionClock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	timer := c.Fake.AfterFunc(d, f)
	if d == time.Minute {
		c.beforeProbe()
	}
	return timer
}
func TestRemovalDuringProbeAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := &admissionClock{Fake: timeshim.NewFake(time.Unix(0, 0))}
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Timeout: time.Minute}})
		defer c.Close()
		clk.beforeProbe = func() { c.Remove("a") }
		c.Put("a", Target{Probe: func(context.Context, Target) (Observation, error) {
			t.Error("probe after removal")
			return Observation{}, nil
		}})
		runDue(clk.Fake)
		if c.Len() != 0 {
			t.Fatal("removal did not run")
		}
	})
}
