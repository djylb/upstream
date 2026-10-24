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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
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
		if rep.Changed {
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
	clk *fakeclock.Clock
	sw  *switchable
	rep *reports
	c   *Checker[string, string]
}

// newHarness starts a checker with a fake clock and a 1s interval without
// jitter.
func newHarness(t *testing.T, cfg Config[string]) *harness {
	h := &harness{clk: fakeclock.New(), sw: newSwitchable(), rep: new(reports)}
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

func (h *harness) tick(n int) {
	for range n {
		h.clk.Advance(time.Second)
		synctest.Wait()
	}
}

func TestRiseAndFall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Rise: 2, Fall: 3}})
		h.c.Set("a", h.sw.target("a"))
		if s, _ := h.c.Status("a"); s.Health != unknown && s.Checks != 0 {
			t.Fatalf("status before the first check: %+v", s)
		}
		synctest.Wait()
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
		h.c.Set("a", h.sw.target("a"))
		synctest.Wait()
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
		h.c.Set("dns", Target{Network: "udp", Address: "192.0.2.1:53"})
		h.c.Set("custom", Target{Network: "udp", Address: "x", Probe: h.sw.probe})
		synctest.Wait()
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
		h.c.Set("up", h.sw.target("up"))
		h.c.Set("down", h.sw.target("down"))
		synctest.Wait()
		h.tick(10)
		if up, down := h.sw.count("up"), h.sw.count("down"); up != 2 || down != 11 {
			t.Fatalf("checks: up %d, down %d; want 2 and 11", up, down)
		}
		// Per-target options override the defaults.
		h.c.Set("up", Target{Probe: h.sw.probe, Address: "up", Options: Options{Interval: time.Second}})
		h.tick(3)
		if got := h.sw.count("up"); got != 5 {
			t.Fatalf("checks after shortening the interval: %d, want 5", got)
		}
	})

	// The default jitter is a tenth of the interval.
	c := New[string, string](t.Context(), Config[string]{Options: Options{Interval: 10 * time.Second}})
	defer c.Close()
	e := &entry[string, string]{lastEnd: time.Unix(1000, 0), target: Target{Address: "x"}}
	seen := map[time.Duration]bool{}
	for range 200 {
		d := c.delayLocked(e, time.Unix(1000, 0))
		if d < 9*time.Second || d > 11*time.Second {
			t.Fatalf("jittered interval %v", d)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Fatal("no jitter")
	}
}

func TestRuntimeOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Interval: time.Hour}})
		h.c.Set("a", h.sw.target("a"))
		synctest.Wait()

		// A shorter interval applies to the pending check at once.
		cfg := Config[string]{Clock: h.clk, OnResult: h.rep.add, Options: Options{Interval: time.Second, Jitter: -1}}
		h.c.SetConfig(cfg)
		h.tick(1)
		if got := h.sw.count("a"); got != 2 {
			t.Fatalf("checks after SetConfig: %d, want 2", got)
		}

		// Disabling one target forgets its health and stops its checks.
		h.c.Set("a", Target{Probe: h.sw.probe, Address: "a", Options: Options{Disabled: true}})
		synctest.Wait()
		h.tick(5)
		s, _ := h.c.Status("a")
		if h.sw.count("a") != 2 || s.Checking || s.Health != unknown || h.clk.Pending() != 0 {
			t.Fatalf("disabled target: %d checks, %+v, %d timers", h.sw.count("a"), s, h.clk.Pending())
		}
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, unknown}) {
			t.Fatalf("changes %v", got)
		}
		// Enabling it checks at once.
		h.c.Set("a", h.sw.target("a"))
		synctest.Wait()
		if got := h.sw.count("a"); got != 3 {
			t.Fatalf("checks after enabling: %d", got)
		}

		// Disabling globally stops every target.
		h.c.Set("b", h.sw.target("b"))
		synctest.Wait()
		cfg.Disabled = true
		h.c.SetConfig(cfg)
		synctest.Wait()
		h.tick(3)
		if h.sw.count("a") != 3 || h.sw.count("b") != 1 || h.clk.Pending() != 0 {
			t.Fatalf("globally disabled: a %d, b %d, %d timers", h.sw.count("a"), h.sw.count("b"), h.clk.Pending())
		}
		cfg.Disabled = false
		h.c.SetConfig(cfg)
		synctest.Wait()
		if h.sw.count("a") != 4 || h.sw.count("b") != 2 {
			t.Fatal("re-enabling did not check at once")
		}
	})
}

func TestAddressChangeStartsAfresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{})
		h.c.Add("tunnel", "k", h.sw.target("old"))
		synctest.Wait()
		h.c.Add("tunnel", "k", h.sw.target("new"))
		synctest.Wait()
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
		h.c.Add("t1", "10.0.0.1:80", h.sw.target("10.0.0.1:80"))
		h.c.Add("t1", "10.0.0.2:80", h.sw.target("10.0.0.2:80"))
		h.c.Add("t2", "10.0.0.1:80", h.sw.target("10.0.0.1:80"))
		synctest.Wait()
		h.tick(2)
		if got := h.sw.count("10.0.0.1:80"); got != 3 {
			t.Fatalf("shared target checked %d times, want once per interval (3)", got)
		}
		keys := h.c.Keys("t1")
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
		if !h.c.RemoveFrom("t2", "10.0.0.1:80") || h.c.RemoveFrom("t2", "10.0.0.1:80") {
			t.Fatal("RemoveFrom")
		}
		synctest.Wait() // OnRemove runs on the dispatcher
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
		h.c.Set("d", h.sw.target("d"))
		h.c.Add("g", "d", h.sw.target("d"))
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
		h.c.Add("other", "a", h.sw.target("a"))
		synctest.Wait()
		h.tick(1)
		h.c.SyncGroup("t", map[string]Target{"b": h.sw.target("b"), "c": h.sw.target("c")})
		synctest.Wait()
		keys := h.c.Keys("t")
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
		clk := fakeclock.New()
		c := New[string, string](t.Context(), Config[string]{Clock: clk, OnResult: rep.add, Concurrency: 1, Options: Options{Timeout: time.Hour}})
		defer c.Close()
		c.Set("hang", Target{Probe: hang})
		<-started
		c.Set("next", Target{Probe: sw.probe, Address: "next"}) // waits for the only slot
		synctest.Wait()
		if sw.count("next") != 0 {
			t.Fatal("concurrency bound not applied")
		}
		c.Remove("hang")
		synctest.Wait()
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
		c.Set("hang", Target{Probe: sw.probe, Address: "hang"})
		synctest.Wait()
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
		h.c.Set("slow", Target{Probe: slow})
		h.c.Set("x", h.sw.target("x"))
		h.c.Set("y", h.sw.target("y"))
		synctest.Wait()
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
		c := New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), Concurrency: 3})
		for _, id := range []string{"a", "b", "c", "d", "e", "f", "g"} {
			c.Set(id, Target{Probe: probe})
		}
		synctest.Wait()
		if peak.Load() != 3 || running.Load() != 3 {
			t.Fatalf("peak %d, running %d; want 3", peak.Load(), running.Load())
		}
		close(gate)
		synctest.Wait()
		if peak.Load() != 3 {
			t.Fatalf("peak %d after the gate opened", peak.Load())
		}
		c.Close()
	})
}

func TestLatencyStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{LatencyWindow: 3})
		h.c.Set("a", h.sw.target("a"))
		synctest.Wait()
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
		want := Latency{Last: 20 * time.Millisecond, Min: 10 * time.Millisecond, Max: 30 * time.Millisecond, Samples: 3}
		avg := s.Latency.Average
		s.Latency.Average = 0
		if s.Latency != want {
			t.Fatalf("latency %+v, want %+v", s.Latency, want)
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
		c := New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), OnResult: rep.add, Options: Options{Timeout: time.Hour}})
		c.Set("a", Target{Probe: probe})
		<-started
		c.Close()
		c.Close()
		<-c.Done() // the probe saw its context end and returned
		if rep.len() != 0 {
			t.Fatal("canceled check reported")
		}
		c.Set("b", Target{Probe: probe})
		if c.Len() != 0 {
			t.Fatal("Set after Close")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		ctx, cancel := context.WithCancel(t.Context())
		sw := newSwitchable()
		c := New[string, string](ctx, Config[string]{Clock: clk})
		c.Set("a", sw.target("a"))
		synctest.Wait()
		cancel()
		synctest.Wait()
		if clk.Pending() != 0 || c.Len() != 0 {
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
		clk := fakeclock.New()
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
				_ = c.Len() // callbacks may call the Checker
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
						c.Add(g, k, Target{Probe: probe, Address: k})
					case 1:
						c.SyncGroup(g, map[string]Target{k: {Probe: probe, Address: k}, key(i + 1): {Probe: probe, Address: key(i + 1)}})
					case 2:
						c.RemoveFrom(g, k)
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
		synctest.Wait()
		if c.Len() != 0 || clk.Pending() != 0 {
			t.Fatalf("%d keys and %d timers after removing every group", c.Len(), clk.Pending())
		}
		c.Close()
	})
}

func TestWatchPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "1"}, {ID: "b", Value: "2"}}); err != nil {
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
		c := Watch(t.Context(), p, target, Config[string]{Clock: clk, Options: Options{Interval: time.Second, Fall: 2, Jitter: -1}})
		synctest.Wait()
		b, _ := p.Backend("b")
		if b.State() != upstream.StateUnhealthy || !errors.Is(b.Status().LastCheck.Err, errDown) || b.Status().Health != down {
			t.Fatalf("b is %v, status %+v", b.State(), b.Status())
		}
		if a, _ := p.Backend("a"); a.Status().Health != up {
			t.Fatalf("a: %+v", a.Status())
		}
		for range 5 {
			clk.Advance(time.Second)
			synctest.Wait()
		}
		mu.Lock()
		got := slices.Clone(states)
		mu.Unlock()
		if !slices.Equal(got, []string{"b:unhealthy"}) {
			t.Fatalf("state events %v", got)
		}

		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "1"}, {ID: "c", Value: "3"}}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if c.Len() != 2 || sw.count("3") != 1 {
			t.Fatalf("checker has %d targets, c checked %d times", c.Len(), sw.count("3"))
		}
		before := sw.count("2")
		clk.Advance(time.Second)
		synctest.Wait()
		if sw.count("2") != before {
			t.Fatal("removed backend still checked")
		}
		c.Close()
		if err := p.Put(upstream.Target[string]{ID: "d", Value: "4"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if sw.count("4") != 0 {
			t.Fatal("closed checker still follows the pool")
		}
	})
}

func TestWatchClosedContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		c := Watch(ctx, p, func(*upstream.Backend[string]) Target { return Target{} }, Config[string]{Clock: fakeclock.New()})
		synctest.Wait()
		c.Close()
		if err := p.Put(upstream.Target[string]{ID: "a"}); err != nil {
			t.Fatal(err)
		}
		if c.Len() != 0 {
			t.Fatal("checker started after its context ended")
		}
	})
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
	defer checkGoroutines(t)()
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
			if err != nil {
				return
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
		if n := <-received; n != 0 {
			t.Fatalf("the check sent %d bytes", n)
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

// checkGoroutines returns a function that fails the test if goroutines
// started since remain after a grace period.
func checkGoroutines(t *testing.T) func() {
	before := runtime.NumGoroutine()
	return func() {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := runtime.NumGoroutine(); n > before {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Errorf("%d goroutines leaked:\n%s", n-before, strings.TrimSpace(string(buf)))
		}
	}
}

func TestDisableCancelsCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
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
		c.Set("a", Target{Probe: probe})
		close(gate)
		synctest.Wait() // first check: up
		gate = make(chan struct{})
		clk.Advance(time.Hour)
		synctest.Wait() // second check hangs
		c.Set("a", Target{Probe: probe, Options: Options{Disabled: true}})
		synctest.Wait()
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
		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "old"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		sw.set("old", true)
		c := Watch(t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: fakeclock.New()})
		defer c.Close()
		synctest.Wait()
		// Reports about the old address do not apply to the new one.
		c.mu.Lock()
		sink := c.sink
		c.mu.Unlock()
		if err := p.Put(upstream.Target[string]{ID: "a", Value: "new"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		sink(Report[string]{Result: Result{Health: down, Changed: true}, Key: "a", Target: Target{Probe: sw.probe, Address: "old"}})
		if b, _ := p.Backend("a"); b.Status().Health != up {
			t.Fatalf("stale report applied: %+v", b.Status())
		}
	})
}

func TestCloseFromCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *Checker[string, string]
		closed := make(chan struct{})
		c = New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), OnChange: func(Report[string]) {
			c.Close() // must not wait for this callback
			close(closed)
		}})
		sw := newSwitchable()
		c.Set("a", sw.target("a"))
		<-closed
		c.Close() // from outside too: does not wait
		<-c.Done()
		if c.Len() != 0 {
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
		c = New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), OnResult: rep.add})
		c.Set("a", Target{Probe: probe})
		<-c.Done()
		if rep.len() != 0 {
			t.Fatal("result of a check that closed the checker was reported")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		// Removal methods from a callback.
		var c *Checker[string, string]
		sw := newSwitchable()
		c = New[string, string](t.Context(), Config[string]{Clock: fakeclock.New(), OnChange: func(r Report[string]) {
			c.RemoveGroup("g")
			c.SyncGroup("h", map[string]Target{"z": sw.target("z")})
			c.Remove(r.Key)
		}})
		defer c.Close()
		c.Add("g", "a", sw.target("a"))
		synctest.Wait()
		if _, ok := c.Status("a"); ok || len(c.Keys("g")) != 0 {
			t.Fatal("removal from a callback did not apply")
		}
	})
}

func TestNoCallbackAfterRemove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
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
				if r.Key == "block" && r.Health == up && r.Changed {
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
		c.Set("block", sw.target("block"))
		synctest.Wait() // the dispatcher is stuck in block's first callback
		c.Set("x", sw.target("x"))
		c.Set("y", sw.target("y"))
		for range 50 {
			clk.Advance(time.Second)
			synctest.Wait()
		}
		gen := func() uint64 { s, _ := c.Status("x"); return s.Generation }()
		c.Remove("x")
		mu.Lock()
		removedAt = true
		mu.Unlock()
		close(block)
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		if late != 0 || xReports != 0 {
			t.Fatalf("%d callbacks for x after Remove (%d in all)", late, xReports)
		}
		if removedGen != gen || gen == 0 {
			t.Fatalf("OnRemove generation %d, want %d", removedGen, gen)
		}
		c.Set("x", sw.target("x"))
		if s, _ := c.Status("x"); s.Generation == gen {
			t.Fatal("re-added key kept its generation")
		}
	})
}

func TestBlockedCallbackCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		block := make(chan struct{})
		var n atomic.Int32
		c := New[string, string](t.Context(), Config[string]{
			Clock:   clk,
			Options: Options{Interval: time.Second, Jitter: -1},
			OnResult: func(r Report[string]) {
				if r.Key == "block" && r.Changed {
					<-block
				}
				if r.Key == "y" {
					n.Add(1)
				}
			},
		})
		defer c.Close()
		c.Set("block", sw.target("block"))
		synctest.Wait()
		c.Set("y", sw.target("y"))
		synctest.Wait()
		for range 100 {
			clk.Advance(time.Second)
			synctest.Wait()
		}
		c.mu.Lock()
		queued := len(c.queue)
		c.mu.Unlock()
		if queued > 4 {
			t.Fatalf("%d callbacks queued behind a blocked one", queued)
		}
		close(block)
		synctest.Wait()
		if s, _ := c.Status("y"); n.Load() > 3 || s.Checks != 101 {
			t.Fatalf("y: %d reports delivered, %d checks", n.Load(), s.Checks)
		}
	})
}

func TestWatchCloseFromPoolSubscriber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Update([]upstream.Target[string]{{ID: "a", Value: "a"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		sw.set("a", true)
		var c *Checker[string, struct{}]
		var once sync.Once
		ready := make(chan struct{})
		p.Subscribe(func(ev upstream.Event[string]) {
			if ev.Kind == upstream.EventStateChanged {
				<-ready
				once.Do(c.Close) // runs on the checker's dispatcher, through ReportCheck
			}
		})
		c = Watch(t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: fakeclock.New()})
		close(ready)
		synctest.Wait()
		c.Close()
		if c.Len() != 0 {
			t.Fatal("checker still running")
		}
	})
}
