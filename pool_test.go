package upstream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

func targets(ids ...string) []Target[string] {
	ts := make([]Target[string], len(ids))
	for i, id := range ids {
		ts[i] = Target[string]{ID: id, Value: "addr-" + id}
	}
	return ts
}

// ordered returns targets for ids with priorities in their order, an
// active-backup setup.
func ordered(ids ...string) []Target[string] {
	ts := targets(ids...)
	for i := range ts {
		ts[i].Priority = i
	}
	return ts
}

// newOrderedPool is newTestPool with the members in tiers of one, in order.
func newOrderedPool(t *testing.T, cfg Config, ids ...string) *Pool[string] {
	t.Helper()
	p := New[string](cfg)
	t.Cleanup(p.Close)
	if err := p.Sync(ordered(ids...)); err != nil {
		t.Fatal(err)
	}
	return p
}

func newTestPool(t *testing.T, cfg Config, ids ...string) *Pool[string] {
	t.Helper()
	p := New[string](cfg)
	t.Cleanup(p.Close)
	if err := p.Sync(targets(ids...)); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustPick(t *testing.T, p *Pool[string]) Lease[string] {
	t.Helper()
	l, err := p.Pick()
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	return l
}

// pickIDs picks n times, releasing each lease at once, and counts IDs.
func pickIDs(t *testing.T, p *Pool[string], n int) map[string]int {
	t.Helper()
	m := make(map[string]int)
	for range n {
		l := mustPick(t, p)
		m[l.Backend().ID()]++
		l.Release(Success)
	}
	return m
}

func TestPoolBasics(t *testing.T) {
	p := New[string](Config{})
	defer p.Close()
	if _, err := p.Pick(); !errors.Is(err, ErrNoBackends) {
		t.Fatalf("empty pool: %v, want ErrNoBackends", err)
	}
	if err := p.Sync([]Target[string]{{ID: "a", Value: "x", Weight: 3}, {ID: "b", Value: "y"}}); err != nil {
		t.Fatal(err)
	}
	got := pickIDs(t, p, 400)
	if got["a"] != 300 || got["b"] != 100 {
		t.Fatalf("counts %v, want a:300 b:100", got)
	}
	l := mustPick(t, p)
	if v := l.Value(); v != "x" && v != "y" {
		t.Fatalf("Value = %q", v)
	}
	if l.Backend().InFlight() != 1 {
		t.Fatalf("InFlight = %d, want 1", l.Backend().InFlight())
	}
	b := l.Backend()
	l.Release(Failure)
	if l.Backend() != nil || l.Value() != "" {
		t.Fatal("released lease still refers to its backend")
	}
	s := b.Status()
	if s.InFlight != 0 || s.Failures != 1 || s.State != StateHealthy {
		t.Fatalf("status %+v", s)
	}
	if p.Len() != 2 || len(p.Status()) != 2 {
		t.Fatalf("Len %d, Status %d entries", p.Len(), len(p.Status()))
	}
	if _, ok := p.Backend("zz"); ok {
		t.Fatal("found unknown backend")
	}
}

func TestPoolInvalidTargets(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	tooHeavy := int64(MaxWeight)
	tooHeavy++ // negative where int has 32 bits, which is invalid too
	for _, ts := range [][]Target[string]{
		{{ID: ""}},
		{{ID: "x", Weight: -1}},
		{{ID: "x", Weight: int(tooHeavy)}},
		{{ID: "x"}, {ID: "x"}},
	} {
		if err := p.Sync(ts); !errors.Is(err, ErrInvalidTarget) {
			t.Errorf("Update(%v) = %v, want ErrInvalidTarget", ts, err)
		}
	}
	if err := p.Put(Target[string]{}); !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("Put = %v", err)
	}
	if ids := backendIDs(p); !slices.Equal(ids, []string{"a"}) {
		t.Fatalf("membership changed by a failed update: %v", ids)
	}
}

func backendIDs(p *Pool[string]) []string {
	var ids []string
	for _, b := range p.Backends() {
		ids = append(ids, b.ID())
	}
	return ids
}

func TestPoolUpdateKeepsState(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	a, _ := p.Backend("a")
	l := mustPick(t, p)
	for l.Backend().ID() != "a" {
		l.Release(Success)
		l = mustPick(t, p)
	}
	p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("down")})

	if err := p.Sync([]Target[string]{{ID: "b"}, {ID: "a", Value: "new", Weight: 7}}); err != nil {
		t.Fatal(err)
	}
	a2, _ := p.Backend("a")
	if a2 == a || a2.Weight() != 7 || a2.Value() != "new" {
		t.Fatalf("a not updated: %+v", a2.Status())
	}
	if a2.InFlight() != 1 || a2.State() != StateUnhealthy {
		t.Fatalf("a lost its state: in flight %d, %v", a2.InFlight(), a2.State())
	}
	if l.Value() != "addr-a" {
		t.Fatal("lease changed version")
	}
	l.Release(Success)
	if a2.InFlight() != 0 {
		t.Fatal("release on old version did not reach shared counters")
	}
	if ids := backendIDs(p); !slices.Equal(ids, []string{"b", "a"}) {
		t.Fatalf("order %v", ids)
	}

	// Put replaces in place and appends new members.
	if err := p.Put(Target[string]{ID: "b", Weight: 2}); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(Target[string]{ID: "c"}); err != nil {
		t.Fatal(err)
	}
	if ids := backendIDs(p); !slices.Equal(ids, []string{"b", "a", "c"}) {
		t.Fatalf("order %v", ids)
	}
}

func TestPoolRemoveIsGraceful(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	l := mustPick(t, p)
	b := l.Backend()
	drained, ok := p.Remove("a")
	if !ok {
		t.Fatal("Remove failed")
	}
	if _, ok := p.Remove("a"); ok {
		t.Fatal("removed twice")
	}
	if _, err := p.Pick(); !errors.Is(err, ErrNoBackends) {
		t.Fatalf("Pick after remove: %v", err)
	}
	select {
	case <-drained.Done():
		t.Fatal("drained with a lease in flight")
	default:
	}
	if b.State() != StateRemoved {
		t.Fatalf("state %v", b.State())
	}
	l.Release(Success)
	<-drained.Done()

	// Removed by Update, with nothing in flight.
	p2 := newTestPool(t, Config{}, "a", "b")
	l = mustPick(t, p2)
	if err := p2.Sync(targets("c")); err != nil {
		t.Fatal(err)
	}
	if got := pickIDs(t, p2, 10); got["c"] != 10 {
		t.Fatalf("picks %v", got)
	}
	l.Release(Success)
	if l.Backend() != nil {
		t.Fatal("lease not cleared")
	}
}

func TestPoolDrain(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	var la Lease[string]
	for {
		la = mustPick(t, p)
		if la.Backend().ID() == "a" {
			break
		}
		la.Release(Success)
	}
	drained, ok := p.Drain("a")
	if !ok {
		t.Fatal("Drain failed")
	}
	if got := pickIDs(t, p, 20); got["a"] != 0 {
		t.Fatalf("draining backend picked: %v", got)
	}
	a, _ := p.Backend("a")
	if a.State() != StateDraining || !a.Status().Draining {
		t.Fatalf("state %v", a.State())
	}
	la.Release(Neutral)
	<-drained.Done()

	// Draining again returns a closed channel; Resume brings it back.
	again, _ := p.Drain("a")
	<-again.Done()
	if !p.Resume("a") {
		t.Fatal("Resume failed")
	}
	if got := pickIDs(t, p, 20); got["a"] != 10 {
		t.Fatalf("resumed backend picks: %v", got)
	}

	// Resume ends a pending drain, as canceled, so waiters never leak.
	l := mustPick(t, p)
	pending, _ := p.Drain(l.Backend().ID())
	if pending.Canceled() {
		t.Fatal("canceled before it ended")
	}
	p.Resume(l.Backend().ID())
	<-pending.Done()
	if !pending.Canceled() {
		t.Fatal("drain ended by Resume not reported as canceled")
	}
	l.Release(Success)
	if again.Canceled() || drained.Canceled() {
		t.Fatal("completed drains reported as canceled")
	}

	if _, ok := p.Drain("zz"); ok || p.Resume("zz") {
		t.Fatal("drained unknown backend")
	}
}

func TestPoolHealthAndFallback(t *testing.T) {
	for _, fb := range []Fallback{FallbackNone, FallbackAll} {
		p := newTestPool(t, Config{Fallback: fb}, "a", "b")
		a, _ := p.Backend("a")
		b, _ := p.Backend("b")
		p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("a down")})
		if got := pickIDs(t, p, 10); got["b"] != 10 {
			t.Fatalf("picks %v, want b only", got)
		}
		if a.Status().LastCheck.Err == nil || a.Status().Health != HealthDown {
			t.Fatalf("status %+v", a.Status())
		}
		p.ReportCheck(b, HealthDown, CheckResult{})
		_, err := p.Pick()
		switch fb {
		case FallbackNone:
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("all down: %v, want ErrUnavailable", err)
			}
		case FallbackAll:
			if err != nil {
				t.Fatalf("panic mode: %v", err)
			}
			if got := pickIDs(t, p, 20); got["a"] != 10 || got["b"] != 10 {
				t.Fatalf("panic mode picks %v", got)
			}
			// Draining backends stay out even in panic mode.
			p.Drain("a")
			if got := pickIDs(t, p, 10); got["b"] != 10 {
				t.Fatalf("panic mode picked a draining backend: %v", got)
			}
			p.Drain("b")
			if _, err := p.Pick(); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("all draining: %v", err)
			}
			p.Resume("b")
		}
		p.ReportCheck(a, HealthUp, CheckResult{})
		if got := pickIDs(t, p, 10); got["a"] == 0 && fb == FallbackNone {
			t.Fatalf("recovered backend not picked: %v", got)
		}
	}
}

func TestPoolIgnoresStaleReports(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	old, _ := p.Backend("a")
	p.Remove("a")
	if err := p.Put(Target[string]{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	p.ReportCheck(old, HealthDown, CheckResult{})
	cur, _ := p.Backend("a")
	if cur.State() != StateHealthy {
		t.Fatal("report for a removed backend applied to its successor")
	}
}

func TestLeaseMisuse(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	l := mustPick(t, p)
	b := l.Backend()
	c := l // a copy shares the slot
	l.Release(Success)
	l.Release(Success) // the same variable again: no-op
	if b.InFlight() != 0 {
		t.Fatalf("in flight %d after double release", b.InFlight())
	}
	var zero Lease[string]
	zero.Release(Failure)
	if zero.Backend() != nil {
		t.Fatal("zero lease has a backend")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("releasing a copy of a released lease did not panic")
		}
		if b.InFlight() != -1 {
			// The panic happens after the decrement; the count is
			// already wrong, which is why it panics.
			t.Logf("in flight after misuse: %d", b.InFlight())
		}
	}()
	c.Release(Success)
}

func TestPoolDo(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	a, _ := p.Backend("a")
	boom := errors.New("boom")
	if err := p.Do(context.Background(), func(*Backend[string]) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Do = %v", err)
	}
	if err := p.DoKey(context.Background(), "k", func(b *Backend[string]) error {
		if b.InFlight() != 1 {
			t.Errorf("in flight %d inside Do", b.InFlight())
		}
		return context.Canceled
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("DoKey = %v", err)
	}
	func() {
		defer func() { _ = recover() }()
		_ = p.Do(context.Background(), func(*Backend[string]) error { panic("x") })
	}()
	if s := a.Status(); s.InFlight != 0 || s.Failures != 2 || s.Successes != 0 {
		t.Fatalf("status after Do: %+v", s)
	}
	p.Remove("a")
	if err := p.Do(context.Background(), func(*Backend[string]) error { return nil }); !errors.Is(err, ErrNoBackends) {
		t.Fatalf("Do on empty pool = %v", err)
	}
	if err := p.DoKey(context.Background(), "k", func(*Backend[string]) error { return nil }); !errors.Is(err, ErrNoBackends) {
		t.Fatalf("DoKey on empty pool = %v", err)
	}
}

func TestPoolClose(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	l := mustPick(t, p)
	p.Close()
	p.Close()
	if _, err := p.Pick(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pick after Close: %v", err)
	}
	if err := p.Sync(targets("b")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Update after Close: %v", err)
	}
	if err := p.Put(Target[string]{ID: "b"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: %v", err)
	}
	l.Release(Success)
	cancel := p.Subscribe(func(Event[string]) { t.Error("event after Close") })
	cancel()
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(ev Event[string]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := ev.Kind.String() + " " + ev.Backend.ID()
	if ev.Kind == EventStateChanged {
		s += " " + ev.From.String() + "->" + ev.To.String()
	}
	l.events = append(l.events, s)
}

func (l *eventLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.events
	l.events = nil
	return e
}

func TestPoolEvents(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	var log eventLog
	cancel := p.Subscribe(log.add)
	want := []string{"added a", "added b"}
	if got := log.take(); !slices.Equal(got, want) {
		t.Fatalf("replay %v, want %v", got, want)
	}

	a, _ := p.Backend("a")
	for range 5 {
		p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("x")})
	}
	p.ReportCheck(a, HealthUp, CheckResult{})
	p.Drain("b")
	p.Resume("b")
	if err := p.Sync(targets("b", "c")); err != nil {
		t.Fatal(err)
	}
	want = []string{
		"state-changed a healthy->unhealthy",
		"state-changed a unhealthy->healthy",
		"state-changed b healthy->draining",
		"state-changed b draining->healthy",
		"removed a", "added c", // b is unchanged
	}
	if got := log.take(); !slices.Equal(got, want) {
		t.Fatalf("events\n%v\nwant\n%v", got, want)
	}
	cancel()
	cancel()
	p.Remove("c")
	if got := log.take(); len(got) != 0 {
		t.Fatalf("events after cancel: %v", got)
	}
}

func TestSubscriberMayCallPool(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	var log eventLog
	p.Subscribe(log.add)
	log.take()
	// A subscriber that reacts to every addition by draining it. Its own
	// events are delivered after it returns, in order.
	p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventAdded && ev.Backend.ID() != "a" {
			p.Drain(ev.Backend.ID())
		}
	})
	if err := p.Put(Target[string]{ID: "x"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"added x", "state-changed x healthy->draining"}
	if got := log.take(); !slices.Equal(got, want) {
		t.Fatalf("events %v, want %v", got, want)
	}

	// A panicking subscriber does not wedge delivery.
	p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventAdded && ev.Backend.ID() == "boom" {
			panic("subscriber")
		}
	})
	func() {
		defer func() { _ = recover() }()
		_ = p.Put(Target[string]{ID: "boom"})
	}()
	log.take()
	p.Remove("boom")
	if got := log.take(); !slices.Contains(got, "removed boom") {
		t.Fatalf("delivery stuck after a panic: %v", got)
	}
}

// TestPickDoesNotAllocate checks every balancer, also above the 256
// candidates beyond which bounded-load retries need a scratch bitset.
func TestPickDoesNotAllocate(t *testing.T) {
	for _, n := range []int{16, 300} {
		for name, b := range testBalancers() {
			testPickAllocs(t, name, b, n)
		}
	}
}

func testPickAllocs(t *testing.T, name string, b Balancer, n int) {
	{
		p := New[string](Config{Balancer: b, Outlier: OutlierConfig{ConsecutiveFailures: 1000}})
		ts := make([]Target[string], n)
		for i := range ts {
			ts[i] = Target[string]{ID: strconv.Itoa(i), Weight: i%3 + 1}
		}
		if err := p.Sync(ts); err != nil {
			t.Fatal(err)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			l, err := p.PickKey("10.0.0.1")
			if err != nil {
				panic(err)
			}
			l.Release(Success)
			l, _ = p.Pick()
			l.Release(Failure)
		})
		if allocs != 0 {
			t.Errorf("%s, %d backends: %.1f allocations per pick", name, n, allocs)
		}
		p.Close()
	}
}

func testBalancers() map[string]Balancer {
	return map[string]Balancer{
		"RoundRobin":         RoundRobin(),
		"WeightedRoundRobin": WeightedRoundRobin(),
		"Random":             Random(),
		"PowerOfTwo":         PowerOfTwo(),
		"LeastConn":          LeastConn(),
		"ConsistentHash":     ConsistentHash(HashConfig{}),
		"BoundedHash":        ConsistentHash(HashConfig{LoadFactor: 1.25}),
	}
}

// TestConcurrentUpdatesWhilePicking hammers picks and releases while the
// membership, weights, tiers, caps, drains, health and offline marks
// change underneath, then checks that every slot came back.
func TestConcurrentUpdatesWhilePicking(t *testing.T) {
	for name, bal := range testBalancers() {
		t.Run(name, func(t *testing.T) {
			p := New[string](Config{Balancer: bal, Fallback: FallbackAll, Overflow: true, Outlier: OutlierConfig{ConsecutiveFailures: 3, BaseEjection: time.Millisecond}})
			defer p.Close()
			ids := []string{"a", "b", "c", "d", "e"}
			// Two tiers, caps on some members, overflow on.
			tiered := func(ids ...string) []Target[string] {
				ts := targets(ids...)
				for i := range ts {
					ts[i].Priority = i % 2
					ts[i].MaxInFlight = i % 3
				}
				return ts
			}
			if err := p.Sync(tiered(ids...)); err != nil {
				t.Fatal(err)
			}
			var events atomic.Int64
			defer p.Subscribe(func(Event[string]) { events.Add(1) })()

			// Bounded iterations, with yields, so that the test also
			// interleaves on single-threaded targets without preemption.
			var stop atomic.Bool
			var wg sync.WaitGroup
			var all sync.Map // every backend version ever seen
			for w := range 8 {
				wg.Go(func() {
					for i := 0; !stop.Load(); i++ {
						if i%64 == 0 {
							runtime.Gosched()
						}
						l, err := p.PickKey(strconv.Itoa(i))
						if err != nil {
							continue
						}
						all.Store(l.Backend().core, true)
						o := Success
						if (i+w)%7 == 0 {
							o = Failure
						}
						l.Release(o)
					}
				})
			}
			wg.Go(func() {
				defer stop.Store(true)
				for i := range 3000 {
					runtime.Gosched()
					id := ids[i%len(ids)]
					switch i % 7 {
					case 0:
						_ = p.Sync(tiered(ids[:1+i%len(ids)]...))
					case 1:
						_ = p.Put(Target[string]{ID: id, Weight: i%5 + 1, Priority: i % 3, MaxInFlight: i % 4})
					case 6:
						if b, ok := p.Backend(id); ok {
							if i%2 == 0 {
								p.MarkOffline(b, nil)
							} else {
								p.MarkOnline(b)
							}
						}
					case 2:
						p.Drain(id)
					case 3:
						p.Resume(id)
					case 4:
						if b, ok := p.Backend(id); ok {
							p.ReportCheck(b, Health(i%3), CheckResult{})
						}
					case 5:
						p.Remove(id)
					}
				}
			})
			wg.Wait()
			all.Range(func(k, _ any) bool {
				if n := k.(*core).inflight.Load(); n != 0 {
					t.Errorf("a backend has %d leases in flight after all were released", n)
				}
				return true
			})
			if events.Load() == 0 {
				t.Error("no events")
			}
		})
	}
}

func ExampleCandidate() {
	// Balancers work without a pool.
	var loadA, loadB atomic.Int64
	loadA.Store(5)
	p := LeastConn().Build([]Candidate{
		{ID: "a", Weight: 1, InFlight: &loadA},
		{ID: "b", Weight: 1, InFlight: &loadB},
	})
	fmt.Println(p.Pick(0, 0))
	// Output: 1
}

func TestPoolReentrantFromSubscriber(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	var calls int
	p.Subscribe(func(ev Event[string]) {
		calls++
		if ev.Kind == EventRemoved {
			_ = p.Sync(targets("c")) // its events are not delivered: Close follows
			p.Close()
		}
	})
	calls = 0
	p.Remove("a")
	if calls != 1 {
		t.Fatalf("%d events delivered, want 1: none starts after Close", calls)
	}
	if _, err := p.Pick(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pick after Close from a subscriber: %v", err)
	}
}

// reentrantBalancer calls back into its pool while building.
type reentrantBalancer struct{ p *Pool[string] }

func (b *reentrantBalancer) Build(c []Candidate) Picker {
	if b.p != nil {
		_ = b.p.Status()
		_, _ = b.p.Backend("a")
		if l, err := b.p.Pick(); err == nil {
			l.Release(Neutral)
		}
	}
	return RoundRobin().Build(c)
}

func TestBalancerMayCallPool(t *testing.T) {
	bal := &reentrantBalancer{}
	p := New[string](Config{Balancer: bal})
	defer p.Close()
	bal.p = p
	if err := p.Sync(targets("a", "b")); err != nil {
		t.Fatal(err)
	}
	l := mustPick(t, p)
	l.Release(Success)
}

// ejectToHalfOpen ejects backend id with failures and waits for its
// ejection to end.
func ejectToHalfOpen(t *testing.T, p *Pool[string], clk *fakeclock.Clock, id string) {
	t.Helper()
	b, _ := p.Backend(id)
	for b.Status().Outlier.State == OutlierOK {
		l := mustPick(t, p)
		o := Success
		if l.Backend().ID() == id {
			o = Failure
		}
		l.Release(o)
	}
	clk.Advance(time.Hour)
	synctest.Wait()
	if b.State() != StateHalfOpen {
		t.Fatalf("%s is %v", id, b.State())
	}
}

func TestPickFallsBackWhenBalancerInsists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 1}})
		defer p.Close()
		if err := p.Sync([]Target[string]{{ID: "heavy", Weight: 50}, {ID: "light"}}); err != nil {
			t.Fatal(err)
		}
		ejectToHalfOpen(t, p, clk, "heavy")
		var trial Lease[string]
		for trial.Backend() == nil {
			l := mustPick(t, p)
			if l.Backend().ID() == "heavy" {
				trial = l
			} else {
				l.Release(Success)
			}
		}
		// The balancer keeps choosing heavy, which has no trial slot.
		for range 200 {
			l, err := p.Pick()
			if err != nil {
				t.Fatalf("Pick with a healthy backend available: %v", err)
			}
			if l.Backend().ID() != "light" {
				t.Fatal("picked heavy past its trial limit")
			}
			l.Release(Success)
		}
		trial.Release(Neutral)
	})
}

func TestSubscriberPanicLeaksNothing(t *testing.T) {
	p := New[string](Config{Outlier: OutlierConfig{ConsecutiveFailures: 1}})
	defer p.Close()
	if err := p.Sync(targets("a", "b")); err != nil {
		t.Fatal(err)
	}
	var log eventLog
	p.Subscribe(log.add)
	panicked := false
	p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventStateChanged && !panicked {
			panicked = true
			panic("subscriber")
		}
	})
	log.take()
	l := mustPick(t, p)
	b := l.Backend()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		l.Release(Failure) // ejects, which runs the subscribers
	}()
	if b.InFlight() != 0 {
		t.Fatalf("in flight %d after a panicking release", b.InFlight())
	}
	// The event the second subscriber panicked on still reached the
	// first one; the next flush delivers what was left.
	p.Drain("zz")
	_ = p.Put(Target[string]{ID: "c"})
	got := log.take()
	if len(got) < 2 || got[0] != "state-changed "+b.ID()+" healthy->ejected" || got[len(got)-1] != "added c" {
		t.Fatalf("events after the panic: %v", got)
	}
}

func TestDrainResumeRace(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	var wg sync.WaitGroup
	chans := make(chan *Drain, 4000)
	for range 4 {
		wg.Go(func() {
			for range 500 {
				ch, _ := p.Drain("a")
				chans <- ch
				p.Resume("a")
				if l, err := p.Pick(); err == nil {
					l.Release(Success)
				}
			}
		})
	}
	wg.Wait()
	close(chans)
	timeout := time.After(10 * time.Second)
	for ch := range chans {
		select {
		case <-ch.Done():
		case <-timeout:
			t.Fatal("a drain channel never closed")
		}
	}
}

func TestActiveHealthUnknown(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	a, _ := p.Backend("a")
	p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("x")})
	if _, err := p.Pick(); !errors.Is(err, ErrUnavailable) {
		t.Fatal("down backend picked")
	}
	p.ReportCheck(a, HealthUnknown, CheckResult{})
	if a.Status().Health != HealthUnknown || a.State() != StateHealthy {
		t.Fatalf("status %+v", a.Status())
	}
	l := mustPick(t, p)
	l.Release(Success)
	p.ReportCheck(a, HealthUp, CheckResult{})
	if a.Status().Health != HealthUp {
		t.Fatal("not up")
	}
	for h := HealthUnknown; h <= HealthDown+1; h++ {
		if h.String() == "" {
			t.Fatal("unnamed health")
		}
	}
}

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

// TestFallbackAllWhenEligibleRefuse checks that FallbackAll also serves a
// pick that every eligible member refuses: half-open members out of trial
// slots, as after an ejection of the whole pool ends.
func TestFallbackAllWhenEligibleRefuse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := newTestPool(t, Config{
			Clock:    clk,
			Fallback: FallbackAll,
			Outlier:  OutlierConfig{ConsecutiveFailures: 1, BaseEjection: time.Second},
		}, "a", "b")
		for _, id := range []string{"a", "b"} {
			b, _ := p.Backend(id)
			for b.State() == StateHealthy {
				l := mustPick(t, p)
				o := Neutral
				if l.Backend().ID() == id {
					o = Failure
				}
				l.Release(o)
			}
		}
		clk.Advance(time.Second)
		synctest.Wait()
		var leases []Lease[string]
		for range 4 {
			leases = append(leases, mustPick(t, p))
		}
		if n := p.Available(); n != 2 {
			t.Fatalf("Available = %d, want the 2 half-open members", n)
		}
		for i := range leases {
			leases[i].Release(Neutral)
		}
		q := newTestPool(t, Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 1, BaseEjection: time.Second}}, "a")
		ejectToHalfOpen(t, q, clk, "a")
		l := mustPick(t, q)
		if _, err := q.Pick(); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("FallbackNone pick beyond the trial slots = %v", err)
		}
		l.Release(Neutral)
	})
}

// countingBalancer counts the Pickers it builds.
type countingBalancer struct {
	Balancer
	builds atomic.Int64
}

func (b *countingBalancer) Build(c []Candidate) Picker {
	b.builds.Add(1)
	return b.Balancer.Build(c)
}

// TestUpdatesThatChangeNothing checks that reports and updates that change
// no routing build nothing, keep the Backend and emit no event.
func TestUpdatesThatChangeNothing(t *testing.T) {
	bal := &countingBalancer{Balancer: RoundRobin()}
	p := newTestPool(t, Config{Balancer: bal}, "a", "b", "c")
	var log eventLog
	defer p.Subscribe(log.add)()
	log.take()
	before := p.Backends()
	bal.builds.Store(0)

	for _, b := range before {
		p.ReportCheck(b, HealthUp, CheckResult{})
		p.ReportCheck(b, HealthUnknown, CheckResult{})
	}
	if err := p.Sync(targets("a", "b", "c")); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(Target[string]{ID: "b", Value: "addr-b"}); err != nil {
		t.Fatal(err)
	}
	if n := bal.builds.Load(); n != 0 {
		t.Fatalf("%d builds", n)
	}
	if got := log.take(); len(got) != 0 {
		t.Fatalf("events %v", got)
	}
	if !slices.Equal(p.Backends(), before) {
		t.Fatal("backends replaced")
	}
	if h := before[0].Status().Health; h != HealthUnknown {
		t.Fatalf("health %v", h)
	}

	// A new order, a new weight and a new value each count.
	_ = p.Sync(targets("c", "b", "a"))
	_ = p.Put(Target[string]{ID: "b", Value: "addr-b", Weight: 2})
	_ = p.Put(Target[string]{ID: "b", Value: "other", Weight: 2})
	if n := bal.builds.Load(); n != 3 {
		t.Fatalf("%d builds, want 3", n)
	}
	if got, want := log.take(), []string{"updated b", "updated b"}; !slices.Equal(got, want) {
		t.Fatalf("events %v, want %v", got, want)
	}

	// Values that are not comparable change on every update unless
	// Config.Equal says otherwise.
	sp := New[[]string](Config{Balancer: bal})
	defer sp.Close()
	ts := []Target[[]string]{{ID: "x", Value: []string{"1"}}}
	_ = sp.Sync(ts)
	bal.builds.Store(0)
	_ = sp.Sync(ts)
	if bal.builds.Load() != 1 {
		t.Fatal("a slice value compared equal")
	}
	_ = sp.SetConfig(Config{Balancer: bal, Equal: func(a, b any) bool { return slices.Equal(a.([]string), b.([]string)) }})
	bal.builds.Store(0)
	_ = sp.Sync(ts)
	if bal.builds.Load() != 0 {
		t.Fatal("Equal ignored")
	}
}

// gateBalancer blocks the Build calls it is told to block.
type gateBalancer struct {
	block   atomic.Bool
	entered chan struct{}
	gate    chan struct{}
	sizes   chan int
}

func (b *gateBalancer) Build(c []Candidate) Picker {
	if b.block.CompareAndSwap(true, false) {
		b.entered <- struct{}{}
		<-b.gate
	}
	b.sizes <- len(c)
	return RoundRobin().Build(c)
}

// TestRebuildsCoalesce checks that commits during a build return at once
// and leave one more build to the building goroutine, which publishes
// their changes.
func TestRebuildsCoalesce(t *testing.T) {
	bal := &gateBalancer{entered: make(chan struct{}), gate: make(chan struct{}), sizes: make(chan int, 16)}
	p := newTestPool(t, Config{Balancer: bal}, "a")
	<-bal.sizes
	bal.block.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.Put(Target[string]{ID: "b"})
	}()
	<-bal.entered
	for _, id := range []string{"c", "d", "e"} {
		if err := p.Put(Target[string]{ID: id}); err != nil { // returns without building
			t.Fatal(err)
		}
	}
	if len(bal.sizes) != 0 {
		t.Fatal("a commit built while another build was in progress")
	}
	close(bal.gate)
	<-done
	if got := []int{<-bal.sizes, <-bal.sizes}; !slices.Equal(got, []int{2, 5}) {
		t.Fatalf("builds of %v candidates, want [2 5]", got)
	}
	if n := p.Available(); n != 5 {
		t.Fatalf("Available = %d", n)
	}
}

// TestMaxEjectionPercent checks that passive health checking keeps the
// share of members out within the cap, and ejects more once one recovers.
func TestMaxEjectionPercent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := newTestPool(t, Config{Clock: clk, Outlier: OutlierConfig{
			ConsecutiveFailures: 1, BaseEjection: time.Second, MaxEjectionPercent: 50,
		}}, "a", "b", "c", "d")
		fail := func(id string) {
			b, _ := p.Backend(id)
			l, _ := p.Pick()
			for l.Backend() != b {
				l.Release(Neutral)
				l = mustPick(t, p)
			}
			l.Release(Failure)
		}
		for _, id := range []string{"a", "b", "c", "d"} {
			fail(id)
		}
		var out []string
		for _, s := range p.Status() {
			if s.State != StateHealthy {
				out = append(out, s.ID)
			}
		}
		if !slices.Equal(out, []string{"a", "b"}) {
			t.Fatalf("out of rotation: %v", out)
		}
		if c, _ := p.Backend("c"); c.Status().Outlier.Refused != 1 {
			t.Fatalf("refused %d", c.Status().Outlier.Refused)
		}
		p.ResetOutlier("a")
		fail("c")
		if c, _ := p.Backend("c"); c.State() != StateEjected {
			t.Fatalf("c is %v after a recovered", c.State())
		}
		// A removed member gives its slot back.
		p.Remove("b")
		p.Remove("c")
		fail("d")
		if d, _ := p.Backend("d"); d.State() != StateEjected {
			t.Fatalf("d is %v", d.State())
		}
	})
}

// TestPanicThreshold checks that FallbackAll panics while too few members
// are eligible.
func TestPanicThreshold(t *testing.T) {
	p := newTestPool(t, Config{Fallback: FallbackAll, PanicThreshold: 50}, "a", "b", "c", "d")
	bs := p.Backends()
	p.ReportCheck(bs[0], HealthDown, CheckResult{})
	p.ReportCheck(bs[1], HealthDown, CheckResult{})
	if n := p.Available(); n != 2 {
		t.Fatalf("at the threshold Available = %d", n)
	}
	p.ReportCheck(bs[2], HealthDown, CheckResult{})
	if n := p.Available(); n != 0 {
		t.Fatalf("below the threshold Available = %d, want panic mode", n)
	}
	if got := pickIDs(t, p, 400); len(got) != 4 {
		t.Fatalf("panic mode picked %v", got)
	}
	p.Drain("a")
	p.Drain("b")
	p.Drain("c")
	if n := p.Available(); n != 1 { // one of one member that is not draining
		t.Fatalf("Available = %d", n)
	}
}

// TestSetConfig checks that a running pool takes a new configuration and
// keeps its members' state.
func TestSetConfig(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := newTestPool(t, Config{Clock: clk, Balancer: inOrder{}}, "a", "b")
		a, _ := p.Backend("a")
		l := mustPick(t, p)
		if l.Backend() != a {
			t.Fatal("in-order balancer picked b")
		}

		if err := p.SetConfig(Config{Balancer: RoundRobin(), Outlier: OutlierConfig{ConsecutiveFailures: 1}, Retry: RetryConfig{Attempts: 2}}); err != nil {
			t.Fatal(err)
		}
		if got := pickIDs(t, p, 10); got["b"] == 0 {
			t.Fatalf("round-robin picked %v", got)
		}
		if a.InFlight() != 1 {
			t.Fatal("in-flight count lost")
		}
		l.Release(Failure) // admitted before passive checks were on: not counted
		if a.State() != StateHealthy {
			t.Fatalf("a is %v", a.State())
		}
		for a.State() == StateHealthy {
			l := mustPick(t, p)
			o := Neutral
			if l.Backend() == a {
				o = Failure
			}
			l.Release(o)
		}
		r := p.Request("")
		_, _ = r.Next()
		if _, err := r.Next(); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("retry = %v", err)
		}

		// Disabling passive health checking clears the ejection.
		if err := p.SetConfig(Config{Clock: clk}); err != nil {
			t.Fatal(err)
		}
		if a.State() != StateHealthy || clk.Pending() != 0 {
			t.Fatalf("a is %v with %d timers", a.State(), clk.Pending())
		}
		p.Close()
		if err := p.SetConfig(Config{}); !errors.Is(err, ErrClosed) {
			t.Fatalf("SetConfig after Close = %v", err)
		}
	})
}

// TestCandidateMetaAndLatency checks what a custom balancer sees.
func TestCandidateMetaAndLatency(t *testing.T) {
	var got []Candidate
	bal := balancerFunc(func(c []Candidate) Picker {
		got = slices.Clone(c)
		return RoundRobin().Build(c)
	})
	p := newTestPool(t, Config{Balancer: bal}, "a")
	a, _ := p.Backend("a")
	if got[0].Meta.(*Backend[string]) != a || got[0].Latency.Load() != 0 {
		t.Fatalf("candidate %+v", got[0])
	}
	l := mustPick(t, p)
	l.Observe(80 * time.Millisecond)
	l.Observe(160 * time.Millisecond)
	l.Release(Success)
	l.Observe(time.Hour) // released: ignored
	if d := time.Duration(got[0].Latency.Load()); d != 90*time.Millisecond || a.Latency() != d || a.Status().Latency != d {
		t.Fatalf("latency %v", d)
	}
}

type balancerFunc func([]Candidate) Picker

func (f balancerFunc) Build(c []Candidate) Picker { return f(c) }

// TestAddressOf checks the shared address rule.
func TestAddressOf(t *testing.T) {
	type name string
	if a, ok := AddressOf(name("h:1")); !ok || a != "h:1" {
		t.Fatal("named string")
	}
	if a, ok := AddressOf(addr{"h:2"}); !ok || a != "h:2" {
		t.Fatal("Addresser")
	}
	if _, ok := AddressOf[*addrPtr](nil); ok {
		t.Fatal("nil Addresser")
	}
	if _, ok := AddressOf(3); ok {
		t.Fatal("int")
	}
	if !Addressable[name]() || !Addressable[addr]() || !Addressable[*addrPtr]() || !Addressable[Addresser]() || Addressable[any]() || Addressable[int]() {
		t.Fatal("Addressable")
	}
}

type addr struct{ a string }

func (a addr) Address() string { return a.a }

type addrPtr struct{ a string }

func (a *addrPtr) Address() string { return a.a }

// stepBalancer blocks each Build while armed until the test lets it go.
type stepBalancer struct {
	gate  chan chan struct{}
	armed atomic.Bool
	panic atomic.Bool // the next gated Build panics once let go
}

func (b *stepBalancer) Build(c []Candidate) Picker {
	if b.armed.Load() {
		done := make(chan struct{})
		b.gate <- done
		<-done
		if b.panic.CompareAndSwap(true, false) {
			panic("balancer")
		}
	}
	return RoundRobin().Build(c)
}

// panicEvents subscribes to p and returns the BalancerPanicErrors seen.
func panicEvents(p *Pool[string]) func() []*BalancerPanicError {
	var mu sync.Mutex
	var got []*BalancerPanicError
	p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventBalancerPanic {
			mu.Lock()
			got = append(got, ev.Err.(*BalancerPanicError))
			mu.Unlock()
		}
	})
	return func() []*BalancerPanicError {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// TestPanickingBuildKeepsCoalescedCommits checks that a Balancer panic
// reaches no committer, is reported as an event, and that a commit that
// coalesced into the panicking build still reaches the view.
func TestPanickingBuildKeepsCoalescedCommits(t *testing.T) {
	bal := &stepBalancer{gate: make(chan chan struct{})}
	p := newTestPool(t, Config{Balancer: bal}, "a")
	panics := panicEvents(p)
	bal.armed.Store(true)
	bal.panic.Store(true)
	done := make(chan error)
	go func() { done <- p.Put(Target[string]{ID: "b", Value: "b"}) }()
	first := <-bal.gate
	if err := p.Put(Target[string]{ID: "c", Value: "c"}); err != nil {
		t.Fatal(err)
	}
	bal.armed.Store(false)
	close(first)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := pickIDs(t, p, 30); len(got) != 3 {
		t.Fatalf("picks %v, want a, b and c", got)
	}
	if got := panics(); len(got) != 1 || got[0].Value != "balancer" || len(got[0].Stack) == 0 {
		t.Fatalf("panic events %v, want one", got)
	}
}

// panicBalancer panics in every Build while on is set.
type panicBalancer struct {
	on     atomic.Bool
	builds atomic.Int32
}

func (b *panicBalancer) Build(c []Candidate) Picker {
	b.builds.Add(1)
	if b.on.Load() {
		panic(errors.New("broken"))
	}
	return RoundRobin().Build(c)
}

// TestBalancerPanicKeepsViewUntilNextCommit checks that a Balancer that
// panics on every build keeps the last view, is not retried in a loop,
// and is retried by the next commit, which then publishes all committed
// state.
func TestBalancerPanicKeepsViewUntilNextCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bal := &panicBalancer{}
		p := newTestPool(t, Config{Balancer: bal}, "a")
		panics := panicEvents(p)
		bal.on.Store(true)
		before := bal.builds.Load()
		if err := p.Put(Target[string]{ID: "b", Value: "b"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if n := bal.builds.Load() - before; n != 1 {
			t.Fatalf("%d builds after one commit, want 1", n)
		}
		if got := pickIDs(t, p, 10); len(got) != 1 || got["a"] == 0 {
			t.Fatalf("picks %v, want the last view (a)", got)
		}
		if got := panics(); len(got) != 1 || errors.Unwrap(got[0]) == nil {
			t.Fatalf("panic events %v, want one wrapping the error", got)
		}
		bal.on.Store(false)
		if err := p.SetConfig(Config{Balancer: bal}); err != nil {
			t.Fatal(err)
		}
		if got := pickIDs(t, p, 30); len(got) != 2 {
			t.Fatalf("picks %v, want a and b", got)
		}
	})
}

// TestHandedOffBuildPanicIsReported checks that a Balancer panic in the
// handed-off build neither crashes nor loses the commit.
func TestHandedOffBuildPanicIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bal := &stepBalancer{gate: make(chan chan struct{})}
		p := newTestPool(t, Config{Balancer: bal}, "a")
		panics := panicEvents(p)
		bal.armed.Store(true)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = p.Put(Target[string]{ID: "b", Value: "b"})
		}()
		round := <-bal.gate
		_ = p.Put(Target[string]{ID: "c", Value: "c"})
		close(round)
		round = <-bal.gate
		_ = p.Put(Target[string]{ID: "d", Value: "d"})
		close(round)
		round = <-bal.gate // handed off
		<-done
		bal.armed.Store(false)
		bal.panic.Store(true)
		close(round)
		synctest.Wait()
		if n := len(panics()); n != 1 {
			t.Fatalf("%d panic events, want 1", n)
		}
		if n := p.Available(); n != 3 {
			t.Fatalf("view has %d members, want the last published 3", n)
		}
		_ = p.Put(Target[string]{ID: "e", Value: "e"})
		if n := p.Available(); n != 5 {
			t.Fatalf("view has %d members after the next commit, want 5", n)
		}
	})
}

// TestCommitBuildsAreBounded checks that a committer builds at most twice
// and hands later commits off, which still reach the view.
func TestCommitBuildsAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bal := &stepBalancer{gate: make(chan chan struct{})}
		p := newTestPool(t, Config{Balancer: bal}, "a")
		bal.armed.Store(true)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = p.Put(Target[string]{ID: "b", Value: "b"})
		}()
		round := <-bal.gate
		_ = p.Put(Target[string]{ID: "c", Value: "c"}) // coalesces
		close(round)
		round = <-bal.gate // the committer's second build
		_ = p.Put(Target[string]{ID: "d", Value: "d"})
		close(round)
		round = <-bal.gate // handed off: the committer has returned
		<-done
		bal.armed.Store(false)
		close(round)
		synctest.Wait()
		if n := p.Available(); n != 4 {
			t.Fatalf("view has %d members, want 4", n)
		}
	})
}

// inOrder is a Balancer that picks its candidates in order.
type inOrder struct{}

func (inOrder) Build(c []Candidate) Picker { return inOrderPicker(len(c)) }

type inOrderPicker int

func (n inOrderPicker) Pick(_ uint64, attempt int) int { return attempt % int(n) }

// rebuildCounter counts full and incremental builds.
type rebuildCounter struct{ builds, rebuilds atomic.Int32 }

func (b *rebuildCounter) Build(c []Candidate) Picker {
	b.builds.Add(1)
	return RoundRobin().Build(c)
}

func (b *rebuildCounter) Rebuild(_ Picker, c []Candidate) Picker {
	b.rebuilds.Add(1)
	return RoundRobin().Build(c)
}

// TestSetConfigSameBalancerRebuilds checks that SetConfig with the
// current Balancer keeps its Picker, which later changes rebuild
// incrementally, and a new one builds.
func TestSetConfigSameBalancerRebuilds(t *testing.T) {
	bal := &rebuildCounter{}
	p := newTestPool(t, Config{Balancer: bal}, "a")
	if err := p.SetConfig(Config{Balancer: bal}); err != nil {
		t.Fatal(err)
	}
	_ = p.Put(Target[string]{ID: "b", Value: "b"})
	if b, r := bal.builds.Load(), bal.rebuilds.Load(); b != 1 || r != 1 {
		t.Fatalf("same balancer: %d builds, %d rebuilds", b, r)
	}
	if err := p.SetConfig(Config{Balancer: &rebuildCounter{}}); err != nil {
		t.Fatal(err)
	}
	if err := p.SetConfig(Config{Balancer: bal}); err != nil {
		t.Fatal(err)
	}
	if b := bal.builds.Load(); b != 2 {
		t.Fatalf("balancer set again: %d builds, want 2", b)
	}
}
