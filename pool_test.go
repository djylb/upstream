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
	"time"
)

func targets(ids ...string) []Target[string] {
	ts := make([]Target[string], len(ids))
	for i, id := range ids {
		ts[i] = Target[string]{ID: id, Value: "addr-" + id}
	}
	return ts
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
		"removed a", "updated b", "added c",
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

func TestPickDoesNotAllocate(t *testing.T) {
	for name, b := range testBalancers() {
		p := New[string](Config{Balancer: b, Outlier: OutlierConfig{ConsecutiveFailures: 1000}})
		ts := make([]Target[string], 16)
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
			t.Errorf("%s: %.1f allocations per pick", name, allocs)
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
		"Failover":           Failover(),
		"ConsistentHash":     ConsistentHash(HashConfig{}),
		"BoundedHash":        ConsistentHash(HashConfig{LoadFactor: 1.25}),
	}
}

// TestConcurrentUpdatesWhilePicking hammers picks and releases while the
// membership, weights, drains and health change underneath, then checks
// that every slot came back.
func TestConcurrentUpdatesWhilePicking(t *testing.T) {
	for name, bal := range testBalancers() {
		t.Run(name, func(t *testing.T) {
			p := New[string](Config{Balancer: bal, Fallback: FallbackAll, Outlier: OutlierConfig{ConsecutiveFailures: 3, BaseEjection: time.Millisecond}})
			defer p.Close()
			ids := []string{"a", "b", "c", "d", "e"}
			if err := p.Sync(targets(ids...)); err != nil {
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
					switch i % 6 {
					case 0:
						_ = p.Sync(targets(ids[:1+i%len(ids)]...))
					case 1:
						_ = p.Put(Target[string]{ID: id, Weight: i%5 + 1})
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
