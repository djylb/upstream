package upstream

import (
	"errors"
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

// tiers returns targets for ids with the given priorities.
func tiers(ids []string, prio ...int) []Target[string] {
	ts := targets(ids...)
	for i := range ts {
		ts[i].Priority = prio[i]
	}
	return ts
}

func TestTiersPreferLowestPriority(t *testing.T) {
	for name, bal := range testBalancers() {
		t.Run(name, func(t *testing.T) {
			p := New[string](Config{Balancer: bal})
			defer p.Close()
			// Membership order does not decide the tier.
			if err := p.Sync(tiers([]string{"b1", "a1", "b2", "a2"}, 5, -1, 5, -1)); err != nil {
				t.Fatal(err)
			}
			got := pickIDs(t, p, 200)
			if got["b1"]+got["b2"] != 0 || got["a1"] == 0 || got["a2"] == 0 {
				t.Fatalf("picks %v, want both of tier -1 and none of tier 5", got)
			}
			if n := p.Available(); n != 4 {
				t.Fatalf("Available = %d, want 4", n)
			}

			// A tier with one eligible member keeps all traffic.
			a1, _ := p.Backend("a1")
			a2, _ := p.Backend("a2")
			p.ReportCheck(a1, HealthDown, CheckResult{})
			if got := pickIDs(t, p, 50); got["a2"] != 50 {
				t.Fatalf("picks %v, want all on a2", got)
			}
			// The whole tier down: the next tier takes over, balanced.
			p.ReportCheck(a2, HealthDown, CheckResult{})
			got = pickIDs(t, p, 200)
			if got["b1"] == 0 || got["b2"] == 0 || got["b1"]+got["b2"] != 200 {
				t.Fatalf("picks %v, want both of tier 5", got)
			}
			// And traffic returns when the tier recovers.
			p.ReportCheck(a1, HealthUp, CheckResult{})
			if got := pickIDs(t, p, 50); got["a1"] != 50 {
				t.Fatalf("picks %v, want all on a1", got)
			}
		})
	}
}

// TestTiersKeyAffinity checks that consistent hashing maps keys within the
// preferred tier and back to the same members after a failover.
func TestTiersKeyAffinity(t *testing.T) {
	p := New[string](Config{Balancer: ConsistentHash(HashConfig{})})
	defer p.Close()
	if err := p.Sync(tiers([]string{"a", "b", "c", "x", "y"}, 0, 0, 0, 1, 1)); err != nil {
		t.Fatal(err)
	}
	owner := func(key string) string {
		l, err := p.PickKey(key)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Release(Success)
		return l.Backend().ID()
	}
	before := make(map[string]string)
	for i := range 100 {
		k := strconv.Itoa(i)
		before[k] = owner(k)
		if before[k] == "x" || before[k] == "y" {
			t.Fatalf("key %s went to the backup tier", k)
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		b, _ := p.Backend(id)
		p.ReportCheck(b, HealthDown, CheckResult{})
	}
	for k := range before {
		if o := owner(k); o != "x" && o != "y" {
			t.Fatalf("key %s went to %s with its tier down", k, o)
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		b, _ := p.Backend(id)
		p.ReportCheck(b, HealthUp, CheckResult{})
	}
	for k, o := range before {
		if got := owner(k); got != o {
			t.Fatalf("key %s moved from %s to %s", k, o, got)
		}
	}
}

// TestTiersHalfOpenPassesOn checks that a tier whose members refuse for
// health, here a half-open member out of trial slots, passes the pick on.
func TestTiersHalfOpenPassesOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 1}})
		defer p.Close()
		if err := p.Sync(ordered("a", "b")); err != nil {
			t.Fatal(err)
		}
		ejectToHalfOpen(t, p, clk, "a")
		trial := mustPick(t, p)
		if trial.Backend().ID() != "a" {
			t.Fatalf("trial went to %s", trial.Backend().ID())
		}
		if l := mustPick(t, p); l.Backend().ID() != "b" {
			t.Fatalf("pick during the trial went to %s", l.Backend().ID())
		} else {
			l.Release(Success)
		}
		trial.Release(Success)
		if got := pickIDs(t, p, 10); got["a"] != 10 {
			t.Fatalf("picks %v after recovery", got)
		}
	})
}

// TestTiersRetry checks that retries try the preferred tier first, then
// the next, and never a backend twice.
func TestTiersRetry(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 10}})
	defer p.Close()
	if err := p.Sync(tiers([]string{"x", "a", "b", "y"}, 1, 0, 0, 2)); err != nil {
		t.Fatal(err)
	}
	r := p.Request("")
	var got []string
	for {
		l, err := r.Next()
		if err != nil {
			if !errors.Is(err, ErrUnavailable) {
				t.Fatal(err)
			}
			break
		}
		got = append(got, l.Backend().ID())
		l.Release(Failure)
	}
	if len(got) != 4 || got[2] != "x" || got[3] != "y" || !slices.Contains(got[:2], "a") || !slices.Contains(got[:2], "b") {
		t.Fatalf("retries went to %v, want a and b, then x, then y", got)
	}
}

func TestMaxInFlight(t *testing.T) {
	p := New[string](Config{})
	defer p.Close()
	if err := p.Sync([]Target[string]{{ID: "a", MaxInFlight: 1}, {ID: "b", MaxInFlight: 2}}); err != nil {
		t.Fatal(err)
	}
	var held []Lease[string]
	for range 3 {
		held = append(held, mustPick(t, p))
	}
	if _, err := p.Pick(); !errors.Is(err, ErrSaturated) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pick beyond every cap = %v, want ErrSaturated, an ErrUnavailable", err)
	}
	a, _ := p.Backend("a")
	b, _ := p.Backend("b")
	if a.InFlight() != 1 || b.InFlight() != 2 {
		t.Fatalf("in flight a=%d b=%d", a.InFlight(), b.InFlight())
	}
	if p.Available() != 2 {
		t.Fatalf("Available = %d: full members are still eligible", p.Available())
	}
	held[0].Release(Success)
	l := mustPick(t, p)
	l.Release(Success)
	for i := range held[1:] {
		held[1+i].Release(Success)
	}
	if err := p.Put(Target[string]{ID: "a", MaxInFlight: -1}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("negative MaxInFlight: %v", err)
	}
	// A new cap is a new version of the member.
	if err := p.Put(Target[string]{ID: "a", MaxInFlight: 3}); err != nil {
		t.Fatal(err)
	}
	if a2, _ := p.Backend("a"); a2 == a || a2.MaxInFlight() != 3 || a2.Status().MaxInFlight != 3 {
		t.Fatal("cap change not applied")
	}
}

// TestMaxInFlightRace checks that concurrent picks never exceed a cap.
func TestMaxInFlightRace(t *testing.T) {
	p := New[string](Config{Balancer: LeastConn()})
	defer p.Close()
	if err := p.Sync([]Target[string]{{ID: "a", MaxInFlight: 3}, {ID: "b", MaxInFlight: 2}}); err != nil {
		t.Fatal(err)
	}
	a, _ := p.Backend("a")
	b, _ := p.Backend("b")
	// Leases held, counted by the test: the in-flight count itself may
	// briefly exceed a cap while a racing pick backs out.
	held := map[*Backend[string]]*atomic.Int64{a: new(atomic.Int64), b: new(atomic.Int64)}
	limit := map[*Backend[string]]int64{a: 3, b: 2}
	var wg sync.WaitGroup
	var over atomic.Bool
	for range 8 {
		wg.Go(func() {
			for i := range 2000 {
				l, err := p.Pick()
				if err != nil {
					continue
				}
				h := held[l.Backend()]
				if h.Add(1) > limit[l.Backend()] {
					over.Store(true)
				}
				if i%16 == 0 {
					runtime.Gosched()
				}
				h.Add(-1)
				l.Release(Success)
			}
		})
	}
	wg.Wait()
	if over.Load() {
		t.Fatal("a cap was exceeded")
	}
	if a.InFlight() != 0 || b.InFlight() != 0 {
		t.Fatalf("leaked slots: a=%d b=%d", a.InFlight(), b.InFlight())
	}
}

// TestTierOverflow checks that a full tier holds picks back unless
// Overflow lets them go on to the next tier, and that panic mode and the
// fallback respect caps too.
func TestTierOverflow(t *testing.T) {
	p := New[string](Config{Fallback: FallbackAll})
	defer p.Close()
	if err := p.Sync([]Target[string]{{ID: "a", MaxInFlight: 1}, {ID: "b", Priority: 1}}); err != nil {
		t.Fatal(err)
	}
	held := mustPick(t, p)
	if held.Backend().ID() != "a" {
		t.Fatalf("first pick went to %s", held.Backend().ID())
	}
	if _, err := p.Pick(); !errors.Is(err, ErrSaturated) {
		t.Fatalf("pick with a full tier and no overflow = %v", err)
	}
	if err := p.SetConfig(Config{Fallback: FallbackAll, Overflow: true}); err != nil {
		t.Fatal(err)
	}
	l := mustPick(t, p)
	if l.Backend().ID() != "b" {
		t.Fatalf("overflow went to %s", l.Backend().ID())
	}
	l.Release(Success)
	held.Release(Success)
	if l := mustPick(t, p); l.Backend().ID() != "a" {
		t.Fatalf("pick after release went to %s", l.Backend().ID())
	} else {
		held = l
	}

	// Panic mode: every member down, a full, b takes the overflow.
	for _, id := range []string{"a", "b"} {
		bk, _ := p.Backend(id)
		p.ReportCheck(bk, HealthDown, CheckResult{})
	}
	if l := mustPick(t, p); l.Backend().ID() != "b" {
		t.Fatalf("panic pick went to %s", l.Backend().ID())
	} else {
		l.Release(Success)
	}
	if err := p.SetConfig(Config{Fallback: FallbackAll}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Pick(); !errors.Is(err, ErrSaturated) {
		t.Fatalf("panic pick with a full tier and no overflow = %v", err)
	}
	held.Release(Success)

	// With overflow, a pick that found a tier full and the next one
	// unavailable is saturated; one that found none full is not.
	q := New[string](Config{Overflow: true})
	defer q.Close()
	if err := q.Sync([]Target[string]{{ID: "a", MaxInFlight: 1}, {ID: "b", Priority: 1}}); err != nil {
		t.Fatal(err)
	}
	b, _ := q.Backend("b")
	q.MarkOffline(b, nil)
	l = mustPick(t, q)
	if _, err := q.Pick(); !errors.Is(err, ErrSaturated) {
		t.Fatalf("pick with a full tier and an offline one = %v", err)
	}
	a, _ := q.Backend("a")
	q.ReportCheck(a, HealthDown, CheckResult{})
	if _, err := q.Pick(); !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrSaturated) {
		t.Fatalf("pick with no member up = %v, want ErrUnavailable only", err)
	}
	l.Release(Success)
}

// buildRecorder records the candidates of every Build.
type buildRecorder struct {
	mu     sync.Mutex
	builds [][]Candidate
}

func (r *buildRecorder) Build(c []Candidate) Picker {
	r.mu.Lock()
	r.builds = append(r.builds, slices.Clone(c))
	r.mu.Unlock()
	return RoundRobin().Build(c)
}

func (r *buildRecorder) take() [][]Candidate {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.builds
	r.builds = nil
	return b
}

// TestTierPickerReuse checks that a change in one tier builds only that
// tier's Picker.
func TestTierPickerReuse(t *testing.T) {
	rec := &buildRecorder{}
	p := New[string](Config{Balancer: rec})
	defer p.Close()
	if err := p.Sync(tiers([]string{"a", "b", "x", "y"}, 0, 0, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.take()); n != 2 {
		t.Fatalf("%d builds for two tiers", n)
	}
	y, _ := p.Backend("y")
	p.ReportCheck(y, HealthDown, CheckResult{})
	builds := rec.take()
	if len(builds) != 1 || len(builds[0]) != 1 || builds[0][0].ID != "x" {
		t.Fatalf("builds after a backup went down: %v", builds)
	}
	// A tier that empties drops its Picker; one that comes back builds.
	x, _ := p.Backend("x")
	p.ReportCheck(x, HealthDown, CheckResult{})
	p.ReportCheck(x, HealthUp, CheckResult{})
	if builds := rec.take(); len(builds) != 1 || builds[0][0].ID != "x" {
		t.Fatalf("builds after the backup tier came back: %v", builds)
	}
	// Moving a member between tiers builds both.
	if err := p.Put(Target[string]{ID: "b", Value: "addr-b", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.take()); n != 2 {
		t.Fatalf("%d builds after a member moved tiers", n)
	}
	if got := pickIDs(t, p, 10); got["a"] != 10 {
		t.Fatalf("picks %v", got)
	}
}

// TestTierSlowStart checks that slow start ramps a member within its tier.
func TestTierSlowStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		rec := &buildRecorder{}
		p := New[string](Config{Clock: clk, Balancer: rec, SlowStart: time.Minute})
		defer p.Close()
		if err := p.Sync(tiers([]string{"a", "b", "x"}, 0, 0, 1)); err != nil {
			t.Fatal(err)
		}
		clk.Advance(2 * time.Minute)
		synctest.Wait()
		b, _ := p.Backend("b")
		p.ReportCheck(b, HealthDown, CheckResult{})
		rec.take()
		p.ReportCheck(b, HealthUp, CheckResult{})
		builds := rec.take()
		if len(builds) != 1 || len(builds[0]) != 2 {
			t.Fatalf("builds after b came back: %v", builds)
		}
		var wa, wb int
		for _, c := range builds[0] {
			switch c.ID {
			case "a":
				wa = c.Weight
			case "b":
				wb = c.Weight
			}
		}
		if wb >= wa {
			t.Fatalf("b is not ramping: weights a=%d b=%d", wa, wb)
		}
		clk.Advance(2 * time.Minute)
		synctest.Wait()
		p.Close()
		if n := clk.Pending(); n != 0 {
			t.Fatalf("%d timers left", n)
		}
	})
}

func TestMarkOffline(t *testing.T) {
	p := New[string](Config{})
	defer p.Close()
	if err := p.Sync(ordered("a", "b")); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var evs []Event[string]
	defer p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventStateChanged {
			mu.Lock()
			evs = append(evs, ev)
			mu.Unlock()
		}
	})()
	takeEvents := func() []Event[string] {
		mu.Lock()
		defer mu.Unlock()
		e := evs
		evs = nil
		return e
	}
	a, _ := p.Backend("a")
	dropped := errors.New("session dropped")
	if !p.MarkOffline(a, dropped) {
		t.Fatal("MarkOffline of a member reported false")
	}
	if a.State() != StateOffline || !a.Status().Offline {
		t.Fatalf("a is %v", a.State())
	}
	if e := takeEvents(); len(e) != 1 || e[0].To != StateOffline || e[0].From != StateHealthy || !errors.Is(e[0].Err, dropped) {
		t.Fatalf("events %v", e)
	}
	if got := pickIDs(t, p, 10); got["b"] != 10 {
		t.Fatalf("picks %v with a offline", got)
	}
	if p.Available() != 1 {
		t.Fatalf("Available = %d", p.Available())
	}
	// Repeating the mark changes nothing.
	p.MarkOffline(a, nil)
	if e := takeEvents(); len(e) != 0 {
		t.Fatalf("events %v", e)
	}

	// Offline takes precedence over unhealthy, and lifting it leaves the
	// active verdict in force.
	p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("refused")})
	if a.State() != StateOffline {
		t.Fatalf("a is %v", a.State())
	}
	p.MarkOnline(a)
	if a.State() != StateUnhealthy || a.Status().Offline {
		t.Fatalf("a is %v", a.State())
	}
	if got := pickIDs(t, p, 10); got["b"] != 10 {
		t.Fatalf("picks %v with a down", got)
	}
	p.ReportCheck(a, HealthUp, CheckResult{})
	if got := pickIDs(t, p, 10); got["a"] != 10 {
		t.Fatalf("picks %v with a back", got)
	}
	e := takeEvents()
	want := []State{StateUnhealthy, StateHealthy}
	if len(e) != len(want) {
		t.Fatalf("events %v", e)
	}
	for i, ev := range e {
		if ev.To != want[i] {
			t.Fatalf("event %d to %v, want %v", i, ev.To, want[i])
		}
	}

	// Draining takes precedence over offline; a nil cause reports
	// ErrOffline.
	p.MarkOffline(a, nil)
	if e := takeEvents(); len(e) != 1 || !errors.Is(e[0].Err, ErrOffline) {
		t.Fatalf("events %v", e)
	}
	p.Drain("a")
	if a.State() != StateDraining {
		t.Fatalf("a is %v", a.State())
	}
	p.Resume("a")
	if a.State() != StateOffline {
		t.Fatalf("a is %v", a.State())
	}

	// The mark follows the ID across updates.
	if err := p.Put(Target[string]{ID: "a", Value: "new"}); err != nil {
		t.Fatal(err)
	}
	a2, _ := p.Backend("a")
	if a2 == a || a2.State() != StateOffline {
		t.Fatalf("updated a is %v", a2.State())
	}
	// The old version still names the same member.
	if !p.MarkOnline(a) || a2.State() != StateHealthy {
		t.Fatal("MarkOnline through an older version of the member failed")
	}

	// A removed member, or one added again, ignores old handles.
	p.Remove("a")
	if p.MarkOffline(a2, nil) {
		t.Fatal("MarkOffline of a removed member reported true")
	}
	if err := p.Put(Target[string]{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	a3, _ := p.Backend("a")
	if p.MarkOffline(a2, nil) || a3.State() != StateHealthy {
		t.Fatalf("an old generation's mark reached the new member: %v", a3.State())
	}
	p.Close()
	if p.MarkOffline(a3, nil) {
		t.Fatal("MarkOffline after Close reported true")
	}
}

// TestOfflineInPanicMode checks that panic mode ignores health verdicts
// but never picks an offline member, which does not count towards the
// panic threshold either.
func TestOfflineInPanicMode(t *testing.T) {
	p := New[string](Config{Fallback: FallbackAll, PanicThreshold: 50})
	defer p.Close()
	if err := p.Sync(targets("a", "b", "c", "d")); err != nil {
		t.Fatal(err)
	}
	set := func(id string, f func(*Backend[string])) {
		b, _ := p.Backend(id)
		f(b)
	}
	off := func(b *Backend[string]) { p.MarkOffline(b, nil) }
	down := func(b *Backend[string]) { p.ReportCheck(b, HealthDown, CheckResult{}) }
	set("a", off)
	set("b", off)
	set("c", down)
	// One of the two members that are not offline is eligible: no panic.
	if got := pickIDs(t, p, 10); got["d"] != 10 || p.Available() != 1 {
		t.Fatalf("picks %v, Available %d", got, p.Available())
	}
	set("d", down)
	// Panic: c and d, never a or b.
	got := pickIDs(t, p, 100)
	if got["a"]+got["b"] != 0 || got["c"] == 0 || got["d"] == 0 {
		t.Fatalf("panic picks %v", got)
	}
	set("c", off)
	set("d", off)
	if _, err := p.Pick(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pick with every member offline = %v", err)
	}
}

// TestOfflineSlowStart checks that a member that comes back online ramps
// up like one that recovers.
func TestOfflineSlowStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		rec := &buildRecorder{}
		p := New[string](Config{Clock: clk, Balancer: rec, SlowStart: time.Minute})
		defer p.Close()
		if err := p.Sync(targets("a", "b")); err != nil {
			t.Fatal(err)
		}
		clk.Advance(2 * time.Minute)
		synctest.Wait()
		b, _ := p.Backend("b")
		p.MarkOffline(b, nil)
		rec.take()
		p.MarkOnline(b)
		builds := rec.take()
		if len(builds) != 1 || len(builds[0]) != 2 || builds[0][1].Weight >= builds[0][0].Weight {
			t.Fatalf("builds after b came online: %v", builds)
		}
		p.Close()
	})
}
