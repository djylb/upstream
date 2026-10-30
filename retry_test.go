package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

func TestAttemptsAreDistinct(t *testing.T) {
	for name, bal := range testBalancers() {
		t.Run(name, func(t *testing.T) {
			p := newTestPool(t, Config{Balancer: bal, Retry: RetryConfig{Attempts: 10}}, "a", "b", "c", "d")
			a := p.Request("client")
			var seen []string
			for {
				l, err := a.Next()
				if err != nil {
					if !errors.Is(err, ErrUnavailable) {
						t.Fatalf("after %v: %v, want ErrUnavailable", seen, err)
					}
					break
				}
				seen = append(seen, l.Backend().ID())
				l.Release(Failure)
			}
			slices.Sort(seen)
			if !slices.Equal(seen, []string{"a", "b", "c", "d"}) || a.Tried() != 4 {
				t.Fatalf("attempts visited %v", seen)
			}
		})
	}
}

func TestAttemptsLimit(t *testing.T) {
	p := newTestPool(t, Config{Retry: RetryConfig{Attempts: 2}}, "a", "b", "c")
	a := p.Request("")
	for range 2 {
		l, err := a.Next()
		if err != nil {
			t.Fatal(err)
		}
		l.Release(Failure)
	}
	if _, err := a.Next(); !errors.Is(err, ErrRetryLimit) {
		t.Fatalf("third attempt: %v, want ErrRetryLimit", err)
	}
	// Without a RetryConfig there are no retries.
	q := newTestPool(t, Config{}, "a", "b")
	b := q.Request("")
	l, _ := b.Next()
	l.Release(Failure)
	if _, err := b.Next(); !errors.Is(err, ErrRetryLimit) {
		t.Fatalf("retry without RetryConfig: %v", err)
	}
}

func TestAttemptsManyTries(t *testing.T) {
	ids := make([]string, 9)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	p := newTestPool(t, Config{Balancer: Failover(), Retry: RetryConfig{Attempts: 100}}, ids...)
	a := p.Request("")
	for i := range ids {
		l, err := a.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := l.Backend().ID(); got != ids[i] {
			t.Fatalf("attempt %d picked %s, want %s", i, got, ids[i])
		}
		l.Release(Failure)
	}
}

func TestRetryBudget(t *testing.T) {
	clk := fakeclock.New()
	p := newTestPool(t, Config{Clock: clk, Retry: RetryConfig{Attempts: 3, Budget: 0.5, MinRetries: -1}}, "a", "b", "c")
	retries := 0
	for range 10 {
		a := p.Request("")
		l, err := a.Next()
		if err != nil {
			t.Fatal(err)
		}
		l.Release(Failure)
		for {
			l, err = a.Next()
			if err != nil {
				break
			}
			retries++
			l.Release(Failure)
		}
		if !errors.Is(err, ErrRetryBudget) && !errors.Is(err, ErrRetryLimit) {
			t.Fatal(err)
		}
	}
	if retries != 5 {
		t.Fatalf("%d retries for 10 requests with a budget of 0.5", retries)
	}
	// The budget refills as the window slides past.
	clk.Advance(time.Minute)
	l, _ := p.Pick()
	l.Release(Success)
	_ = p.Do(context.Background(), func(*Backend[string]) error { return nil }) // a request
	a := p.Request("")
	l, _ = a.Next()
	l.Release(Failure)
	if l, err := a.Next(); err != nil {
		t.Fatalf("retry after the window: %v", err)
	} else {
		l.Release(Success)
	}
}

func TestRetryBudgetMinimum(t *testing.T) {
	p := newTestPool(t, Config{Clock: fakeclock.New(), Retry: RetryConfig{Attempts: 2, Budget: 0.01}}, "a", "b")
	n := 0
	for range 20 {
		a := p.Request("")
		l, _ := a.Next()
		l.Release(Failure)
		if l, err := a.Next(); err == nil {
			n++
			l.Release(Success)
		}
	}
	if n != 10 {
		t.Fatalf("%d retries, want the default minimum of 10", n)
	}
}

func TestDoRetries(t *testing.T) {
	p := newTestPool(t, Config{Balancer: Failover(), Retry: RetryConfig{Attempts: 3}}, "a", "b", "c")
	var tried []string
	err := p.Do(context.Background(), func(b *Backend[string]) error {
		tried = append(tried, b.ID())
		if b.ID() != "c" {
			return errors.New("refused")
		}
		return nil
	})
	if err != nil || !slices.Equal(tried, []string{"a", "b", "c"}) {
		t.Fatalf("Do = %v after %v", err, tried)
	}

	// The last error comes back when every attempt fails.
	tried = nil
	last := errors.New("last")
	err = p.Do(context.Background(), func(b *Backend[string]) error {
		tried = append(tried, b.ID())
		if len(tried) == 3 {
			return last
		}
		return errors.New("refused")
	})
	if !errors.Is(err, last) || len(tried) != 3 {
		t.Fatalf("Do = %v after %v", err, tried)
	}

	// Neutral and successful outcomes are not retried.
	tried = nil
	bad := WithOutcome(errors.New("bad request"), Neutral)
	err = p.Do(context.Background(), func(b *Backend[string]) error {
		tried = append(tried, b.ID())
		return bad
	})
	if !errors.Is(err, bad) || len(tried) != 1 {
		t.Fatalf("Do = %v after %v", err, tried)
	}
	if s := p.Status()[0]; s.Neutrals != 1 {
		t.Fatalf("status %+v", s)
	}

	// A done context stops retries.
	ctx, cancel := context.WithCancel(context.Background())
	tried = nil
	refused := errors.New("refused")
	err = p.Do(ctx, func(b *Backend[string]) error {
		tried = append(tried, b.ID())
		cancel()
		return refused
	})
	if !errors.Is(err, refused) || len(tried) != 1 {
		t.Fatalf("Do = %v after %v", err, tried)
	}
	if err := p.Do(ctx, func(*Backend[string]) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do with a done context = %v", err)
	}
}

func TestRequestDoesNotAllocate(t *testing.T) {
	p := newTestPool(t, Config{Balancer: ConsistentHash(HashConfig{}), Retry: RetryConfig{Attempts: 4, Budget: 100}}, "a", "b", "c", "d", "e")
	allocs := testing.AllocsPerRun(1000, func() {
		a := p.Request("client")
		for range 4 {
			l, err := a.Next()
			if err != nil {
				panic(err)
			}
			l.Release(Failure)
		}
		_ = p.DoKey(context.Background(), "client", func(*Backend[string]) error { return nil })
	})
	if allocs != 0 {
		t.Fatalf("%.1f allocations", allocs)
	}
}

// TestReportEndsTrial checks that a lease that reports its outcome early,
// as a long-lived connection does, ends a half-open trial at once instead
// of holding the trial slot until it is released.
func TestReportEndsTrial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Balancer: Failover(), Outlier: OutlierConfig{ConsecutiveFailures: 1}})
		defer p.Close()
		if err := p.Sync(targets("a", "b")); err != nil {
			t.Fatal(err)
		}
		ejectToHalfOpen(t, p, clk, "a")
		trial := mustPick(t, p)
		if trial.Backend().ID() != "a" {
			t.Fatalf("trial went to %s", trial.Backend().ID())
		}
		trial.Report(Success)
		trial.Report(Failure) // ignored
		synctest.Wait()
		a, _ := p.Backend("a")
		if a.State() != StateHealthy {
			t.Fatalf("a is %v after a reported successful trial", a.State())
		}
		if s := a.Status(); s.InFlight != 1 || s.Successes != 1 || s.Failures != 1 {
			t.Fatalf("status %+v", s)
		}
		trial.Release(Failure) // the outcome was reported
		if s := a.Status(); s.InFlight != 0 || s.Failures != 1 || a.State() != StateHealthy {
			t.Fatalf("status after release %+v", s)
		}
	})
}

// TestEmptyKeyHasNoAffinity checks that requests without a key spread
// over the ring instead of all mapping to the hash of the empty string.
func TestEmptyKeyHasNoAffinity(t *testing.T) {
	p := newTestPool(t, Config{Balancer: ConsistentHash(HashConfig{})}, "a", "b", "c", "d")
	seen := map[string]bool{}
	for range 200 {
		l, err := p.PickKey("")
		if err != nil {
			t.Fatal(err)
		}
		seen[l.Backend().ID()] = true
		l.Release(Success)
	}
	if len(seen) < 3 {
		t.Fatalf("empty keys went to %v", seen)
	}
}

func TestAvailable(t *testing.T) {
	p := newTestPool(t, Config{Fallback: FallbackAll}, "a", "b")
	if n := p.Available(); n != 2 {
		t.Fatalf("Available = %d", n)
	}
	a, _ := p.Backend("a")
	b, _ := p.Backend("b")
	p.ReportCheck(a, HealthDown, CheckResult{})
	if n := p.Available(); n != 1 {
		t.Fatalf("Available = %d with one down", n)
	}
	p.ReportCheck(b, HealthDown, CheckResult{})
	if n := p.Available(); n != 0 {
		t.Fatalf("Available = %d in panic mode", n)
	}
	p.Close()
	if n := p.Available(); n != 0 {
		t.Fatalf("Available = %d after Close", n)
	}
}

func TestDrainWait(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	l := mustPick(t, p)
	d, _ := p.Drain("a")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v", err)
	}
	l.Release(Success)
	if err := d.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Resume("a")
	l = mustPick(t, p)
	d, _ = p.Drain("a")
	p.Resume("a")
	if err := d.Wait(context.Background()); !errors.Is(err, ErrDrainCanceled) {
		t.Fatalf("Wait after Resume = %v", err)
	}
	l.Release(Success)
}

func TestNamesRoundTrip(t *testing.T) {
	for _, name := range []string{"round-robin", "weighted-round-robin", "random", "power-of-two", "least-conn", "failover", "consistent-hash"} {
		b, err := ParseBalancer(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(b); got != name {
			t.Errorf("ParseBalancer(%q) prints as %q", name, got)
		}
	}
	if _, err := ParseBalancer("fastest"); err == nil {
		t.Error("parsed an unknown balancer")
	}
	for _, f := range []Fallback{FallbackNone, FallbackAll} {
		if got, err := ParseFallback(f.String()); err != nil || got != f {
			t.Errorf("ParseFallback(%q) = %v, %v", f, got, err)
		}
	}
	if _, err := ParseFallback("some"); err == nil {
		t.Error("parsed an unknown fallback")
	}
	out, err := json.Marshal(struct {
		S State
		H Health
		O OutlierState
		R Outcome
		K EventKind
		F Fallback
	}{StateHalfOpen, HealthDown, OutlierEjected, Neutral, EventRemoved, FallbackAll})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"S":"half-open","H":"down","O":"ejected","R":"neutral","K":"removed","F":"all"}`; string(out) != want {
		t.Fatalf("JSON %s, want %s", out, want)
	}
	var back struct {
		S State
		H Health
		O OutlierState
		R Outcome
		K EventKind
		F Fallback
	}
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if back.S != StateHalfOpen || back.H != HealthDown || back.O != OutlierEjected || back.R != Neutral || back.K != EventRemoved || back.F != FallbackAll {
		t.Fatalf("decoded %+v", back)
	}
	if err := json.Unmarshal([]byte(`{"S":"sleeping"}`), &back); err == nil {
		t.Fatal("decoded an unknown state")
	}
}

func TestWithOutcome(t *testing.T) {
	base := errors.New("x")
	for _, o := range []Outcome{Success, Failure, Neutral} {
		err := WithOutcome(fmt.Errorf("wrapped: %w", base), o)
		if OutcomeOf(fmt.Errorf("again: %w", err)) != o || !errors.Is(err, base) || err.Error() != "wrapped: x" {
			t.Errorf("WithOutcome(%v) = %v", o, err)
		}
	}
	if WithOutcome(nil, Failure) != nil {
		t.Error("WithOutcome(nil) is not nil")
	}
}

// FuzzRequest checks that retries visit every member exactly once, for
// every balancer, before reporting that none is left.
func FuzzRequest(f *testing.F) {
	f.Add([]byte{1, 2, 3}, "k", uint8(0))
	f.Add([]byte{0, 255, 9, 9, 1, 1}, "", uint8(7))
	balancers := []Balancer{RoundRobin(), WeightedRoundRobin(), Random(), PowerOfTwo(), LeastConn(), Failover(),
		ConsistentHash(HashConfig{VirtualNodes: 8}), ConsistentHash(HashConfig{VirtualNodes: 3, LoadFactor: 1.1})}
	f.Fuzz(func(t *testing.T, weights []byte, key string, which uint8) {
		if len(weights) == 0 || len(weights) > 300 {
			return
		}
		p := New[string](Config{Balancer: balancers[int(which)%len(balancers)], Retry: RetryConfig{Attempts: len(weights) + 1}})
		defer p.Close()
		ts := make([]Target[string], len(weights))
		for i, w := range weights {
			ts[i] = Target[string]{ID: strconv.Itoa(i), Weight: int(w)}
		}
		if err := p.Sync(ts); err != nil {
			t.Fatal(err)
		}
		a := p.Request(key)
		seen := make(map[string]bool, len(weights))
		var held []Lease[string]
		for {
			l, err := a.Next()
			if err != nil {
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("after %d: %v", len(seen), err)
				}
				break
			}
			id := l.Backend().ID()
			if seen[id] {
				t.Fatalf("%s returned twice", id)
			}
			seen[id] = true
			held = append(held, l) // keep loads changing
		}
		if len(seen) != len(weights) {
			t.Fatalf("visited %d of %d", len(seen), len(weights))
		}
		for i := range held {
			held[i].Release(Failure)
		}
	})
}

func TestBudgetOvershoot(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 2, Budget: 1e-9, MinRetries: 1}})
	defer p.Close()
	var ts []Target[string]
	for i := range 4 {
		ts = append(ts, Target[string]{ID: string('a' + rune(i))})
	}
	_ = p.Sync(ts)
	const n = 64
	var granted atomic.Int32
	var start, wg sync.WaitGroup
	start.Add(1)
	for range n {
		wg.Go(func() {
			a := p.Request("")
			l, err := a.Next()
			if err != nil {
				return
			}
			l.Release(Failure)
			start.Wait()
			if l2, err := a.Next(); err == nil {
				granted.Add(1)
				l2.Release(Success)
			}
		})
	}
	start.Done()
	wg.Wait()
	if g := granted.Load(); g > 1 {
		t.Fatalf("budget of 1 retry granted %d", g)
	}
}

func TestDoWithoutKeyDoesNotAllocate(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 3, Budget: 0.5}})
	defer p.Close()
	_ = p.Sync([]Target[string]{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	n := testing.AllocsPerRun(1000, func() {
		a := p.Request("k")
		l, _ := a.Next()
		l.Release(Failure)
		l, _ = a.Next()
		l.Release(Success)
	})
	fn := func(*Backend[string]) error { return nil }
	m := testing.AllocsPerRun(1000, func() { _ = p.Do(context.Background(), fn) })
	t.Logf("attempts %v, do %v", n, m)
	if n != 0 || m != 0 {
		t.Fatalf("allocs attempts %v do %v", n, m)
	}
}

// TestDoNeverReturnsNilOnFailure checks that Do reports an error when fn
// asks for a retry without an error and none can be made.
func TestDoNeverReturnsNilOnFailure(t *testing.T) {
	p := newTestPool(t, Config{Retry: RetryConfig{Attempts: 1}}, "a", "b")
	r0 := p.Request("")
	err := r0.Do(context.Background(), func(*Lease[string]) (bool, error) { return true, nil })
	if !errors.Is(err, ErrRetryLimit) {
		t.Fatalf("Do = %v, want ErrRetryLimit", err)
	}
	r := p.Request("")
	l, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	l.Release(Success)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Do(ctx, func(*Lease[string]) (bool, error) { return false, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do after Next with a done ctx = %v", err)
	}
}
