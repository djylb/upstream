package upstream

import (
	"errors"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"testing"
)

// TestRendezvousRankMatchesPick checks that Rank orders the candidates as
// Pick returns them attempt by attempt.
func TestRendezvousRankMatchesPick(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, weighted := range []bool{false, true} {
		for _, n := range []int{1, 2, 7, 40} {
			ws := make([]int, n)
			for i := range ws {
				ws[i] = 1
				if weighted {
					ws[i] = 1 + rng.IntN(5)
				}
			}
			p := Rendezvous(RendezvousConfig{Key: [16]byte{3}}).Build(candidates(ws...))
			rk, ok := p.(Ranker)
			if !ok {
				t.Fatal("the Rendezvous Picker is not a Ranker")
			}
			got := make([]int, n)
			for range 20 {
				key := rng.Uint64()
				rk.Rank(key, got)
				for a := range n {
					if want := p.Pick(key, a); got[a] != want {
						t.Fatalf("n=%d weighted=%v key %x: rank %d is %d, Pick says %d", n, weighted, key, a, got[a], want)
					}
				}
			}
		}
	}
}

// rankBalancer builds Rankers that count their calls.
type rankBalancer struct{ picks, ranks *int }

func (b rankBalancer) Build(c []Candidate) Picker { return &rankPicker{b: b, n: len(c)} }

type rankPicker struct {
	b rankBalancer
	n int
}

func (p *rankPicker) Pick(_ uint64, attempt int) int {
	*p.b.picks++
	return attempt % p.n
}

func (p *rankPicker) Rank(_ uint64, dst []int) {
	*p.b.ranks++
	for i := range dst {
		dst[i] = i
	}
}

// TestPoolRanksOnce checks that a pick that must look past the first
// choice asks a Ranker for the whole order once, not Pick per attempt.
func TestPoolRanksOnce(t *testing.T) {
	var picks, ranks int
	p := New[int](Config{Balancer: rankBalancer{&picks, &ranks}})
	defer p.Close()
	const n = 6
	ts := make([]Target[int], n)
	for i := range ts {
		ts[i] = Target[int]{ID: strconv.Itoa(i), MaxInFlight: 1, Value: i}
	}
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	var held []Lease[int]
	for i := range n - 1 {
		l, err := p.Acquire(strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}
	picks, ranks = 0, 0
	l, err := p.PickHash(1)
	if err != nil || l.Value() != n-1 {
		t.Fatalf("pick: %v, %v; want the last backend", l.Value(), err)
	}
	l.Release(Success)
	if picks != 1 || ranks != 1 {
		t.Fatalf("%d Pick and %d Rank calls, want 1 and 1", picks, ranks)
	}
	for i := range held {
		held[i].Release(Success)
	}
}

func TestAcquire(t *testing.T) {
	p := New[string](Config{})
	if err := p.Sync([]Target[string]{{ID: "a", MaxInFlight: 1, Value: "va"}, {ID: "b"}, {ID: "c"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire("x"); !errors.Is(err, ErrUnknownBackend) {
		t.Fatalf("unknown ID: %v", err)
	}
	l, err := p.Acquire("a")
	if err != nil || l.Value() != "va" {
		t.Fatalf("acquire a: %q, %v", l.Value(), err)
	}
	if _, err = p.Acquire("a"); !errors.Is(err, ErrSaturated) {
		t.Fatalf("a at its cap: %v", err)
	}
	l.Release(Success)
	if l, err = p.Acquire("a"); err != nil {
		t.Fatalf("a after release: %v", err)
	}
	l.Release(Success)

	p.Drain("b")
	if _, err = p.Acquire("b"); !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrSaturated) {
		t.Fatalf("draining b: %v", err)
	}
	c, _ := p.Backend("c")
	p.ReportCheck(c, HealthDown, CheckResult{})
	if _, err = p.Acquire("c"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("down c: %v", err)
	}
	a, _ := p.Backend("a")
	p.MarkOffline(a, nil)
	if _, err = p.Acquire("a"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("offline a: %v", err)
	}
	p.MarkOnline(a)
	p.ReportCheck(a, HealthDown, CheckResult{})

	// Panic mode ignores health, as Pick does.
	if err = p.SetConfig(Config{Fallback: FallbackAll}); err != nil {
		t.Fatal(err)
	}
	if l, err = p.Acquire("c"); err != nil {
		t.Fatalf("down c in panic mode: %v", err)
	}
	l.Release(Success)
	if _, err := p.Acquire("b"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("draining b in panic mode: %v", err)
	}

	p.Close()
	if _, err := p.Acquire("c"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

// TestRequestTriesManyDistinct checks that a Request that retries past
// the size where it switches its tried set to a map still never returns a
// backend twice.
func TestRequestTriesManyDistinct(t *testing.T) {
	for _, bal := range []Balancer{RoundRobin(), Random(), Rendezvous(RendezvousConfig{})} {
		const n = 100
		p := New[int](Config{Balancer: bal, Retry: RetryConfig{Attempts: n + 1}})
		ts := make([]Target[int], n)
		for i := range ts {
			ts[i] = Target[int]{ID: strconv.Itoa(i), Value: i}
		}
		if err := p.Sync(ts); err != nil {
			t.Fatal(err)
		}
		r := p.Request("k")
		seen := make(map[int]bool)
		for range n {
			l, err := r.Next()
			if err != nil {
				t.Fatalf("%v: after %d: %v", bal, len(seen), err)
			}
			if seen[l.Value()] {
				t.Fatalf("%v: backend %d returned twice", bal, l.Value())
			}
			seen[l.Value()] = true
			l.Release(Failure)
		}
		if _, err := r.Next(); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("%v: with every backend tried: %v", bal, err)
		}
		p.Close()
	}
}

// TestStateChangeKeepsOtherTiers checks that a state change recomputes
// only the tier of the backend that changed: the other tiers keep their
// member lists and Pickers.
func TestStateChangeKeepsOtherTiers(t *testing.T) {
	p := New[int](Config{})
	defer p.Close()
	var ts []Target[int]
	for i := range 9 {
		ts = append(ts, Target[int]{ID: strconv.Itoa(i), Priority: i % 3})
	}
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	before := p.view.Load()
	b, _ := p.Backend("4") // priority 1
	p.MarkOffline(b, nil)
	after := p.view.Load()
	if len(after.tiers) != 3 || len(after.tiers[1].members) != 2 {
		t.Fatalf("tiers after the change: %+v", after.tiers)
	}
	for _, i := range []int{0, 2} {
		if !sameList(before.tiers[i].members, after.tiers[i].members) || before.tiers[i].picker != after.tiers[i].picker {
			t.Errorf("tier %d was rebuilt", i)
		}
	}
	if slices.Contains(after.tiers[1].members, b) {
		t.Fatal("the offline backend is still in its tier")
	}
	p.MarkOnline(b)
	if got := p.view.Load().tiers[1].members; len(got) != 3 || got[1] != b {
		t.Fatalf("tier 1 after MarkOnline: %v", got)
	}
}

func TestAcquireDoesNotAllocate(t *testing.T) {
	p := New[int](Config{})
	defer p.Close()
	if err := p.Put(Target[int]{ID: "backend-a"}); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		l, err := p.Acquire("backend-a")
		if err != nil {
			t.Fatal(err)
		}
		l.Release(Success)
	}); n != 0 {
		t.Fatalf("%v allocations per Acquire", n)
	}
}

// TestStateChangeCostIsPerTier checks that a state change costs the same
// however large the other tiers are: their candidates are not made again.
func TestStateChangeCostIsPerTier(t *testing.T) {
	bytes := func(others int) uint64 {
		p := New[int](Config{})
		defer p.Close()
		var ts []Target[int]
		for tr := range 4 {
			n := others
			if tr == 0 {
				n = 20
			}
			for i := range n {
				ts = append(ts, Target[int]{ID: strconv.Itoa(tr) + "/" + strconv.Itoa(i), Priority: tr})
			}
		}
		if err := p.Sync(ts); err != nil {
			t.Fatal(err)
		}
		b, _ := p.Backend("0/3")
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		for range 20 {
			p.MarkOffline(b, nil)
			p.MarkOnline(b)
		}
		runtime.ReadMemStats(&m1)
		return (m1.TotalAlloc - m0.TotalAlloc) / 40
	}
	small, large := bytes(20), bytes(2000)
	if large > small+1024 {
		t.Fatalf("%d bytes per change beside large tiers, %d beside small ones", large, small)
	}
}
