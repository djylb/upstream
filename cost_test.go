package upstream

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// costed returns targets for ids with the given costs.
func costed(ids []string, cost ...uint64) []Target[string] {
	ts := targets(ids...)
	for i := range ts {
		ts[i].Cost = cost[i]
	}
	return ts
}

func bandIDs(bs [][]*Backend[string]) [][]string {
	out := make([][]string, len(bs))
	for i, b := range bs {
		for _, x := range b {
			out[i] = append(out[i], x.id)
		}
	}
	return out
}

func TestCostBands(t *testing.T) {
	mk := func(cost ...uint64) []*Backend[string] {
		bs := make([]*Backend[string], len(cost))
		for i, c := range cost {
			bs[i] = &Backend[string]{id: strconv.Itoa(i), cost: c}
		}
		return bs
	}
	for _, tc := range []struct {
		name string
		cost []uint64
		tol  uint64
		want [][]string
	}{
		{"uniform", []uint64{5, 5, 5}, 0, [][]string{{"0", "1", "2"}}},
		{"exact", []uint64{3, 1, 3, 2}, 0, [][]string{{"1"}, {"3"}, {"0", "2"}}},
		{"tolerance", []uint64{1000, 1200, 1201, 1441, 1442}, 20, [][]string{{"0", "1"}, {"2", "3"}, {"4"}}},
		{"chained bands start anew", []uint64{100, 119, 121, 145, 146}, 20, [][]string{{"0", "1"}, {"2", "3"}, {"4"}}},
		{"zero cost only matches zero", []uint64{0, 1, 0}, 1000, [][]string{{"0", "2"}, {"1"}}},
		{"saturating", []uint64{math.MaxUint64 / 2, math.MaxUint64}, 101, [][]string{{"0", "1"}}},
		{"order kept within a band", []uint64{9, 1, 8, 2}, 100, [][]string{{"1", "3"}, {"0", "2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := bandIDs(costBands(mk(tc.cost...), tc.tol)); !slices.EqualFunc(got, tc.want, slices.Equal) {
				t.Fatalf("bands %v, want %v", got, tc.want)
			}
		})
	}
	if bandLimit(math.MaxUint64, math.MaxUint64) != math.MaxUint64 || bandLimit(math.MaxUint64-1, 1) != math.MaxUint64 || bandLimit(200, 50) != 300 {
		t.Fatal("bandLimit does not saturate or scale")
	}
	uni := mk(4, 4)
	if bs := costBands(uni, 0); len(bs) != 1 || !sameList(bs[0], uni) {
		t.Fatal("a list of one cost is not its own band")
	}
}

func TestCostPrefersCheapBand(t *testing.T) {
	for name, bal := range testBalancers() {
		t.Run(name, func(t *testing.T) {
			p := New[string](Config{Balancer: bal, CostTolerance: 20})
			defer p.Close()
			if err := p.Sync(costed([]string{"far", "near1", "mid", "near2"}, 5000, 1000, 2000, 1150)); err != nil {
				t.Fatal(err)
			}
			got := pickIDs(t, p, 200)
			if got["far"]+got["mid"] != 0 || got["near1"] == 0 || got["near2"] == 0 {
				t.Fatalf("picks %v, want both near members only", got)
			}
			if n := p.Available(); n != 4 {
				t.Fatalf("Available = %d, want 4", n)
			}
			// The cheap band down: the next band takes over.
			for _, id := range []string{"near1", "near2"} {
				b, _ := p.Backend(id)
				p.ReportCheck(b, HealthDown, CheckResult{})
			}
			if got := pickIDs(t, p, 50); got["mid"] != 50 {
				t.Fatalf("picks %v, want all on mid", got)
			}
			// A cost change moves traffic: far becomes the cheapest.
			if err := p.Put(Target[string]{ID: "far", Value: "addr-far", Cost: 10}); err != nil {
				t.Fatal(err)
			}
			if got := pickIDs(t, p, 50); got["far"] != 50 {
				t.Fatalf("picks %v, want all on far", got)
			}
			if b, _ := p.Backend("far"); b.Cost() != 10 || b.Status().Cost != 10 {
				t.Fatalf("cost %d, status %d, want 10", b.Cost(), b.Status().Cost)
			}
		})
	}
}

// TestCostWithinTier checks that bands split tiers, never join them: a
// cheap member of a less preferred tier stays a backup.
func TestCostWithinTier(t *testing.T) {
	p := New[string](Config{})
	defer p.Close()
	ts := costed([]string{"a", "b", "backup"}, 100, 200, 1)
	ts[2].Priority = 1
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	if got := pickIDs(t, p, 20); got["a"] != 20 {
		t.Fatalf("picks %v, want all on a", got)
	}
	a, _ := p.Backend("a")
	p.MarkOffline(a, nil)
	if got := pickIDs(t, p, 20); got["b"] != 20 {
		t.Fatalf("picks %v, want all on b before the backup tier", got)
	}
}

// TestCostBandsOverflow checks that a full band passes picks on to the
// dearer bands of its tier without Overflow, and that the tier boundary
// still holds them.
func TestCostBandsOverflow(t *testing.T) {
	p := New[string](Config{})
	defer p.Close()
	ts := costed([]string{"near", "far", "backup"}, 1, 9, 0)
	ts[0].MaxInFlight, ts[1].MaxInFlight = 1, 1
	ts[2].Priority = 1
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	l1 := mustPick(t, p)
	l2 := mustPick(t, p)
	if l1.Backend().ID() != "near" || l2.Backend().ID() != "far" {
		t.Fatalf("picks %s, %s, want near then far", l1.Backend().ID(), l2.Backend().ID())
	}
	if _, err := p.Pick(); !errors.Is(err, ErrSaturated) {
		t.Fatalf("full tier: %v, want ErrSaturated", err)
	}
	if err := p.SetConfig(Config{Overflow: true}); err != nil {
		t.Fatal(err)
	}
	l3 := mustPick(t, p)
	if l3.Backend().ID() != "backup" {
		t.Fatalf("pick %s with Overflow, want backup", l3.Backend().ID())
	}
	for _, l := range []Lease[string]{l1, l2, l3} {
		l.Release(Success)
	}
}

// TestCostRetryAndFallback checks that a request tries the cheap band
// before the dear one, and that panic mode keeps the bands.
func TestCostRetryAndFallback(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 3}, Fallback: FallbackAll})
	defer p.Close()
	if err := p.Sync(costed([]string{"far", "near"}, 7, 3)); err != nil {
		t.Fatal(err)
	}
	r := p.Request("")
	var order []string
	for {
		l, err := r.Next()
		if err != nil {
			break
		}
		order = append(order, l.Backend().ID())
		l.Release(Failure)
	}
	if !slices.Equal(order, []string{"near", "far"}) {
		t.Fatalf("attempts %v, want near then far", order)
	}
	for _, id := range []string{"near", "far"} {
		b, _ := p.Backend(id)
		p.ReportCheck(b, HealthDown, CheckResult{})
	}
	if got := pickIDs(t, p, 20); got["near"] != 20 {
		t.Fatalf("panic picks %v, want all on near", got)
	}
}

// TestCostRendezvousStable checks that keys stay on their members when a
// member of another band changes, and spread over the cheap band.
func TestCostRendezvousStable(t *testing.T) {
	p := New[string](Config{Balancer: Rendezvous(RendezvousConfig{}), CostTolerance: 10})
	defer p.Close()
	if err := p.Sync(costed([]string{"a", "b", "c", "x"}, 100, 105, 109, 500)); err != nil {
		t.Fatal(err)
	}
	owners := func() map[string]string {
		m := map[string]string{}
		for i := range 300 {
			k := strconv.Itoa(i)
			l, err := p.PickKey(k)
			if err != nil {
				t.Fatal(err)
			}
			m[k] = l.Backend().ID()
			l.Release(Success)
		}
		return m
	}
	before := owners()
	seen := map[string]bool{}
	for _, id := range before {
		seen[id] = true
	}
	if !seen["a"] || !seen["b"] || !seen["c"] || seen["x"] {
		t.Fatalf("owners %v, want a, b and c only", seen)
	}
	if err := p.Put(Target[string]{ID: "x", Value: "addr-x", Cost: 300}); err != nil {
		t.Fatal(err)
	}
	if after := owners(); !mapsEqual(before, after) {
		t.Fatal("a change of another band moved keys")
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestCostKeepsPickers checks that a health change in a banded tier
// rebuilds only the band it touches.
func TestCostKeepsPickers(t *testing.T) {
	builds := &countingBalancer{Balancer: WeightedRoundRobin()}
	p := New[string](Config{Balancer: builds})
	defer p.Close()
	if err := p.Sync(costed([]string{"a", "b", "c", "d"}, 1, 1, 2, 2)); err != nil {
		t.Fatal(err)
	}
	n := builds.builds.Load()
	c, _ := p.Backend("c")
	p.ReportCheck(c, HealthDown, CheckResult{})
	if d := builds.builds.Load() - n; d != 1 {
		t.Fatalf("%d builds for a change in one band, want 1", d)
	}
	n = builds.builds.Load()
	if err := p.SetConfig(Config{Balancer: builds}); err != nil {
		t.Fatal(err)
	}
	if d := builds.builds.Load() - n; d != 0 {
		t.Fatalf("%d builds for the same settings, want 0", d)
	}
}

func TestCostPickAllocFree(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	p := New[string](Config{Balancer: Rendezvous(RendezvousConfig{})})
	defer p.Close()
	if err := p.Sync(costed([]string{"a", "b", "c"}, 3, 1, 2)); err != nil {
		t.Fatal(err)
	}
	b, _ := p.Backend("b")
	p.MarkOffline(b, nil)
	if n := testing.AllocsPerRun(1000, func() {
		l, err := p.PickKey("k")
		if err != nil {
			t.Fatal(err)
		}
		l.Release(Success)
	}); n != 0 {
		t.Fatalf("%v allocations per pick, want 0", n)
	}
}

// BenchmarkPickCostBands measures Pick and Release on 16 backends in four
// cost bands (the cheapest offline) against one band.
func BenchmarkPickCostBands(b *testing.B) {
	for _, bands := range []int{1, 4} {
		b.Run("bands="+strconv.Itoa(bands), func(b *testing.B) {
			p := New[string](Config{Balancer: Rendezvous(RendezvousConfig{})})
			b.Cleanup(p.Close)
			ts := make([]Target[string], 16)
			for i := range ts {
				ts[i] = Target[string]{ID: "backend-" + strconv.Itoa(i), Value: strconv.Itoa(i), Cost: uint64(i % bands)}
			}
			if err := p.Sync(ts); err != nil {
				b.Fatal(err)
			}
			if bands > 1 {
				for i := 0; i < 16; i += bands {
					x, _ := p.Backend(ts[i].ID)
					p.MarkOffline(x, nil)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				l, err := p.PickKey("192.0.2.17")
				if err != nil {
					b.Fatal(err)
				}
				l.Release(Success)
			}
		})
	}
}

// TestCostOracle checks random pools against the definition: a pick is a
// member of the cheapest band, by CostTolerance, of the eligible members
// of the most preferred tier that has one.
func TestCostOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for round := range 200 {
		tol := rng.IntN(60)
		p := New[string](Config{Balancer: testBalancerList()[round%len(testBalancerList())], CostTolerance: tol})
		n := 1 + rng.IntN(12)
		ts := make([]Target[string], n)
		for i := range ts {
			ts[i] = Target[string]{ID: "m" + strconv.Itoa(i), Value: "v", Priority: rng.IntN(2), Cost: uint64(rng.IntN(5) * 50)}
		}
		if err := p.Sync(ts); err != nil {
			t.Fatal(err)
		}
		down := map[string]bool{}
		for _, x := range ts {
			if rng.IntN(3) == 0 {
				b, _ := p.Backend(x.ID)
				p.MarkOffline(b, nil)
				down[x.ID] = true
			}
		}
		// The oracle.
		want := map[string]bool{}
		for prio := range 2 {
			var cheapest uint64 = math.MaxUint64
			for _, x := range ts {
				if x.Priority == prio && !down[x.ID] {
					cheapest = min(cheapest, x.Cost)
				}
			}
			if cheapest == math.MaxUint64 {
				continue
			}
			for _, x := range ts {
				if x.Priority == prio && !down[x.ID] && x.Cost <= bandLimit(cheapest, uint64(tol)) {
					want[x.ID] = true
				}
			}
			break
		}
		for k := range 30 {
			l, err := p.PickKey(strconv.Itoa(k))
			if len(want) == 0 {
				if err == nil {
					t.Fatalf("round %d: picked %s with every member offline", round, l.Backend().ID())
				}
				break
			}
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			if !want[l.Backend().ID()] {
				t.Fatalf("round %d: picked %s, want one of %v", round, l.Backend().ID(), want)
			}
			l.Release(Success)
		}
		p.Close()
	}
}

func testBalancerList() []Balancer {
	var out []Balancer
	for _, name := range sortedBalancers() {
		out = append(out, testBalancers()[name])
	}
	return out
}
