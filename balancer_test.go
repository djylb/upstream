package upstream

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
)

func candidates(weights ...int) []Candidate {
	c := make([]Candidate, len(weights))
	for i, w := range weights {
		c[i] = Candidate{InFlight: new(atomic.Int64), ID: "b" + strconv.Itoa(i), Weight: w}
	}
	return c
}

func countPicks(p Picker, n, picks int) []int {
	counts := make([]int, n)
	for range picks {
		counts[p.Pick(rand.Uint64(), 0)]++
	}
	return counts
}

func TestRoundRobinIsExact(t *testing.T) {
	p := RoundRobin().Build(candidates(1, 5, 1, 1))
	if got := countPicks(p, 4, 4000); !slices.Equal(got, []int{1000, 1000, 1000, 1000}) {
		t.Fatalf("counts = %v, want 1000 each", got)
	}
}

func isRotation(got, want []int) bool {
	for off := range want {
		if slices.Equal(got, append(slices.Clone(want[off:]), want[:off]...)) {
			return true
		}
	}
	return false
}

func TestWeightedRoundRobinIsSmooth(t *testing.T) {
	want := []int{0, 0, 1, 0, 2, 0, 0}
	p := WeightedRoundRobin().Build(candidates(5, 1, 1))
	if _, ok := p.(*sequencePicker); !ok {
		t.Fatalf("picker is %T, want a precomputed sequence", p)
	}
	for range 3 {
		got := make([]int, len(want))
		for i := range got {
			got[i] = p.Pick(0, 0)
		}
		if !isRotation(got, want) {
			t.Fatalf("cycle = %v, want a rotation of %v", got, want)
		}
	}

}

func TestWeightedRoundRobinCounts(t *testing.T) {
	// Weights with a common divisor, and equal weights, which use a plain
	// counter.
	for _, weights := range [][]int{{30, 20, 10}, {4, 4, 4}, {0, 1, 2}} {
		p := WeightedRoundRobin().Build(candidates(weights...))
		sum := 0
		for _, w := range weights {
			sum += max(w, 1)
		}
		got := countPicks(p, len(weights), sum*100)
		for i, w := range weights {
			if got[i] != max(w, 1)*100 {
				t.Errorf("weights %v: counts %v", weights, got)
				break
			}
		}
	}
}

func TestWeightedRoundRobinLongCycle(t *testing.T) {
	weights := []int{MaxWeight, MaxWeight - 1, MaxWeight / 3}
	p := WeightedRoundRobin().Build(candidates(weights...))
	if _, ok := p.(*aliasPicker); !ok {
		t.Fatalf("picker is %T, want the weighted random fallback", p)
	}
	assertShares(t, countPicks(p, 3, 100000), weights, 0.01)
}

func assertShares(t *testing.T, got []int, weights []int, tolerance float64) {
	t.Helper()
	total, wsum := 0, 0
	for i := range got {
		total += got[i]
		wsum += max(weights[i], 1)
	}
	for i := range got {
		want := float64(max(weights[i], 1)) / float64(wsum)
		share := float64(got[i]) / float64(total)
		if math.Abs(share-want) > tolerance {
			t.Errorf("candidate %d: share %.3f, want %.3f±%.3f (counts %v)", i, share, want, tolerance, got)
		}
	}
}

func TestRandomRespectsWeights(t *testing.T) {
	for _, weights := range [][]int{{1, 2, 7}, {1, 1, 1, 1}, {100, 1}, {3, 0, 5, 9, 2}} {
		p := Random().Build(candidates(weights...))
		assertShares(t, countPicks(p, len(weights), 200000), weights, 0.01)
	}
}

func TestPowerOfTwoAvoidsLoadedBackend(t *testing.T) {
	c := candidates(1, 1, 1, 1)
	c[0].InFlight.Store(10)
	p := PowerOfTwo().Build(c)
	if got := countPicks(p, 4, 10000); got[0] != 0 {
		t.Fatalf("loaded backend picked %d times", got[0])
	}
	if PowerOfTwo().Build(candidates(1)).Pick(0, 0) != 0 {
		t.Fatal("single candidate not picked")
	}

	// Idle candidates are chosen by weight, and nil load reads as zero.
	c = candidates(1, 3)
	c[0].InFlight = nil
	got := countPicks(PowerOfTwo().Build(c), 2, 1000)
	if got[1] != 1000 {
		t.Fatalf("counts %v, want the heavier idle backend every time", got)
	}
}

func TestLeastConn(t *testing.T) {
	c := candidates(1, 1, 1)
	c[0].InFlight.Store(3)
	c[1].InFlight.Store(1)
	c[2].InFlight.Store(2)
	p := LeastConn().Build(c)
	if got := countPicks(p, 3, 1000); got[1] != 1000 {
		t.Fatalf("counts %v, want backend 1 every time", got)
	}

	// Weighted: 4 in flight on weight 4 is lighter than 2 on weight 1.
	c = candidates(4, 1)
	c[0].InFlight.Store(4)
	c[1].InFlight.Store(2)
	if got := LeastConn().Build(c).Pick(0, 0); got != 0 {
		t.Fatalf("picked %d, want 0", got)
	}

	// Ties are broken at random.
	assertShares(t, countPicks(LeastConn().Build(candidates(1, 1, 1, 1)), 4, 40000), []int{1, 1, 1, 1}, 0.02)

	// Retries pick at random.
	seen := make(map[int]bool)
	for range 200 {
		seen[p.Pick(0, 1)] = true
	}
	if len(seen) != 3 {
		t.Fatalf("retries reached %d candidates, want 3", len(seen))
	}
}

func ringOwners(b Balancer, c []Candidate, keys int) map[int]string {
	p := b.Build(c)
	m := make(map[int]string, keys)
	for k := range keys {
		m[k] = c[p.Pick(hashString(strconv.Itoa(k)), 0)].ID
	}
	return m
}

func TestConsistentHashStability(t *testing.T) {
	const keys = 20000
	h := ConsistentHash(HashConfig{})
	base := candidates(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	before := ringOwners(h, base, keys)

	// The ring does not depend on candidate order.
	shuffled := slices.Clone(base)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for k, id := range ringOwners(h, shuffled, keys) {
		if before[k] != id {
			t.Fatalf("key %d moved from %s to %s when candidates were reordered", k, before[k], id)
		}
	}

	// Adding an 11th candidate moves only keys to it, about 1/11 of them.
	added := append(slices.Clone(base), Candidate{InFlight: new(atomic.Int64), ID: "new", Weight: 1})
	moved := 0
	for k, id := range ringOwners(h, added, keys) {
		if id != before[k] {
			moved++
			if id != "new" {
				t.Fatalf("key %d moved from %s to %s, not to the new candidate", k, before[k], id)
			}
		}
	}
	if frac := float64(moved) / keys; frac < 0.05 || frac > 0.14 {
		t.Fatalf("adding moved %.3f of keys, want about 1/11", frac)
	}

	// Removing a candidate moves only its own keys.
	removed := slices.Delete(slices.Clone(base), 3, 4)
	moved = 0
	for k, id := range ringOwners(h, removed, keys) {
		if id != before[k] {
			moved++
			if before[k] != base[3].ID {
				t.Fatalf("key %d moved from %s although %s was removed", k, before[k], base[3].ID)
			}
		}
	}
	if frac := float64(moved) / keys; frac < 0.05 || frac > 0.15 {
		t.Fatalf("removing moved %.3f of keys, want about 1/10", frac)
	}
}

func TestConsistentHashWeights(t *testing.T) {
	weights := []int{1, 3, 6}
	p := ConsistentHash(HashConfig{VirtualNodes: 400}).Build(candidates(weights...))
	assertShares(t, countPicks(p, 3, 100000), weights, 0.04)
}

func TestConsistentHashRetriesVisitEveryCandidate(t *testing.T) {
	for _, n := range []int{1, 5, 300} {
		c := candidates(slices.Repeat([]int{1}, n)...)
		p := ConsistentHash(HashConfig{VirtualNodes: 10}).Build(c)
		for k := range 20 {
			key := rand.Uint64()
			seen := make(map[int]bool, n)
			for attempt := range n {
				seen[p.Pick(key, attempt)] = true
			}
			if len(seen) != n {
				t.Fatalf("n=%d key %d: attempts reached %d candidates", n, k, len(seen))
			}
			if p.Pick(key, 0) != p.Pick(key, n) {
				// Attempts past the last candidate fall back to the owner.
				t.Fatalf("n=%d: attempt %d did not fall back to the owner", n, n)
			}
		}
	}
}

func TestConsistentHashBoundedLoad(t *testing.T) {
	c := candidates(1, 1, 1, 1)
	p := ConsistentHash(HashConfig{LoadFactor: 1.25}).Build(c)
	key := uint64(42)
	owner := p.Pick(key, 0)
	if again := ConsistentHash(HashConfig{}).Build(c).Pick(key, 0); again != owner {
		t.Fatalf("idle bounded ring picked %d, unbounded %d", owner, again)
	}
	// Owner at 10 in flight, total 10: its cap is ceil(1.25*11/4) = 4.
	c[owner].InFlight.Store(10)
	next := p.Pick(key, 0)
	if next == owner {
		t.Fatal("overloaded owner still picked")
	}
	if p.Pick(key, 0) != next {
		t.Fatal("bounded pick is not deterministic")
	}
	// Every candidate over its cap: the first in ring order is used.
	for i := range c {
		c[i].InFlight.Store(100)
	}
	c[owner].InFlight.Store(1000)
	if got := p.Pick(key, 0); got == owner {
		t.Fatal("picked the most loaded candidate")
	}
	_ = ConsistentHash(HashConfig{LoadFactor: 0.5}) // raised to 1
	_ = ConsistentHash(HashConfig{LoadFactor: math.NaN()})
}

func TestHashStringIsFNV1a(t *testing.T) {
	// Placement depends on this function; it must never change.
	for s, want := range map[string]uint64{"": 0xcbf29ce484222325, "a": 0xaf63dc4c8601ec8c, "foobar": 0x85944171f73967e8} {
		if got := hashString(s); got != want {
			t.Errorf("hashString(%q) = %#x, want %#x", s, got, want)
		}
	}
}

func FuzzBalancers(f *testing.F) {
	f.Add([]byte{1, 2, 3}, uint64(7), uint8(0))
	f.Add([]byte{0, 255, 9, 9}, uint64(0), uint8(3))
	balancers := []Balancer{RoundRobin(), WeightedRoundRobin(), Random(), PowerOfTwo(), LeastConn(),
		ConsistentHash(HashConfig{VirtualNodes: 8}), ConsistentHash(HashConfig{VirtualNodes: 3, LoadFactor: 1.1})}
	f.Fuzz(func(t *testing.T, weights []byte, key uint64, attempt uint8) {
		if len(weights) == 0 || len(weights) > 64 {
			return
		}
		ws := make([]int, len(weights))
		for i, w := range weights {
			ws[i] = int(w)
		}
		c := candidates(ws...)
		for i := range c {
			c[i].InFlight.Store(int64(weights[len(weights)-1-i]))
		}
		for _, b := range balancers {
			p := b.Build(c)
			for range 4 {
				if i := p.Pick(key, int(attempt)); i < 0 || i >= len(c) {
					t.Fatalf("%T picked %d of %d", b, i, len(c))
				}
			}
		}
		// A full smooth cycle picks each candidate exactly its weight.
		p := WeightedRoundRobin().Build(c)
		counts := make([]int, len(c))
		g, sum := 0, 0
		for _, w := range ws {
			g = int(gcd(int64(g), int64(max(w, 1))))
		}
		for _, w := range ws {
			sum += max(w, 1) / g
		}
		for range sum {
			counts[p.Pick(0, 0)]++
		}
		for i, w := range ws {
			if counts[i] != max(w, 1)/g {
				t.Fatalf("weights %v: cycle counts %v", ws, counts)
			}
		}
	})
}

func TestBoundedHashAttemptsAreDistinct(t *testing.T) {
	c := candidates(1, 1, 1, 1, 1)
	p := ConsistentHash(HashConfig{LoadFactor: 1.25}).Build(c)
	for k := range 200 {
		key := uint64(k) * 0x9e3779b97f4a7c15
		owner := p.Pick(key, 0)
		c[owner].InFlight.Store(50) // over its cap
		seen := map[int]bool{}
		for attempt := range len(c) {
			seen[p.Pick(key, attempt)] = true
		}
		if len(seen) != len(c) {
			t.Fatalf("key %d: %d distinct candidates over %d attempts", k, len(seen), len(c))
		}
		if p.Pick(key, 0) == owner {
			t.Fatal("overloaded owner picked first")
		}
		c[owner].InFlight.Store(0)
	}
}

// TestConsistentHashRebuildMatchesBuild checks that updating a ring gives
// the ring that building it again would, as members come, go and change
// weight.
func TestConsistentHashRebuildMatchesBuild(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, cfg := range []HashConfig{{}, {VirtualNodes: 7}} {
		bal := ConsistentHash(cfg).(Rebuilder)
		var cands []Candidate
		for i := range 40 {
			cands = append(cands, Candidate{ID: "c" + strconv.Itoa(i), Weight: 1})
		}
		prev := bal.Build(cands)
		next := 40
		for step := range 200 {
			cands = slices.Clone(cands)
			switch k := r.IntN(len(cands)); r.IntN(4) {
			case 0:
				if len(cands) > 1 {
					cands = slices.Delete(cands, k, k+1)
				}
			case 1:
				cands = slices.Insert(cands, k, Candidate{ID: "c" + strconv.Itoa(next), Weight: 1})
				next++
			case 2:
				cands[k].Weight = 1 + r.IntN(4)
			case 3:
				r.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
			}
			got := bal.Rebuild(prev, cands).(*ringPicker)
			want := bal.Build(cands).(*ringPicker)
			if !slices.Equal(got.hashes, want.hashes) || !slices.Equal(got.owners, want.owners) || !slices.Equal(got.counts, want.counts) {
				t.Fatalf("%+v step %d: rebuilt ring differs", cfg, step)
			}
			prev = got
		}
	}
	// A Picker of another balancer is built afresh.
	c := candidates(1, 1)
	if _, ok := ConsistentHash(HashConfig{}).(Rebuilder).Rebuild(RoundRobin().Build(c), c).(*ringPicker); !ok {
		t.Fatal("foreign picker")
	}
}
