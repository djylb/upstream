package upstream

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
)

// A Candidate is a balancer's view of one backend.
type Candidate struct {
	// InFlight counts the backend's requests in flight. Load-aware
	// balancers read it; nil means no load information, read as zero.
	InFlight *atomic.Int64

	// Latency is the backend's moving average latency in nanoseconds, as
	// reported with Lease.Observe; zero means no sample yet, and nil no
	// latency information. Latency-aware balancers read it at pick time.
	Latency *atomic.Int64

	// Meta is whatever the caller attaches to the candidate. A Pool sets
	// it to the *Backend[T], so that a balancer can route by the
	// backend's value, such as its zone or labels, or by its Status.
	Meta any

	// ID identifies the backend. Consistent hashing places backends by ID,
	// so equal IDs map keys to the same place in every process.
	ID string

	// Weight is the backend's relative capacity. Values below 1 mean 1.
	Weight int
}

func (c *Candidate) weight() int64 {
	return max(int64(c.Weight), 1)
}

// A Balancer builds Pickers for sets of candidates. A Pool calls Build
// when its routing view changes: when members join, leave or change
// weight, go down or come back, are ejected or restored, and at the steps
// of a slow start. Calls never overlap for one pool and never happen
// while the pool holds a lock; picks use the previous Picker until Build
// returns. If Build or Rebuild panics, the pool recovers, keeps the
// previous Picker, reports an [EventBalancerPanic] and calls the Balancer
// again on its next change. A Balancer that can update a Picker more cheaply than it builds
// one also implements Rebuilder.
//
// Balancers can be used without a Pool: build a Picker from candidates and
// call Pick.
type Balancer interface {
	// Build returns a Picker for candidates, which is never empty. The
	// Picker may keep the slice; the caller does not modify it afterwards.
	Build(candidates []Candidate) Picker
}

// A Rebuilder is a Balancer that derives a Picker from the previous one,
// for balancers whose Build is expensive, such as hash rings. A Pool calls
// Rebuild instead of Build when it has a Picker that the same Balancer
// built. The result must be what Build would return for candidates;
// Rebuild only saves work, such as by keeping the parts of prev whose
// candidates did not change.
type Rebuilder interface {
	Balancer

	// Rebuild returns a Picker for candidates, which is never empty. prev
	// was returned by Build or Rebuild of this Balancer and may still be
	// in use by picks: Rebuild must not modify it.
	Rebuild(prev Picker, candidates []Candidate) Picker
}

// A Picker chooses among the candidates it was built from. It must be safe
// for concurrent use and should not allocate.
type Picker interface {
	// Pick returns the index of the chosen candidate. key is the request's
	// hash for key-affine balancers, or a random number when the caller
	// has no key. attempt is 0 for the first choice; when the caller
	// cannot use the chosen backend it asks again with attempt 1, 2 and so
	// on, and a Picker that is deterministic for a key should then return
	// other candidates.
	Pick(key uint64, attempt int) int
}

// RoundRobin returns a Balancer that cycles through the candidates in
// order and ignores weights. Each Picker starts at a random position so
// that many pools do not hit the same backend in step.
func RoundRobin() Balancer { return roundRobin{} }

type roundRobin struct{}

func (roundRobin) Build(c []Candidate) Picker { return newCounterPicker(len(c)) }

func (roundRobin) String() string { return "round-robin" }

type counterPicker struct {
	next atomic.Uint64
	n    uint64
}

func newCounterPicker(n int) *counterPicker {
	p := &counterPicker{n: uint64(n)}
	p.next.Store(rand.Uint64())
	return p
}

func (p *counterPicker) Pick(_ uint64, _ int) int { return int(p.next.Add(1) % p.n) }

// WeightedRoundRobin returns a Balancer that implements smooth weighted
// round-robin: over each cycle every candidate is picked in proportion to
// its weight, and its picks are spread through the cycle instead of
// bunched together. For weights 5, 1, 1 a cycle is a a b a c a a.
//
// The cycle is precomputed, which makes Pick a single atomic add, when it is
// short enough: the sum of the weights divided by their greatest common
// divisor, times the number of candidates, at most 1<<22. Longer cycles,
// which take extreme weight ratios, fall back to weighted random picks:
// proportions then hold on average rather than exactly in every cycle, and
// picks stay lock-free.
func WeightedRoundRobin() Balancer { return weightedRoundRobin{} }

type weightedRoundRobin struct{}

func (weightedRoundRobin) String() string { return "weighted-round-robin" }

// maxSmoothWork bounds the work, and the memory, spent precomputing a
// smooth weighted round-robin cycle.
const maxSmoothWork = 1 << 22

func (weightedRoundRobin) Build(c []Candidate) Picker {
	w := make([]int64, len(c))
	g := int64(0)
	for i := range c {
		w[i] = c[i].weight()
		g = gcd(g, w[i])
	}
	var cycle int64
	for i := range w {
		w[i] /= g
		cycle += w[i]
	}
	if cycle == int64(len(c)) {
		return newCounterPicker(len(c))
	}
	if cycle <= maxSmoothWork/int64(len(c)) {
		s := newSmooth(w)
		seq := make([]int32, cycle)
		for i := range seq {
			seq[i] = int32(s.next())
		}
		p := &sequencePicker{seq: seq, n: uint64(cycle)}
		p.next.Store(rand.Uint64())
		return p
	}
	return newAliasPicker(c)
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

type sequencePicker struct {
	next atomic.Uint64
	seq  []int32
	n    uint64
}

func (p *sequencePicker) Pick(_ uint64, _ int) int { return int(p.seq[p.next.Add(1)%p.n]) }

// smooth is the smooth weighted round-robin state machine: every step adds
// each weight to its current value, picks the largest current value and
// subtracts the total weight from it.
type smooth struct {
	weight  []int64
	current []int64
	total   int64
}

func newSmooth(w []int64) smooth {
	s := smooth{weight: w, current: make([]int64, len(w))}
	for _, x := range w {
		s.total += x
	}
	return s
}

func (s *smooth) next() int {
	best := 0
	for i, w := range s.weight {
		s.current[i] += w
		if s.current[i] > s.current[best] {
			best = i
		}
	}
	s.current[best] -= s.total
	return best
}

// Random returns a Balancer that picks candidates at random in proportion
// to their weights, using the alias method so each pick takes constant
// time.
func Random() Balancer { return random{} }

type random struct{}

func (random) String() string { return "random" }

func (random) Build(c []Candidate) Picker {
	equal := true
	for i := range c {
		equal = equal && c[i].weight() == c[0].weight()
	}
	if equal {
		return uniformPicker{n: len(c)}
	}
	return newAliasPicker(c)
}

type uniformPicker struct{ n int }

func (p uniformPicker) Pick(_ uint64, _ int) int { return rand.IntN(p.n) }

// aliasPicker implements Vose's alias method.
type aliasPicker struct {
	prob  []float64
	alias []int32
}

func newAliasPicker(c []Candidate) *aliasPicker {
	n := len(c)
	var total float64
	for i := range c {
		total += float64(c[i].weight())
	}
	p := &aliasPicker{prob: make([]float64, n), alias: make([]int32, n)}
	scaled := make([]float64, n)
	var small, large []int32
	for i := range c {
		scaled[i] = float64(c[i].weight()) * float64(n) / total
		if scaled[i] < 1 {
			small = append(small, int32(i))
		} else {
			large = append(large, int32(i))
		}
	}
	for len(small) > 0 && len(large) > 0 {
		s, l := small[len(small)-1], large[len(large)-1]
		small = small[:len(small)-1]
		p.prob[s] = scaled[s]
		p.alias[s] = l
		scaled[l] -= 1 - scaled[s]
		if scaled[l] < 1 {
			large = large[:len(large)-1]
			small = append(small, l)
		}
	}
	for _, i := range large {
		p.prob[i] = 1
	}
	for _, i := range small { // only left over by rounding
		p.prob[i] = 1
	}
	return p
}

func (p *aliasPicker) Pick(_ uint64, _ int) int {
	i := rand.IntN(len(p.prob))
	if rand.Float64() < p.prob[i] {
		return i
	}
	return int(p.alias[i])
}

// loadPicker holds what the load-aware pickers read.
type loadPicker struct {
	loads   []*atomic.Int64
	weights []int64
}

func newLoadPicker(c []Candidate) loadPicker {
	p := loadPicker{loads: make([]*atomic.Int64, len(c)), weights: make([]int64, len(c))}
	for i := range c {
		p.loads[i] = c[i].InFlight
		p.weights[i] = c[i].weight()
	}
	return p
}

func (p *loadPicker) load(i int) int64 {
	if l := p.loads[i]; l != nil {
		return l.Load()
	}
	return 0
}

// lighter reports whether candidate i carries less load per unit of weight
// than candidate j. One is added to each load so that weights also decide
// between idle candidates.
func (p *loadPicker) lighter(i, j int) bool {
	return (p.load(i)+1)*p.weights[j] < (p.load(j)+1)*p.weights[i]
}

// PowerOfTwo returns a Balancer that samples two distinct candidates at
// random and picks the one with fewer requests in flight relative to its
// weight. It approaches least-connections balancing at constant cost and
// avoids the herding that strict least-connections shows when many
// clients see the same idle backend.
func PowerOfTwo() Balancer { return powerOfTwo{} }

type powerOfTwo struct{}

func (powerOfTwo) String() string { return "power-of-two" }

func (powerOfTwo) Build(c []Candidate) Picker {
	return &p2cPicker{loadPicker: newLoadPicker(c)}
}

type p2cPicker struct{ loadPicker }

func (p *p2cPicker) Pick(_ uint64, _ int) int {
	n := len(p.weights)
	if n == 1 {
		return 0
	}
	i := rand.IntN(n)
	j := rand.IntN(n - 1)
	if j >= i {
		j++
	}
	if p.lighter(j, i) {
		return j
	}
	return i
}

// LeastConn returns a Balancer that picks the candidate with the fewest
// requests in flight relative to its weight, breaking ties at random. Each
// pick scans every candidate. A retry (attempt above 0) picks at random so
// that a candidate that cannot take the request is not chosen again.
func LeastConn() Balancer { return leastConn{} }

type leastConn struct{}

func (leastConn) String() string { return "least-conn" }

func (leastConn) Build(c []Candidate) Picker {
	return &leastConnPicker{loadPicker: newLoadPicker(c)}
}

type leastConnPicker struct{ loadPicker }

func (p *leastConnPicker) Pick(_ uint64, attempt int) int {
	n := len(p.weights)
	start := rand.IntN(n)
	if attempt > 0 {
		return start
	}
	best := start
	for k := 1; k < n; k++ {
		i := start + k
		if i >= n {
			i -= n
		}
		if p.lighter(i, best) {
			best = i
		}
	}
	return best
}

// Failover returns a Balancer for active-backup setups: it always picks the
// first candidate, in membership order, that can take traffic. A Pool
// leaves unhealthy and drained backends out of the candidates, so traffic
// moves to the next backend in order and returns when the first recovers.
func Failover() Balancer { return failover{} }

type failover struct{}

func (failover) Build(c []Candidate) Picker { return failoverPicker{n: len(c)} }

func (failover) String() string { return "failover" }

// ParseBalancer returns the built-in Balancer with the given name, as its
// String method writes it: "round-robin", "weighted-round-robin",
// "random", "power-of-two", "least-conn", "failover" or "consistent-hash"
// (with the default HashConfig). It suits configuration files; build a
// ConsistentHash with options in code.
func ParseBalancer(name string) (Balancer, error) {
	switch name {
	case "round-robin":
		return RoundRobin(), nil
	case "weighted-round-robin":
		return WeightedRoundRobin(), nil
	case "random":
		return Random(), nil
	case "power-of-two":
		return PowerOfTwo(), nil
	case "least-conn":
		return LeastConn(), nil
	case "failover":
		return Failover(), nil
	case "consistent-hash":
		return ConsistentHash(HashConfig{}), nil
	}
	return nil, fmt.Errorf("upstream: unknown balancer %q", name)
}

type failoverPicker struct{ n int }

func (p failoverPicker) Pick(_ uint64, attempt int) int { return attempt % p.n }
