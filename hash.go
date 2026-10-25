package upstream

import (
	"cmp"
	"math"
	"slices"
)

// HashConfig configures ConsistentHash.
type HashConfig struct {
	// VirtualNodes is the average number of ring points per candidate. More
	// points spread keys more evenly at the cost of memory and build time.
	// The default is 160.
	VirtualNodes int

	// LoadFactor enables bounded loads when above zero: a candidate that
	// already carries LoadFactor times its weighted share of the requests
	// in flight is passed over for the next one on the ring. Values below
	// 1 mean 1. Bounded loads keep a hot key from overloading one backend
	// but cost a scan of the in-flight counts on every pick.
	LoadFactor float64
}

// ConsistentHash returns a Balancer that maps keys to candidates on a hash
// ring.
//
// Each candidate owns ring points derived from its ID, in number
// proportional to its weight, and a key belongs to the first point at or
// after the key's hash. Adding or removing a candidate therefore moves only
// the keys of the ring segments it gains or loses, about 1/n of all keys,
// and every process that uses the same IDs maps a key to the same
// candidate. Retries walk the ring to the next distinct candidates, which
// are the candidates the key would move to if the earlier ones left.
//
// Pick hashes the key it is given again, so any 64-bit key works. Picks do
// not allocate, except retries and bounded-load walks on rings of more than
// 256 candidates, which need a scratch bitset.
func ConsistentHash(cfg HashConfig) Balancer {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 160
	}
	if cfg.LoadFactor < 0 || math.IsNaN(cfg.LoadFactor) {
		cfg.LoadFactor = 0
	} else if cfg.LoadFactor > 0 && cfg.LoadFactor < 1 {
		cfg.LoadFactor = 1
	}
	return consistentHash{cfg: cfg}
}

type consistentHash struct{ cfg HashConfig }

func (consistentHash) String() string { return "consistent-hash" }

type ringPoint struct {
	hash  uint64
	owner int32
}

func (h consistentHash) Build(c []Candidate) Picker {
	var sum int64
	for i := range c {
		sum += c[i].weight()
	}
	perUnit := float64(h.cfg.VirtualNodes) * float64(len(c)) / float64(sum)
	points := make([]ringPoint, 0, h.cfg.VirtualNodes*len(c)+len(c))
	for i := range c {
		n := max(int(math.Round(perUnit*float64(c[i].weight()))), 1)
		base := hashString(c[i].ID)
		for r := range n {
			points = append(points, ringPoint{hash: mix64(base + uint64(r)*0x9e3779b97f4a7c15), owner: int32(i)})
		}
	}
	// Equal hashes are ordered by ID, not by position, so the ring does
	// not depend on the order of the candidates.
	slices.SortFunc(points, func(a, b ringPoint) int {
		if r := cmp.Compare(a.hash, b.hash); r != 0 {
			return r
		}
		return cmp.Compare(c[a.owner].ID, c[b.owner].ID)
	})
	p := &ringPicker{
		hashes:     make([]uint64, len(points)),
		owners:     make([]int32, len(points)),
		loadPicker: newLoadPicker(c),
		factor:     h.cfg.LoadFactor,
		weightSum:  float64(sum),
	}
	for i, pt := range points {
		p.hashes[i] = pt.hash
		p.owners[i] = pt.owner
	}
	return p
}

type ringPicker struct {
	hashes []uint64
	owners []int32
	loadPicker
	factor    float64
	weightSum float64
}

func (p *ringPicker) Pick(key uint64, attempt int) int {
	pos, _ := slices.BinarySearch(p.hashes, mix64(key))
	if pos == len(p.hashes) {
		pos = 0
	}
	if attempt == 0 && p.factor == 0 {
		return int(p.owners[pos])
	}

	var budget float64 // in-flight requests per unit of weight allowed
	if p.factor > 0 {
		var total int64
		for i := range p.weights {
			total += p.load(i)
		}
		budget = p.factor * float64(total+1) / p.weightSum
	}
	// Attempt k takes the k-th candidate of the ring walk in which the
	// candidates under their load cap come first, so successive attempts
	// pick distinct candidates.
	o, under := p.walk(pos, attempt, budget, true)
	if o >= 0 {
		return o
	}
	if o, _ = p.walk(pos, attempt-under, budget, false); o >= 0 {
		return o
	}
	return int(p.owners[pos])
}

// walk visits the distinct candidates in ring order from pos and returns
// the skip-th of those whose being under their cap equals under, or -1 and
// the number it found. Without bounded loads every candidate is under its
// cap.
func (p *ringPicker) walk(pos, skip int, budget float64, under bool) (owner, found int) {
	n := len(p.weights)
	var small [4]uint64
	seen := small[:]
	if n > 64*len(small) {
		seen = make([]uint64, (n+63)/64)
	}
	distinct := 0
	for j, i := 0, pos; j < len(p.owners) && distinct < n; j++ {
		o := int(p.owners[i])
		if i++; i == len(p.owners) {
			i = 0
		}
		if seen[o/64]&(1<<(o%64)) != 0 {
			continue
		}
		seen[o/64] |= 1 << (o % 64)
		distinct++
		ok := p.factor == 0 || float64(p.load(o)) < math.Ceil(budget*float64(p.weights[o]))
		if ok != under {
			continue
		}
		if found == skip {
			return o, found
		}
		found++
	}
	return -1, found
}

// hashString is 64-bit FNV-1a. It is part of the placement contract of
// ConsistentHash and Pool.PickKey and does not change.
func hashString(s string) uint64 {
	h := uint64(0xcbf29ce484222325)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 0x100000001b3
	}
	return h
}

// mix64 is the SplitMix64 finalizer, a bijection that spreads any input
// over all 64 bits.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
