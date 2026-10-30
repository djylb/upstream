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
// not allocate, except retries (attempts above 0) on rings of more than
// 256 candidates, which need a scratch bitset. The Balancer is a
// Rebuilder: when members come and go, a Pool updates the ring instead of
// building it again.
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

// ringCounts returns the number of ring points of each candidate and the
// sum of their weights.
func (h consistentHash) ringCounts(c []Candidate) (counts []int32, sum int64) {
	for i := range c {
		sum += c[i].weight()
	}
	perUnit := float64(h.cfg.VirtualNodes) * float64(len(c)) / float64(sum)
	counts = make([]int32, len(c))
	for i := range c {
		counts[i] = int32(min(max(math.Round(perUnit*float64(c[i].weight())), 1), math.MaxInt32))
	}
	return counts, sum
}

// appendPoints appends the ring points of candidate i.
func appendPoints(points []ringPoint, id string, n int32, i int) []ringPoint {
	base := hashString(id)
	for r := range n {
		points = append(points, ringPoint{hash: mix64(base + uint64(r)*0x9e3779b97f4a7c15), owner: int32(i)})
	}
	return points
}

// pointOrder orders ring points by hash, and equal hashes by ID rather
// than by position, so the ring does not depend on the order of the
// candidates.
func pointOrder(c []Candidate) func(a, b ringPoint) int {
	return func(a, b ringPoint) int {
		if r := cmp.Compare(a.hash, b.hash); r != 0 {
			return r
		}
		return cmp.Compare(c[a.owner].ID, c[b.owner].ID)
	}
}

func (h consistentHash) Build(c []Candidate) Picker {
	counts, sum := h.ringCounts(c)
	points := make([]ringPoint, 0, h.cfg.VirtualNodes*len(c)+len(c))
	for i := range c {
		points = appendPoints(points, c[i].ID, counts[i], i)
	}
	slices.SortFunc(points, pointOrder(c))
	hashes, owners := make([]uint64, len(points)), make([]int32, len(points))
	for i, pt := range points {
		hashes[i], owners[i] = pt.hash, pt.owner
	}
	return h.picker(c, counts, sum, hashes, owners)
}

// Rebuild keeps the ring points of the candidates whose ID and number of
// points did not change, and merges in the points of the others, in time
// linear in the ring plus the sorting of the new points. It falls back to
// Build when most points change, as with unequal weights after a member
// joins or leaves, which changes every candidate's number of points.
func (h consistentHash) Rebuild(prev Picker, c []Candidate) Picker {
	old, ok := prev.(*ringPicker)
	if !ok || old.vnodes != h.cfg.VirtualNodes {
		return h.Build(c)
	}
	counts, sum := h.ringCounts(c)
	oldIndex := make(map[string]int32, len(old.ids))
	for j, id := range old.ids {
		oldIndex[id] = int32(j)
	}
	remap := make([]int32, len(old.ids)) // old owner to new owner, or -1
	for j := range remap {
		remap[j] = -1
	}
	var fresh []ringPoint
	changed := 0
	for i := range c {
		if j, ok := oldIndex[c[i].ID]; ok && old.counts[j] == counts[i] {
			remap[j] = int32(i)
			continue
		}
		changed += int(counts[i])
		if changed > len(old.hashes)/2 {
			return h.Build(c)
		}
		fresh = appendPoints(fresh, c[i].ID, counts[i], i)
	}
	order := pointOrder(c)
	slices.SortFunc(fresh, order)
	n := len(old.hashes) + len(fresh)
	hashes, owners := make([]uint64, 0, n), make([]int32, 0, n)
	for k, hash := range old.hashes {
		o := remap[old.owners[k]]
		if o < 0 {
			continue
		}
		pt := ringPoint{hash: hash, owner: o}
		for len(fresh) > 0 && order(fresh[0], pt) < 0 {
			hashes, owners = append(hashes, fresh[0].hash), append(owners, fresh[0].owner)
			fresh = fresh[1:]
		}
		hashes, owners = append(hashes, hash), append(owners, o)
	}
	for _, pt := range fresh {
		hashes, owners = append(hashes, pt.hash), append(owners, pt.owner)
	}
	return h.picker(c, counts, sum, hashes, owners)
}

func (h consistentHash) picker(c []Candidate, counts []int32, sum int64, hashes []uint64, owners []int32) *ringPicker {
	p := &ringPicker{
		hashes:     hashes,
		owners:     owners,
		ids:        make([]string, len(c)),
		counts:     counts,
		loadPicker: newLoadPicker(c),
		factor:     h.cfg.LoadFactor,
		weightSum:  float64(sum),
		vnodes:     h.cfg.VirtualNodes,
	}
	for i := range c {
		p.ids[i] = c[i].ID
	}
	return p
}

type ringPicker struct {
	hashes []uint64
	owners []int32
	loadPicker
	ids       []string // for Rebuild
	counts    []int32  // ring points per candidate, for Rebuild
	factor    float64
	weightSum float64
	vnodes    int
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
	if attempt == 0 {
		// The first candidate under its cap on the ring, which needs no
		// record of the candidates seen. The caps add up to more than the
		// load, so there is one.
		for j, i := 0, pos; j < len(p.owners); j++ {
			if o := int(p.owners[i]); p.under(o, budget) {
				return o
			}
			if i++; i == len(p.owners) {
				i = 0
			}
		}
		return int(p.owners[pos])
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

// under reports whether candidate o carries less than its cap. Without
// bounded loads every candidate is under its cap.
func (p *ringPicker) under(o int, budget float64) bool {
	return p.factor == 0 || float64(p.load(o)) < math.Ceil(budget*float64(p.weights[o]))
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
		if p.under(o, budget) != under {
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
