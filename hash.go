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
// not allocate, including retries. The Balancer is an
// Updater: when members come and go, a Pool updates the ring instead of
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
	if len(c) > math.MaxInt32 || len(c) > 0 && h.cfg.VirtualNodes > math.MaxInt/len(c) {
		panic("upstream: ring size exceeds index range")
	}
	for i := range c {
		sum += c[i].weight()
	}
	perUnit := float64(h.cfg.VirtualNodes) * float64(len(c)) / float64(sum)
	counts = make([]int32, len(c))
	for i := range c {
		points := max(math.Round(perUnit*float64(c[i].weight())), 1)
		if points > math.MaxInt32 {
			panic("upstream: ring size exceeds index range")
		}
		counts[i] = int32(points)
	}
	return counts, sum
}

// appendPoints appends the ring points of candidate i.
func appendPoints(points []ringPoint, id string, n int32, i int) []ringPoint {
	base := hashString(id)
	for r := range n {
		points = append(points, ringPoint{hash: mix64(base + uint64(r)*0x9e3779b97f4a7c15), owner: i32(i)})
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
	capacity := 0
	for _, n := range counts {
		if int(n) > math.MaxInt-capacity {
			panic("upstream: ring size exceeds index range")
		}
		capacity += int(n)
	}
	points := make([]ringPoint, 0, capacity)
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

// Update keeps the ring points of the candidates whose ID and number of
// points did not change, and merges in the points of the others, in time
// linear in the ring plus the sorting of the new points. With a Delta it
// matches candidates by position, without it by ID. It falls back to Build
// when most points change, as with unequal weights after a member joins or
// leaves, which changes every candidate's number of points.
func (h consistentHash) Update(prev Picker, c []Candidate, d *Delta) Picker {
	old, ok := prev.(*ringPicker)
	if !ok || old.vnodes != h.cfg.VirtualNodes {
		return h.Build(c)
	}
	counts, sum := h.ringCounts(c)
	remap := make([]int32, len(old.ids)) // old owner to new owner, or -1
	for j := range remap {
		remap[j] = -1
	}
	from := make([]int32, len(c)) // new candidate to old, or -1
	for i := range from {
		from[i] = -1
	}
	if d != nil {
		d.Remap(len(c), func(o, n int) { from[n] = i32(o) })
	} else {
		oldIndex := make(map[string]int32, len(old.ids))
		for j, id := range old.ids {
			oldIndex[id] = int32(j)
		}
		for i := range c {
			if j, ok := oldIndex[c[i].ID]; ok {
				from[i] = j
			}
		}
	}
	var fresh []ringPoint
	changed := 0
	for i := range c {
		if j := from[i]; j >= 0 && old.counts[j] == counts[i] {
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
	p.gap = make([]int, len(owners))
	last := make([]int, len(c))
	for i, o := range owners {
		last[o] = i - len(owners)
	}
	for i, o := range owners {
		p.gap[i] = i - last[o]
		last[o] = i
	}
	for i := range c {
		p.ids[i] = c[i].ID
	}
	return p
}

type ringPicker struct {
	gap    []int // distance to previous point of the same owner, around the ring
	hashes []uint64
	owners []int32
	loadPicker
	ids       []string // for Update
	counts    []int32  // ring points per candidate, for Update
	factor    float64
	weightSum float64
	vnodes    int
}

func (p *ringPicker) Pick(key uint64, attempt int) int {
	pos := p.position(key)
	if attempt == 0 && p.factor == 0 {
		return int(p.owners[pos])
	}
	result := int(p.owners[pos])
	p.visit(pos, func(owner int) bool {
		if attempt <= 0 {
			result = owner
			return true
		}
		attempt--
		return false
	})
	return result
}

func (p *ringPicker) position(key uint64) int {
	pos, _ := slices.BinarySearch(p.hashes, mix64(key))
	if pos == len(p.hashes) {
		return 0
	}
	return pos
}

// visit considers each owner once, in ring order within each load class.
func (p *ringPicker) visit(pos int, accept func(int) bool) {
	var budget float64
	if p.factor > 0 {
		var total float64
		for i := range p.weights {
			total += float64(p.load(i))
		}
		budget = p.factor * (total + 1) / p.weightSum
	}
	for pass := range 2 {
		for j, i := 0, pos; j < len(p.owners); j++ {
			o := int(p.owners[i])
			if j < p.gap[i] && p.under(o, budget) == (pass == 0) && accept(o) {
				return
			}
			i++
			if i == len(p.owners) {
				i = 0
			}
		}
		if p.factor == 0 {
			return
		}
	}
}

// under reports whether candidate o carries less than its cap. Without
// bounded loads every candidate is under its cap.
func (p *ringPicker) under(o int, budget float64) bool {
	return p.factor == 0 || float64(p.load(o)) < math.Ceil(budget*float64(p.weights[o]))
}

// hashString is 64-bit FNV-1a. It is part of the placement contract of
// ConsistentHash and Pool.PickKey and does not change.
func hashString(s string) uint64 {
	h := uint64(0xcbf29ce484222325)
	for i := range len(s) {
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
