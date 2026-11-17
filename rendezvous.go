package upstream

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
)

// RendezvousConfig configures Rendezvous.
type RendezvousConfig struct {
	// Key is the SipHash key that seeds the placement. Every process that
	// should map a key to the same candidate uses the same Key; a secret
	// Key keeps outsiders from choosing keys that land on one candidate.
	// The zero Key is valid.
	Key [16]byte
}

// Rendezvous returns a Balancer that maps keys to candidates by
// highest-random-weight (rendezvous) hashing.
//
// For a key k and a candidate with ID id and weight w, the score is
// -w / ln(u), where u in (0, 1) comes from the top 53 bits of
// SipHash-2-4(Key, k ‖ id). The candidate with the highest score wins, and
// equal scores go to the smaller ID, so the choice depends only on the key,
// the IDs, the weights and cfg.Key, never on the order of the candidates or
// on the process. A candidate wins a share of the keys proportional to its
// weight; adding or removing a candidate moves only the keys that go to or
// come from that candidate, and changing one weight moves keys only to or
// from that candidate.
//
// Pick hashes the key it is given again, so any 64-bit key works. A first
// pick costs one hash per candidate and does not allocate. Retries
// (attempts above 0) rank all candidates, O(n log n), and return the next
// best candidates in score order, the ones the key
// would move to if the earlier ones left; attempts past the last candidate
// start over. Unlike ConsistentHash, nothing is precomputed beyond the
// IDs, and Update prepares only the candidates that changed. Its Pickers
// are Rankers, so a Pool that walks past the first choice ranks once.
func Rendezvous(cfg RendezvousConfig) Balancer { return rendezvous{cfg: cfg} }

type rendezvous struct{ cfg RendezvousConfig }

func (rendezvous) String() string { return "rendezvous" }

// rendezvousMember is a candidate's ID prepared for hashing: the message is
// the 8-byte key followed by the ID, so the ID's whole 8-byte blocks, its
// tail and the length byte of SipHash's last block are fixed per candidate.
type rendezvousMember struct {
	id     string
	words  []uint64
	last   uint64
	weight float64
}

func (r rendezvous) Build(c []Candidate) Picker {
	p := r.newPicker(make([]rendezvousMember, len(c)))
	for i := range c {
		p.members[i] = newRendezvousMember(&c[i])
	}
	p.setEqual(c)
	return p
}

// Update copies the prepared IDs of the candidates that d keeps and
// prepares only the added ones; without d it builds.
func (r rendezvous) Update(prev Picker, c []Candidate, d *Delta) Picker {
	p := r.newPicker(nil)
	old, ok := prev.(*rendezvousPicker)
	if !ok || d == nil || old.k0 != p.k0 || old.k1 != p.k1 {
		return r.Build(c)
	}
	p.members = splice(old.members, d, func(i int) rendezvousMember { return newRendezvousMember(&c[i]) })
	p.setEqual(c)
	return p
}

func (r rendezvous) newPicker(members []rendezvousMember) *rendezvousPicker {
	return &rendezvousPicker{
		k0:      binary.LittleEndian.Uint64(r.cfg.Key[:8]),
		k1:      binary.LittleEndian.Uint64(r.cfg.Key[8:]),
		members: members,
	}
}

func newRendezvousMember(c *Candidate) rendezvousMember {
	id := c.ID
	m := rendezvousMember{id: id, weight: float64(c.weight())}
	for len(id) >= 8 {
		m.words = append(m.words, leUint64(id))
		id = id[8:]
	}
	for j := len(id) - 1; j >= 0; j-- {
		m.last = m.last<<8 | uint64(id[j])
	}
	m.last |= uint64(8+len(c.ID)) << 56
	return m
}

// setEqual records whether all weights are equal, so that the hash alone
// orders the candidates.
func (p *rendezvousPicker) setEqual(c []Candidate) {
	p.equal = true
	for i := range c {
		if c[i].weight() != c[0].weight() {
			p.equal = false
			return
		}
	}
}

func leUint64(s string) uint64 {
	_ = s[7]
	return uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24 |
		uint64(s[4])<<32 | uint64(s[5])<<40 | uint64(s[6])<<48 | uint64(s[7])<<56
}

type rendezvousPicker struct {
	members []rendezvousMember
	k0, k1  uint64
	equal   bool // all weights equal: the hash alone orders the candidates
}

// hash is SipHash-2-4 of the message key ‖ ID of member m.
func (p *rendezvousPicker) hash(key uint64, m *rendezvousMember) uint64 {
	v0 := p.k0 ^ 0x736f6d6570736575
	v1 := p.k1 ^ 0x646f72616e646f6d
	v2 := p.k0 ^ 0x6c7967656e657261
	v3 := p.k1 ^ 0x7465646279746573
	block := func(w uint64) {
		v3 ^= w
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= w
	}
	block(key)
	for _, w := range m.words {
		block(w)
	}
	block(m.last)
	v2 ^= 0xff
	for range 4 {
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	}
	return v0 ^ v1 ^ v2 ^ v3
}

func sipRound(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 += v1
	v1 = bits.RotateLeft64(v1, 13)
	v1 ^= v0
	v0 = bits.RotateLeft64(v0, 32)
	v2 += v3
	v3 = bits.RotateLeft64(v3, 16)
	v3 ^= v2
	v0 += v3
	v3 = bits.RotateLeft64(v3, 21)
	v3 ^= v0
	v2 += v1
	v1 = bits.RotateLeft64(v1, 17)
	v1 ^= v2
	v2 = bits.RotateLeft64(v2, 32)
	return v0, v1, v2, v3
}

// score is -w / ln(u) for u in (0, 1) taken from the top 53 bits of h,
// which float64 holds exactly. With equal weights it is the 53 bits
// themselves, which order candidates as the score would, without the
// logarithm.
func (p *rendezvousPicker) score(h uint64, w float64) float64 {
	if p.equal {
		return float64(h >> 11)
	}
	return -w / math.Log((float64(h>>11)+0.5)/(1<<53))
}

// beats reports whether (score a, member i) ranks before (score b, member
// j): higher score first, then smaller ID, then lower index.
func (p *rendezvousPicker) beats(a float64, i int, b float64, j int) bool {
	if a != b {
		return a > b
	}
	if x, y := p.members[i].id, p.members[j].id; x != y {
		return x < y
	}
	return i < j
}

func (p *rendezvousPicker) Pick(key uint64, attempt int) int {
	n := len(p.members)
	if attempt < 0 {
		attempt = 0
	}
	attempt %= n
	if attempt == 0 {
		best, bestScore := 0, 0.0
		for i := range p.members {
			m := &p.members[i]
			s := p.score(p.hash(key, m), m.weight)
			if i == 0 || p.beats(s, i, bestScore, best) {
				best, bestScore = i, s
			}
		}
		return best
	}
	var buf [rankStack]rankEntry
	return p.ranked(key, buf[:0])[attempt].index
}

// Rank writes the candidates in score order, as Pick returns them for
// attempts 0 to n-1: one hash per candidate and a sort, O(n log n), where
// calling Pick for every attempt would cost O(n²) hashes. It allocates only
// for more than rankStack candidates.
func (p *rendezvousPicker) Rank(key uint64, dst []int) {
	var buf [rankStack]rankEntry
	for i, r := range p.ranked(key, buf[:0]) {
		dst[i] = r.index
	}
}

// rankEntry is a candidate's score for one key.
type rankEntry struct {
	id    string
	score float64
	index int
}

// rankOrder orders entries as beats does. It captures nothing, so sorting
// does not allocate a closure.
func rankOrder(a, b rankEntry) int {
	switch {
	case a.score != b.score:
		if a.score > b.score {
			return -1
		}
		return 1
	case a.id != b.id:
		if a.id < b.id {
			return -1
		}
		return 1
	}
	return a.index - b.index
}

// rankStack is the number of candidates ranked in a buffer on the stack;
// larger tiers allocate a buffer of their size for each ranking, which the
// O(n log n) ranking outweighs.
const rankStack = 64

// ranked returns the candidates sorted by rank for key, in buf if it has
// room.
func (p *rendezvousPicker) ranked(key uint64, buf []rankEntry) []rankEntry {
	r := slices.Grow(buf, len(p.members))[:len(p.members)]
	for i := range p.members {
		m := &p.members[i]
		r[i] = rankEntry{id: m.id, score: p.score(p.hash(key, m), m.weight), index: i}
	}
	slices.SortFunc(r, rankOrder)
	return r
}
