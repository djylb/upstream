package upstream

import (
	"encoding/binary"
	"math"
	"math/bits"
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
// Pick hashes the key it is given again, so any 64-bit key works. A pick
// costs one hash per candidate and does not allocate. Retries (attempts
// above 0) return the next best candidates in score order, the ones the key
// would move to if the earlier ones left; attempts past the last candidate
// start over. Unlike ConsistentHash, nothing is precomputed beyond the
// IDs, so Build is cheap and the Balancer needs no Rebuilder; it suits tiers
// of up to a few hundred candidates.
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
	p := &rendezvousPicker{
		k0:      binary.LittleEndian.Uint64(r.cfg.Key[:8]),
		k1:      binary.LittleEndian.Uint64(r.cfg.Key[8:]),
		members: make([]rendezvousMember, len(c)),
	}
	first := c[0].weight()
	p.equal = true
	for i := range c {
		id := c[i].ID
		m := rendezvousMember{id: id, weight: float64(c[i].weight())}
		for len(id) >= 8 {
			m.words = append(m.words, leUint64(id))
			id = id[8:]
		}
		for j := len(id) - 1; j >= 0; j-- {
			m.last = m.last<<8 | uint64(id[j])
		}
		m.last |= uint64(8+len(c[i].ID)) << 56
		p.members[i] = m
		p.equal = p.equal && c[i].weight() == first
	}
	return p
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
	// Rank 0 is the best candidate; each further rank is the best of those
	// ranked below the previous one.
	best, bestScore := -1, 0.0
	for range attempt + 1 {
		prev, prevScore := best, bestScore
		best = -1
		for i := range p.members {
			m := &p.members[i]
			s := p.score(p.hash(key, m), m.weight)
			if prev >= 0 && !p.beats(prevScore, prev, s, i) {
				continue // ranks above or equal to the previous one
			}
			if best < 0 || p.beats(s, i, bestScore, best) {
				best, bestScore = i, s
			}
		}
	}
	return best
}
