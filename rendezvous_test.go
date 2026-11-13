package upstream

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// TestSipHash24Vector checks the reference vector of SipHash-2-4: the key
// 00 01 ... 0f and the 15-byte message 00 01 ... 0e.
func TestSipHash24Vector(t *testing.T) {
	var key [16]byte
	for i := range key {
		key[i] = byte(i)
	}
	p := Rendezvous(RendezvousConfig{Key: key}).Build([]Candidate{{ID: "\x08\x09\x0a\x0b\x0c\x0d\x0e"}}).(*rendezvousPicker)
	const want uint64 = 0xa129ca6149be45e5
	if got := p.hash(binary.LittleEndian.Uint64([]byte{0, 1, 2, 3, 4, 5, 6, 7}), &p.members[0]); got != want {
		t.Fatalf("hash = %#x, want %#x", got, want)
	}
	// The empty ID: the 8-byte message 00..07.
	p = Rendezvous(RendezvousConfig{Key: key}).Build([]Candidate{{ID: ""}}).(*rendezvousPicker)
	if got := p.hash(binary.LittleEndian.Uint64([]byte{0, 1, 2, 3, 4, 5, 6, 7}), &p.members[0]); got != 0x93f5f5799a932462 {
		t.Fatalf("hash of 8 bytes = %#x", got)
	}
}

func rendezvousOwners(b Balancer, c []Candidate, keys int) []string {
	p := b.Build(c)
	owners := make([]string, keys)
	for k := range owners {
		owners[k] = c[p.Pick(uint64(k)*0x9e3779b97f4a7c15, 0)].ID
	}
	return owners
}

func TestRendezvousStability(t *testing.T) {
	const keys = 20000
	h := Rendezvous(RendezvousConfig{})
	base := candidates(1, 2, 1, 3, 1, 1, 2, 1, 1, 1)
	before := rendezvousOwners(h, base, keys)

	shuffled := slices.Clone(base)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for k, id := range rendezvousOwners(h, shuffled, keys) {
		if before[k] != id {
			t.Fatalf("key %d moved from %s to %s when candidates were reordered", k, before[k], id)
		}
	}

	added := append(slices.Clone(base), Candidate{ID: "new", Weight: 2})
	for k, id := range rendezvousOwners(h, added, keys) {
		if id != before[k] && id != "new" {
			t.Fatalf("key %d moved from %s to %s, not to the new candidate", k, before[k], id)
		}
	}

	removed := slices.Delete(slices.Clone(base), 3, 4)
	for k, id := range rendezvousOwners(h, removed, keys) {
		if id != before[k] && before[k] != base[3].ID {
			t.Fatalf("key %d moved from %s to %s although %s was removed", k, before[k], id, base[3].ID)
		}
	}

	// A weight change moves keys only to or from that candidate.
	heavier := slices.Clone(base)
	heavier[5].Weight = 5
	for k, id := range rendezvousOwners(h, heavier, keys) {
		if id != before[k] && id != base[5].ID {
			t.Fatalf("key %d moved from %s to %s when %s grew", k, before[k], id, base[5].ID)
		}
	}
}

func TestRendezvousWeights(t *testing.T) {
	for _, weights := range [][]int{{1, 3, 6}, {1, 1, 1, 1}, {1, 100}} {
		p := Rendezvous(RendezvousConfig{}).Build(candidates(weights...))
		assertShares(t, countPicks(p, len(weights), 200000), weights, 0.01)
	}
}

func TestRendezvousKey(t *testing.T) {
	c := candidates(1, 1, 1, 1, 1, 1, 1, 1)
	a := Rendezvous(RendezvousConfig{}).Build(c)
	b := Rendezvous(RendezvousConfig{Key: [16]byte{7}}).Build(c)
	same := Rendezvous(RendezvousConfig{Key: [16]byte{7}}).Build(c)
	differ := 0
	for k := range uint64(2000) {
		if a.Pick(k, 0) != b.Pick(k, 0) {
			differ++
		}
		if b.Pick(k, 0) != same.Pick(k, 0) {
			t.Fatalf("key %d: equal keys disagree", k)
		}
	}
	if differ < 1000 {
		t.Fatalf("different SipHash keys agreed on %d of 2000 keys", 2000-differ)
	}
}

func TestRendezvousRetriesFollowRank(t *testing.T) {
	for _, weights := range [][]int{{1, 1, 1, 1, 1}, {1, 2, 3, 4, 5, 6, 7}} {
		c := candidates(weights...)
		p := Rendezvous(RendezvousConfig{}).Build(c)
		for k := range uint64(200) {
			seen := map[int]bool{}
			var order []int
			for a := range len(c) {
				i := p.Pick(k, a)
				if seen[i] {
					t.Fatalf("key %d attempt %d repeated candidate %d", k, a, i)
				}
				seen[i] = true
				order = append(order, i)
			}
			if p.Pick(k, len(c)) != order[0] {
				t.Fatalf("key %d: attempts do not start over", k)
			}
			// Removing the best candidate promotes the second best.
			rest := slices.Delete(slices.Clone(c), order[0], order[0]+1)
			q := Rendezvous(RendezvousConfig{}).Build(rest)
			if got := rest[q.Pick(k, 0)].ID; got != c[order[1]].ID {
				t.Fatalf("key %d: after the best left, %s, want %s", k, got, c[order[1]].ID)
			}
		}
	}
}

func TestRendezvousTiesGoToSmallerID(t *testing.T) {
	// Equal IDs hash alike; the smaller index then wins deterministically.
	c := []Candidate{{ID: "x"}, {ID: "x"}, {ID: "x"}}
	p := Rendezvous(RendezvousConfig{}).Build(c)
	for a := range 3 {
		if got := p.Pick(1, a); got != a {
			t.Fatalf("attempt %d picked %d", a, got)
		}
	}
}

func TestRendezvousIDLengths(t *testing.T) {
	// IDs around the 8-byte block boundaries all hash and pick in range.
	for n := range 40 {
		id := string(make([]byte, n))
		p := Rendezvous(RendezvousConfig{}).Build([]Candidate{{ID: id}, {ID: id + "x"}, {ID: "t" + strconv.Itoa(n)}})
		if i := p.Pick(uint64(n), 0); i < 0 || i > 2 {
			t.Fatalf("length %d picked %d", n, i)
		}
	}
}

func TestRendezvousPickDoesNotAllocate(t *testing.T) {
	p := Rendezvous(RendezvousConfig{}).Build(candidates(1, 2, 3, 4, 5, 6, 7, 8))
	var key uint64
	if n := testing.AllocsPerRun(100, func() { key++; p.Pick(key, 0); p.Pick(key, 3) }); n != 0 {
		t.Fatalf("Pick allocates %v times", n)
	}
}

func BenchmarkRendezvousPick(b *testing.B) {
	for _, n := range []int{8, 64, 256} {
		for _, name := range []string{"equal", "weighted"} {
			b.Run(strconv.Itoa(n)+"/"+name, func(b *testing.B) {
				c := make([]Candidate, n)
				for i := range c {
					c[i] = Candidate{ID: "backend-" + strconv.Itoa(i), Weight: 1}
					if name == "weighted" {
						c[i].Weight = i%4 + 1
					}
				}
				p := Rendezvous(RendezvousConfig{}).Build(c)
				b.ReportAllocs()
				var key uint64
				for b.Loop() {
					key++
					p.Pick(key, 0)
				}
			})
		}
	}
}
