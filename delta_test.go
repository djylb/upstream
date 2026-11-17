package upstream

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// randomDelta returns a random Delta from a list of n elements and the
// length of the new list.
func randomDelta(rng *rand.Rand, n int) (*Delta, int) {
	d := new(Delta)
	for i := range n {
		if rng.IntN(4) == 0 {
			d.Removed = append(d.Removed, i)
		}
	}
	m := n - len(d.Removed) + rng.IntN(5)
	for j := range m {
		if len(d.Added) < m-(n-len(d.Removed)) && rng.IntN(max(m-j, 1)) < m-(n-len(d.Removed))-len(d.Added) {
			d.Added = append(d.Added, j)
		}
	}
	return d, m
}

// naiveApply applies d to old the slow way.
func naiveApply(old []string, d *Delta, newLen int) []string {
	var kept []string
	for i, x := range old {
		if !slices.Contains(d.Removed, i) {
			kept = append(kept, x)
		}
	}
	out := make([]string, 0, newLen)
	for j := range newLen {
		if slices.Contains(d.Added, j) {
			out = append(out, "new"+strconv.Itoa(j))
		} else {
			out, kept = append(out, kept[0]), kept[1:]
		}
	}
	return out
}

func TestSpliceAndRemap(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 9))
	for range 2000 {
		n := rng.IntN(12)
		old := make([]string, n)
		for i := range old {
			old[i] = "old" + strconv.Itoa(i)
		}
		d, m := randomDelta(rng, n)
		want := naiveApply(old, d, m)
		got := splice(old, d, func(j int) string { return "new" + strconv.Itoa(j) })
		if !slices.Equal(got, want) {
			t.Fatalf("splice %v by %+v: %v, want %v", old, d, got, want)
		}
		pairs := 0
		d.Remap(m, func(o, j int) {
			pairs++
			if want[j] != old[o] {
				t.Fatalf("remap %+v: %d -> %d, but %q is %q", d, o, j, want[j], old[o])
			}
		})
		if pairs != n-len(d.Removed) {
			t.Fatalf("remap %+v: %d pairs", d, pairs)
		}
	}
}

func TestRendezvousUpdateMatchesBuild(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	bal := Rendezvous(RendezvousConfig{Key: [16]byte{9}}).(Updater)
	for range 300 {
		n := 1 + rng.IntN(10)
		ids := make([]string, n)
		for i := range ids {
			ids[i] = "backend-" + strconv.Itoa(rng.IntN(1000))
		}
		d, m := randomDelta(rng, n)
		if m == 0 {
			continue
		}
		ws := func(xs []string) []Candidate {
			c := make([]Candidate, len(xs))
			for i, x := range xs {
				c[i] = Candidate{ID: x, Weight: 1 + len(x)%3}
			}
			return c
		}
		next := naiveApply(ids, d, m)
		prev := bal.Build(ws(ids))
		got, want := bal.Update(prev, ws(next), d), bal.Build(ws(next))
		for range 20 {
			key := rng.Uint64()
			for a := range m {
				if g, w := got.Pick(key, a), want.Pick(key, a); g != w {
					t.Fatalf("attempt %d: update picks %d, build %d", a, g, w)
				}
			}
		}
	}
}

// contractBalancer checks every Delta a pool passes against the candidates
// it built last, for a pool of one tier.
type contractBalancer struct {
	t              *testing.T
	last           []Candidate
	deltas, nilDel int
	removed, added int
}

func (b *contractBalancer) Build(c []Candidate) Picker {
	b.last = slices.Clone(c)
	return RoundRobin().Build(c)
}

func (b *contractBalancer) Update(_ Picker, c []Candidate, d *Delta) Picker {
	if d == nil {
		b.nilDel++
	} else {
		b.deltas++
		b.removed += len(d.Removed)
		b.added += len(d.Added)
		ids := func(cs []Candidate) []string {
			s := make([]string, len(cs))
			for i, x := range cs {
				s[i] = x.ID + "/" + strconv.Itoa(x.Weight)
			}
			return s
		}
		want := ids(c)
		old := ids(b.last)
		got := naiveApply(old, d, len(c))
		for _, j := range d.Added {
			got[j] = want[j]
		}
		if !slices.Equal(got, want) {
			b.t.Fatalf("delta %+v from %v gives %v, candidates are %v", d, old, got, want)
		}
	}
	return b.Build(c)
}

// TestPoolDeltas drives a pool through random state and membership
// changes and checks that every Delta it passes is right, and that state
// changes pass small Deltas.
func TestPoolDeltas(t *testing.T) {
	bal := &contractBalancer{t: t}
	p := New[int](Config{Balancer: bal})
	defer p.Close()
	rng := rand.New(rand.NewPCG(5, 6))
	ids := make([]string, 30)
	ts := make([]Target[int], len(ids))
	for i := range ids {
		ids[i] = strconv.Itoa(i)
		ts[i] = Target[int]{ID: ids[i]}
	}
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	for range 2000 {
		id := ids[rng.IntN(len(ids))]
		b, ok := p.Backend(id)
		switch rng.IntN(9) {
		case 0:
			if ok {
				p.MarkOffline(b, nil)
			}
		case 1:
			if ok {
				p.MarkOnline(b)
			}
		case 2:
			p.Drain(id)
		case 3:
			p.Resume(id)
		case 4:
			if ok {
				p.ReportCheck(b, []Health{HealthUnknown, HealthUp, HealthDown}[rng.IntN(3)], CheckResult{})
			}
		case 5:
			_ = p.Put(Target[int]{ID: id, Weight: 1 + rng.IntN(3)})
		case 6:
			p.Remove(id)
		case 7:
			_ = p.Put(Target[int]{ID: id})
		case 8:
			if rng.IntN(20) == 0 {
				rng.Shuffle(len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] })
				_ = p.Sync(ts)
			}
		}
	}
	if bal.deltas < 300 || bal.nilDel > bal.deltas/10 {
		t.Fatalf("%d updates with a Delta, %d without", bal.deltas, bal.nilDel)
	}
	if avg := float64(bal.removed+bal.added) / float64(bal.deltas); avg > 3 {
		t.Fatalf("Deltas change %.1f candidates on average", avg)
	}
}

// TestApplyPendingSeveral checks applyPending with several members leaving
// and joining in one update, as when commits coalesce during a build.
func TestApplyPendingSeveral(t *testing.T) {
	bs := make([]*Backend[int], 8)
	for i := range bs {
		bs[i] = &Backend[int]{core: new(core), id: strconv.Itoa(i), slot: i}
	}
	in := map[int]bool{0: true, 1: true, 3: true, 5: true, 6: true}
	var list []*Backend[int]
	for i, b := range bs {
		if in[i] {
			list = append(list, b)
		}
	}
	// 1 and 5 leave, 2, 4 and 7 join.
	in[1], in[5], in[2], in[4], in[7] = false, false, true, true, true
	pend := []*Backend[int]{bs[1], bs[2], bs[4], bs[5], bs[7]}
	got, d := applyPending(list, pend, func(b *Backend[int]) bool { return in[b.slot] })
	var want []*Backend[int]
	for i, b := range bs {
		if in[i] {
			want = append(want, b)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("list %v, want %v", got, want)
	}
	names := func(l []*Backend[int]) []string {
		s := make([]string, len(l))
		for i, b := range l {
			s[i] = b.id
		}
		return s
	}
	applied := naiveApply(names(list), d, len(want))
	for _, j := range d.Added {
		applied[j] = want[j].id
	}
	if !slices.Equal(applied, names(want)) {
		t.Fatalf("delta %+v gives %v, want %v", d, applied, names(want))
	}
}

// TestDiffFromEmpty checks that a list with no predecessor gets no Delta:
// there is no Picker to update, and a small pool pays nothing for one.
func TestDiffFromEmpty(t *testing.T) {
	if d := diffLists(nil, []*Backend[int]{{slot: -1}}); d != nil {
		t.Fatalf("delta %+v from an empty list", d)
	}
}
