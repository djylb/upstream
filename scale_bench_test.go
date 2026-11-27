package upstream

import (
	"strconv"
	"testing"
)

// benchSizes are the two ends a pool must serve: a small router and a
// million backends.
var benchSizes = []int{10, 1 << 20}

func scalePool(b *testing.B, bal Balancer, n, maxInFlight int) *Pool[int] {
	b.Helper()
	p := New[int](Config{Balancer: bal, Retry: RetryConfig{Attempts: n}})
	b.Cleanup(p.Close)
	ts := make([]Target[int], n)
	for i := range ts {
		ts[i] = Target[int]{ID: "backend-" + strconv.Itoa(i), MaxInFlight: maxInFlight, Value: i}
	}
	if err := p.Sync(ts); err != nil {
		b.Fatal(err)
	}
	return p
}

// BenchmarkNewSmallPool measures the memory of a pool of 3 backends.
func BenchmarkNewSmallPool(b *testing.B) {
	ts := []Target[int]{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	b.ReportAllocs()
	for b.Loop() {
		p := New[int](Config{})
		if err := p.Sync(ts); err != nil {
			b.Fatal(err)
		}
		p.Close()
	}
}

// BenchmarkPickSizes measures Pick and Release at both ends.
func BenchmarkPickSizes(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := scalePool(b, nil, n, 0)
			b.ReportAllocs()
			for b.Loop() {
				l, err := p.Pick()
				if err != nil {
					b.Fatal(err)
				}
				l.Release(Success)
			}
		})
	}
}

// BenchmarkPickFullTier measures a pick on a Rendezvous tier at
// MaxInFlight 1 in which only one backend has a free slot, so the pick
// walks the key's ranking until it reaches that backend.
func BenchmarkPickFullTier(b *testing.B) {
	for _, n := range []int{10, 256, 4096} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := scalePool(b, Rendezvous(RendezvousConfig{}), n, 1)
			held := make([]Lease[int], 0, n)
			for range n - 1 {
				l, err := p.Pick()
				if err != nil {
					b.Fatal(err)
				}
				held = append(held, l)
			}
			b.ReportAllocs()
			key := uint64(0)
			for b.Loop() {
				key++
				l, err := p.PickHash(key)
				if err != nil {
					b.Fatal(err)
				}
				l.Release(Success)
			}
			for i := range held {
				held[i].Release(Success)
			}
		})
	}
}

// BenchmarkRequestManyRetries measures a Request that tries every backend.
func BenchmarkRequestManyRetries(b *testing.B) {
	for _, n := range []int{10, 128, 1024} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := scalePool(b, RoundRobin(), n, 0)
			b.ReportAllocs()
			for b.Loop() {
				r := p.Request("")
				for range n {
					l, err := r.Next()
					if err != nil {
						b.Fatal(err)
					}
					l.Release(Failure)
				}
			}
		})
	}
}

// BenchmarkStateChange measures taking one backend offline and back, with
// the default balancer and with Rendezvous, on one tier and on 16.
func BenchmarkStateChange(b *testing.B) {
	for _, bal := range []Balancer{WeightedRoundRobin(), Rendezvous(RendezvousConfig{})} {
		for _, n := range benchSizes {
			for _, tiers := range []int{1, 16} {
				b.Run(bal.(interface{ String() string }).String()+"/"+strconv.Itoa(n)+"/tiers-"+strconv.Itoa(tiers), func(b *testing.B) {
					p := New[int](Config{Balancer: bal})
					b.Cleanup(p.Close)
					ts := make([]Target[int], n)
					for i := range ts {
						ts[i] = Target[int]{ID: "backend-" + strconv.Itoa(i), Priority: i % tiers}
					}
					if err := p.Sync(ts); err != nil {
						b.Fatal(err)
					}
					bk, _ := p.Backend("backend-7")
					b.ReportAllocs()
					for b.Loop() {
						p.MarkOffline(bk, nil)
						p.MarkOnline(bk)
					}
				})
			}
		}
	}
}
