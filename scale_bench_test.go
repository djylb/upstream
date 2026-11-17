package upstream

import (
	"strconv"
	"testing"
)

// BenchmarkPickFullTier measures a pick on a Rendezvous tier of 256
// backends at MaxInFlight 1 in which only one backend has a free slot, so
// the pick walks the key's ranking until it reaches that backend.
func BenchmarkPickFullTier(b *testing.B) {
	const n = 256
	p := New[int](Config{Balancer: Rendezvous(RendezvousConfig{})})
	b.Cleanup(p.Close)
	ts := make([]Target[int], n)
	for i := range ts {
		ts[i] = Target[int]{ID: "backend-" + strconv.Itoa(i), MaxInFlight: 1}
	}
	if err := p.Sync(ts); err != nil {
		b.Fatal(err)
	}
	var held []Lease[int]
	for len(held) < n-1 {
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
}

// BenchmarkRequestManyRetries measures a Request that retries through all
// of n backends.
func BenchmarkRequestManyRetries(b *testing.B) {
	for _, n := range []int{16, 128, 1024} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := New[int](Config{Balancer: RoundRobin(), Retry: RetryConfig{Attempts: n}})
			b.Cleanup(p.Close)
			ts := make([]Target[int], n)
			for i := range ts {
				ts[i] = Target[int]{ID: "backend-" + strconv.Itoa(i)}
			}
			if err := p.Sync(ts); err != nil {
				b.Fatal(err)
			}
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

// BenchmarkStateChange measures taking one backend offline and back on a
// pool of 16 tiers of 1024 backends, with and without slow start off.
func BenchmarkStateChange(b *testing.B) {
	for _, tiers := range []int{1, 16} {
		b.Run("tiers-"+strconv.Itoa(tiers), func(b *testing.B) {
			const n = 16384
			p := New[int](Config{})
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
