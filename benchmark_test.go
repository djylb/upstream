package upstream

import (
	"slices"
	"strconv"
	"testing"
)

func benchPool(b *testing.B, bal Balancer, n int) *Pool[string] {
	b.Helper()
	p := New[string](Config{Balancer: bal, Outlier: OutlierConfig{ConsecutiveFailures: 5}})
	b.Cleanup(p.Close)
	ts := make([]Target[string], n)
	for i := range ts {
		ts[i] = Target[string]{ID: "backend-" + strconv.Itoa(i), Value: strconv.Itoa(i), Weight: i%4 + 1}
	}
	if err := p.Sync(ts); err != nil {
		b.Fatal(err)
	}
	return p
}

func sortedBalancers() []string {
	names := make([]string, 0, len(testBalancers()))
	for name := range testBalancers() {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// BenchmarkPick measures one Pick and Release on a pool of 16 backends,
// on one goroutine.
func BenchmarkPick(b *testing.B) {
	for _, name := range sortedBalancers() {
		b.Run(name, func(b *testing.B) {
			p := benchPool(b, testBalancers()[name], 16)
			b.ReportAllocs()
			for b.Loop() {
				l, err := p.PickKey("192.0.2.17")
				if err != nil {
					b.Fatal(err)
				}
				l.Release(Success)
			}
		})
	}
}

// BenchmarkPickParallel measures Pick and Release from GOMAXPROCS
// goroutines at once, with distinct keys.
func BenchmarkPickParallel(b *testing.B) {
	for _, name := range sortedBalancers() {
		b.Run(name, func(b *testing.B) {
			p := benchPool(b, testBalancers()[name], 16)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var key uint64
				for pb.Next() {
					key++
					l, err := p.PickHash(key)
					if err != nil {
						b.Fatal(err)
					}
					l.Release(Success)
				}
			})
		})
	}
}

// BenchmarkBuild measures building a picker for 100 candidates, which a
// pool does on every membership or health change.
func BenchmarkBuild(b *testing.B) {
	c := candidates(slices.Repeat([]int{1, 2, 3, 4}, 25)...)
	for _, name := range sortedBalancers() {
		b.Run(name, func(b *testing.B) {
			bal := testBalancers()[name]
			b.ReportAllocs()
			for b.Loop() {
				bal.Build(c)
			}
		})
	}
}
