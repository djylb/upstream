package upstream

import (
	"strconv"
	"testing"
)

// BenchmarkAcquire measures Acquire and Release by ID at both ends.
func BenchmarkAcquire(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := scalePool(b, nil, n, 0)
			id := "backend-" + strconv.Itoa(n/2)
			b.ReportAllocs()
			for b.Loop() {
				l, err := p.Acquire(id)
				if err != nil {
					b.Fatal(err)
				}
				l.Release(Success)
			}
		})
	}
}
