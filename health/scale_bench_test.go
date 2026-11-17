package health

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// BenchmarkNewSmallChecker measures the memory of a Checker of 3 targets,
// before their first check.
func BenchmarkNewSmallChecker(b *testing.B) {
	never := func(ctx context.Context, _ Target) (Observation, error) {
		<-ctx.Done()
		return Observation{}, ctx.Err()
	}
	b.ReportAllocs()
	for b.Loop() {
		c := New[string, string](context.Background(), Config[string]{Options: Options{Interval: time.Hour}, Concurrency: 1})
		for _, k := range []string{"a", "b", "c"} {
			c.Put(k, Target{Probe: never})
		}
		c.Close()
		<-c.Done()
	}
}

// BenchmarkReschedule measures SetConfig rescheduling idle targets, at
// both ends.
func BenchmarkReschedule(b *testing.B) {
	for _, n := range []int{10, 1 << 20} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			ok := func(context.Context, Target) (Observation, error) { return Observation{}, nil }
			done := make(chan struct{}, n)
			cfg := Config[string]{Options: Options{Interval: time.Hour}, Concurrency: 64, OnResult: func(Report[string]) { done <- struct{}{} }}
			c := New[string, string](b.Context(), cfg)
			b.Cleanup(func() { c.Close(); <-c.Done() })
			for i := range n {
				c.Put(strconv.Itoa(i), Target{Probe: ok})
			}
			for range n {
				<-done
			}
			cfg.OnResult = nil
			b.ReportAllocs()
			for b.Loop() {
				c.SetConfig(cfg)
			}
		})
	}
}
