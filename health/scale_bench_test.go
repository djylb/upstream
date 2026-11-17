package health

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// BenchmarkReschedule measures SetConfig rescheduling 10000 idle targets.
func BenchmarkReschedule(b *testing.B) {
	const n = 10000
	ok := func(context.Context, Target) (Observation, error) { return Observation{}, nil }
	done := make(chan struct{}, n)
	cfg := Config[string]{Options: Options{Interval: time.Hour}, OnResult: func(Report[string]) { done <- struct{}{} }}
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
}
