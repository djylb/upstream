package upstream

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
)

func TestReviewFallbackJSONRoundTrip(t *testing.T) {
	type cfg struct{ Fallback Fallback }
	b, err := json.Marshal(cfg{FallbackAll})
	if err != nil {
		t.Fatal(err)
	}
	var c cfg
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatalf("%s does not decode: %v", b, err)
	}
}

func TestReviewBudgetOvershoot(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 2, Budget: 1e-9, MinRetries: 1}})
	defer p.Close()
	var ts []Target[string]
	for i := range 4 {
		ts = append(ts, Target[string]{ID: string('a' + rune(i))})
	}
	_ = p.Sync(ts)
	const n = 64
	var granted atomic.Int32
	var start, wg sync.WaitGroup
	start.Add(1)
	for range n {
		wg.Go(func() {
			a := p.Attempts("")
			l, err := a.Next()
			if err != nil {
				return
			}
			l.Release(Failure)
			start.Wait()
			if l2, err := a.Next(); err == nil {
				granted.Add(1)
				l2.Release(Success)
			}
		})
	}
	start.Done()
	wg.Wait()
	if g := granted.Load(); g > 1 {
		t.Fatalf("budget of 1 retry granted %d", g)
	}
}

func TestReviewAllocs(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 3, Budget: 0.5}})
	defer p.Close()
	_ = p.Sync([]Target[string]{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	n := testing.AllocsPerRun(1000, func() {
		a := p.Attempts("k")
		l, _ := a.Next()
		l.Release(Failure)
		l, _ = a.Next()
		l.Release(Success)
	})
	fn := func(*Backend[string]) error { return nil }
	m := testing.AllocsPerRun(1000, func() { _ = p.Do(context.Background(), fn) })
	t.Logf("attempts %v, do %v", n, m)
	if n != 0 || m != 0 {
		t.Fatalf("allocs attempts %v do %v", n, m)
	}
}
