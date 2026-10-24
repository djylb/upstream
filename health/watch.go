package health

import (
	"context"
	"sync"

	"github.com/djylb/upstream"
)

// Watch starts a Checker that follows the membership of pool: every member
// is checked under its ID, with the Target that target returns for it, and
// every result is reported to the pool with Pool.ReportCheck, which takes
// members out of rotation while they are down. The pool then announces
// health transitions as upstream.EventStateChanged events; cfg's callbacks
// also see every report.
//
// Reports are matched to members by generation: a report about a member
// that has since been removed, or re-added under the same ID, never
// reaches the new member.
//
// The Checker belongs to the pool: it closes when the pool closes, and
// closing it earlier ends its subscription to the pool. Its Done channel
// reports when its goroutines have ended.
func Watch[T any](ctx context.Context, pool *upstream.Pool[T], target func(*upstream.Backend[T]) Target, cfg Config[string]) *Checker[string, struct{}] {
	c := New[string, struct{}](ctx, cfg)
	type member struct {
		b   *upstream.Backend[T]
		gen uint64
	}
	var mu sync.Mutex // also held across Set, which never calls back
	members := make(map[string]member)
	c.mu.Lock()
	c.sink = func(r Report[string]) {
		mu.Lock()
		m, ok := members[r.Key]
		mu.Unlock()
		if ok && m.gen == r.Generation {
			pool.ReportCheck(m.b, r.Health, upstream.CheckResult{Time: r.Time, Err: r.Err, Latency: r.Latency})
		}
	}
	c.mu.Unlock()
	cancel := pool.Subscribe(func(ev upstream.Event[T]) {
		id := ev.Backend.ID()
		switch ev.Kind {
		case upstream.EventAdded, upstream.EventUpdated:
			t := target(ev.Backend)
			mu.Lock()
			members[id] = member{b: ev.Backend, gen: c.Set(id, t)}
			mu.Unlock()
		case upstream.EventRemoved:
			mu.Lock()
			delete(members, id)
			mu.Unlock()
			c.Remove(id)
		case upstream.EventStateChanged:
		}
	})
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return c
	}
	c.detach = cancel // ended by shutdown
	c.pending++       // the goroutine below
	c.mu.Unlock()
	go func() {
		c.enter()
		defer c.leave()
		select {
		case <-pool.Closed():
			c.Close()
		case <-c.ctx.Done():
		}
	}()
	return c
}
