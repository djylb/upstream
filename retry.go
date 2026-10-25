package upstream

import (
	"errors"
	"math"
	"time"
)

// Errors returned by Attempts.Next when it declines a retry.
var (
	// ErrRetryLimit means the request has tried RetryConfig.Attempts
	// backends.
	ErrRetryLimit = errors.New("upstream: retry limit reached")

	// ErrRetryBudget means the pool's retry budget is spent.
	ErrRetryBudget = errors.New("upstream: retry budget exhausted")
)

// RetryConfig configures retries on other backends, by Pool.Do,
// Pool.DoKey and Attempts. The zero value allows no retries.
//
// A retry budget bounds the extra load that retries put on the backends
// when many requests fail at once: within any Window, retries may number
// at most MinRetries plus Budget times the requests started.
type RetryConfig struct {
	// Window is the span over which the budget counts requests and
	// retries. The default is 10s. The counts are a sliding estimate over
	// the current and the previous window.
	Window time.Duration

	// Budget, if positive, is the largest ratio of retries to requests,
	// such as 0.2 for one retry per five requests. Zero means no budget:
	// only Attempts bounds retries.
	Budget float64

	// Attempts is the most backends one request tries, the first one
	// included. Zero and one mean no retries.
	Attempts int

	// MinRetries is the number of retries per Window that the budget
	// allows whatever the request rate, so that a pool with little
	// traffic can still retry. Zero means 10; a negative value means none.
	MinRetries int
}

type retryParams struct {
	epoch    time.Time
	clock    Clock
	window   time.Duration
	budget   float64 // 0 means no budget
	min      float64
	attempts int
}

func newRetryParams(c RetryConfig, clock Clock) retryParams {
	p := retryParams{clock: clock, window: c.Window, attempts: max(c.Attempts, 1)}
	if c.Budget > 0 && !math.IsInf(c.Budget, 1) {
		p.budget = c.Budget
		p.epoch = clock.Now()
	}
	if p.window <= 0 {
		p.window = 10 * time.Second
	}
	switch {
	case c.MinRetries == 0:
		p.min = 10
	case c.MinRetries > 0:
		p.min = float64(c.MinRetries)
	}
	return p
}

// Attempts picks distinct backends for one request: the first Next is a
// pick like PickKey, and every later Next is a retry that leases a backend
// this Attempts has not returned before, as the pool's RetryConfig allows.
// It reads the pool's current routing view on every call, so a retry
// avoids backends that went down meanwhile.
//
// Pool.Do is Attempts with a function; use Attempts directly when a lease
// outlives a function call, such as for a connection or a response body.
// An Attempts is for one goroutine. It does not allocate for up to four
// attempts.
type Attempts[T any] struct {
	p     *Pool[T]
	more  []*core // tried after the first four
	tried [4]*core
	hash  uint64
	n     int
}

// Attempts returns the picker for one request routed by key; an empty key
// carries no affinity, as with PickKey.
func (p *Pool[T]) Attempts(key string) Attempts[T] {
	return Attempts[T]{p: p, hash: keyHash(key)}
}

// Count returns the number of leases Next has returned.
func (a *Attempts[T]) Count() int { return a.n }

// Next leases the next backend. The first call fails like PickHash; a
// retry fails with ErrRetryLimit after RetryConfig.Attempts leases, with
// ErrRetryBudget when the budget is spent, and with ErrUnavailable when
// every backend that takes traffic has been tried. Release the previous
// lease, usually with Failure, before or after calling Next.
func (a *Attempts[T]) Next() (Lease[T], error) {
	p := a.p
	r := &p.retry
	var now time.Duration
	if a.n > 0 {
		if a.n >= r.attempts {
			return Lease[T]{}, ErrRetryLimit
		}
		if r.budget > 0 {
			// The retry is counted before the pick, atomically with the
			// check, so concurrent retries cannot overrun the budget; it is
			// given back if no backend is left.
			now = r.clock.Now().Sub(r.epoch)
			if !p.budget.addPartWithin(now, r.window, r.min, r.budget) {
				return Lease[T]{}, ErrRetryBudget
			}
		}
	} else if r.budget > 0 {
		now = r.clock.Now().Sub(r.epoch)
	}
	v := p.view.Load()
	tried := a
	if a.n == 0 {
		tried = nil
	}
	var l Lease[T]
	ok := false
	if len(v.members) > 0 {
		l, ok = pickFrom(v, a.hash, tried)
	}
	if !ok {
		if a.n > 0 && r.budget > 0 {
			p.budget.removePart()
		}
		if len(v.members) == 0 {
			return Lease[T]{}, v.err
		}
		return Lease[T]{}, ErrUnavailable
	}
	if a.n == 0 && r.budget > 0 {
		p.budget.add(now, r.window, windowTotal)
	}
	if a.n < len(a.tried) {
		a.tried[a.n] = l.b.core
	} else {
		a.more = append(a.more, l.b.core)
	}
	a.n++
	return l, nil
}

func (a *Attempts[T]) has(c *core) bool {
	for _, t := range a.tried[:min(a.n, len(a.tried))] {
		if t == c {
			return true
		}
	}
	for _, t := range a.more {
		if t == c {
			return true
		}
	}
	return false
}
