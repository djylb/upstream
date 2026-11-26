package upstream

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"
)

// Errors returned by Request.Next when it declines a retry.
var (
	// ErrRetryLimit means the request has tried RetryConfig.Attempts
	// backends.
	ErrRetryLimit = errors.New("upstream: retry limit reached")

	// ErrRetryBudget means the pool's retry budget is spent.
	ErrRetryBudget = errors.New("upstream: retry budget exhausted")
)

// RetryConfig configures retries on other backends, by Pool.Do,
// Pool.DoKey and Request. The zero value allows no retries.
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

// retryParams is a normalized RetryConfig. It is immutable once
// published; a change publishes a new one.
type retryParams struct {
	counts   *window
	epoch    time.Time
	clock    Clock
	window   time.Duration
	budget   float64 // 0 means no budget
	min      float64
	attempts int
}

func newRetryParams(c RetryConfig, clock Clock, epoch time.Time) *retryParams {
	p := &retryParams{counts: new(window), clock: clock, window: c.Window, attempts: max(c.Attempts, 1), epoch: epoch}
	if c.Budget > 0 && !math.IsInf(c.Budget, 1) {
		p.budget = c.Budget
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

// A Request picks distinct backends for one request: the first Next is a
// pick like PickKey, and every later Next is a retry that leases a backend ID
// this Request has not returned before, as the pool's RetryConfig allows.
// It reads the pool's current routing view and retry settings on every
// call, so a retry avoids backends that went down meanwhile.
//
// Do runs the whole retry loop; Pool.Do is Do with a function of the
// backend. Use Next directly for loops that Do cannot express. A Request
// is for one goroutine. It does not allocate for up to four attempts.
type Request[T any] struct {
	p     *Pool[T]
	set   map[string]struct{} // every tried backend, once more holds triedLinear
	more  []string            // tried after the first four, until set takes over
	tried [4]string
	hash  uint64
	n     int
}

// triedLinear is the number of backends past the first four that a
// Request looks up by scanning; beyond it, a map keeps lookups constant
// so that a request that retries through n backends costs O(n), not O(n²).
const triedLinear = 32

// Request returns the picker for one request routed by key; an empty key
// carries no affinity, as with PickKey.
func (p *Pool[T]) Request(key string) Request[T] {
	return Request[T]{p: p, hash: keyHash(key)}
}

// Tried returns the number of leases Next has returned.
func (r *Request[T]) Tried() int { return r.n }

// Next leases the next backend. The first call fails like PickHash; a
// retry fails with ErrRetryLimit after RetryConfig.Attempts leases, with
// ErrRetryBudget when the budget is spent, with ErrUnavailable when
// every backend that takes traffic has been tried, and with ErrSaturated
// when those left are full. Release the previous
// lease, usually with Failure, before or after calling Next.
func (r *Request[T]) Next() (Lease[T], error) {
	p := r.p
	rp := p.retry.Load()
	var reserved reservation
	var now time.Duration
	if rp.budget > 0 {
		now = rp.clock.Now().Sub(rp.epoch)
	}
	if r.n > 0 {
		if r.n >= rp.attempts {
			return Lease[T]{}, ErrRetryLimit
		}
		// The retry is counted before the pick, atomically with the
		// check, so concurrent retries cannot overrun the budget; it is
		// given back if no backend is left.
		if rp.budget > 0 {
			reserved = rp.counts.reserve(now, rp.window, rp.min, rp.budget)
		}
		if rp.budget > 0 && reserved.credits == nil {
			return Lease[T]{}, ErrRetryBudget
		}
	}
	if reserved.credits != nil {
		defer func() {
			if reserved.credits != nil {
				reserved.refund()
			}
		}()
	}
	v := p.view.Load()
	tried := r
	if r.n == 0 {
		tried = nil
	}
	l, err := v.pick(r.hash, tried)
	if err != nil {
		return Lease[T]{}, err
	}
	if r.n == 0 && rp.budget > 0 {
		rp.counts.add(now, rp.window, 0)
	}
	reserved = reservation{}
	r.add(l.b.id)
	return l, nil
}

// Do calls fn with a lease from Next, again with a new lease for as long as
// fn asks for a retry, ctx is not done and Next allows one. fn owns its
// lease: it may release it, or hand it on with Lease.Take, as for a
// connection that outlives fn. A lease fn leaves unreleased is released
// when fn returns, with OutcomeOf of fn's error, or with Failure if fn
// panics, in which case the panic goes on.
//
// Do returns fn's error once fn asks for no retry. When a retry is
// declined, it returns fn's last error, or, if that was nil, ctx's error
// or the pick's; so does it when no lease could be picked at all. Do never
// returns nil unless fn's last call did.
func (r *Request[T]) Do(ctx context.Context, fn func(l *Lease[T]) (retry bool, err error)) error {
	return r.loop(ctx, func(l Lease[T]) (bool, error) { return runLease(&l, fn) })
}

// loop is Do with fn's lease passed by value, so that callers that do not
// hand the lease's address to unknown code, such as Pool.DoKey, do not
// allocate.
func (r *Request[T]) loop(ctx context.Context, attempt func(Lease[T]) (bool, error)) error {
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return last
			}
			return err
		}
		l, err := r.Next()
		if err != nil {
			if last != nil {
				return last
			}
			return err
		}
		retry, err := attempt(l)
		if !retry {
			return err
		}
		last = err
	}
}

// runLease calls fn and releases the lease if fn left it, with OutcomeOf
// of fn's error, or with Failure if fn panics.
func runLease[T any](l *Lease[T], fn func(*Lease[T]) (bool, error)) (retry bool, err error) {
	retry, _, err = runAttempt(l, l, fn)
	return retry, err
}

func runBackend[T any](l Lease[T], fn func(*Backend[T]) error) (bool, error) {
	_, o, err := runAttempt(&l, l.b, func(b *Backend[T]) (bool, error) { return false, fn(b) })
	return o == Failure, err
}

// The callback argument is independent of l so a backend-only call keeps
// its single-owner lease on the stack.
func runAttempt[T, A any](l *Lease[T], arg A, fn func(A) (bool, error)) (bool, Outcome, error) {
	o := Failure
	defer func() { l.Release(o) }()
	retry, err := fn(arg)
	o = OutcomeOf(err)
	return retry, o, err
}

// add records c as tried.
func (r *Request[T]) add(c string) {
	switch {
	case r.set != nil:
		r.set[c] = struct{}{}
	case r.n < len(r.tried):
		r.tried[r.n] = c
	case len(r.more) < triedLinear:
		r.more = append(r.more, c)
	default:
		r.set = make(map[string]struct{}, 2*r.n)
		for _, x := range r.tried {
			r.set[x] = struct{}{}
		}
		for _, x := range r.more {
			r.set[x] = struct{}{}
		}
		r.set[c] = struct{}{}
		r.more = nil
	}
	r.n++
}

func (r *Request[T]) has(c string) bool {
	if r.set != nil {
		_, ok := r.set[c]
		return ok
	}
	if slices.Contains(r.tried[:min(r.n, len(r.tried))], c) {
		return true
	}
	return slices.Contains(r.more, c)
}
