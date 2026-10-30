package upstream

import (
	"context"
	"time"
)

// A Clock tells the time and schedules callbacks. The pool, the outlier
// detector and the health checker read time only through a Clock, so tests
// and simulations can substitute a fake one.
//
// Implementations must be safe for concurrent use. AfterFunc must call f in
// its own goroutine, as time.AfterFunc does; f may start before AfterFunc
// returns, for example when d is not positive, and callers in this module
// allow for that. The returned stop function reports whether it prevented
// f from running, like time.Timer.Stop.
//
// WithTimeout behaves like context.WithTimeout on the Clock's time: the
// returned context's Deadline reports Now()+d (or the parent's deadline if
// that is sooner), and once the Clock reaches it the context is canceled
// with Err and Cause both context.DeadlineExceeded. Every timeout in this
// module comes from it, so probes and transports see a real deadline.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// AfterFunc arranges for f to run in its own goroutine once d has
	// elapsed, and returns a function that cancels the call.
	AfterFunc(d time.Duration, f func()) (stop func() bool)

	// WithTimeout returns a copy of ctx that ends d from now on this Clock.
	WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc)
}

// SystemClock returns the Clock backed by package time.
func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(d time.Duration, f func()) func() bool {
	return time.AfterFunc(d, f).Stop
}

func (systemClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

func clockOrSystem(c Clock) Clock {
	if c == nil {
		return systemClock{}
	}
	return c
}
