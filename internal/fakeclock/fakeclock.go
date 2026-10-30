// Package fakeclock provides a manually advanced clock for tests.
package fakeclock

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

// Clock is a fake time source. Its time moves only when Advance is called.
// It satisfies upstream.Clock.
type Clock struct {
	now    time.Time
	timers []*timer
	mu     sync.Mutex
	seq    uint64
}

type timer struct {
	at  time.Time
	f   func()
	seq uint64
}

// New returns a clock set to a fixed instant.
func New() *Clock {
	return &Clock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the clock's time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules f to run in its own goroutine once the clock has
// advanced by d. Like time.AfterFunc, it starts f at once if d is not
// positive.
func (c *Clock) AfterFunc(d time.Duration, f func()) func() bool {
	if d <= 0 {
		go f()
		return func() bool { return false }
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &timer{at: c.now.Add(d), f: f, seq: c.seq}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		i := slices.Index(c.timers, t)
		if i < 0 {
			return false
		}
		c.timers = slices.Delete(c.timers, i, i+1)
		return true
	}
}

// WithTimeout returns a copy of ctx whose Deadline is the clock's time
// plus d, or the parent's deadline if that is sooner, and that is canceled
// with context.DeadlineExceeded once Advance reaches it.
func (c *Clock) WithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	inner, cancel := context.WithCancelCause(parent)
	t := &timeoutCtx{Context: inner, deadline: c.Now().Add(d), done: make(chan struct{})}
	if pd, ok := parent.Deadline(); ok && pd.Before(t.deadline) {
		t.deadline = pd
	}
	stopWatch := context.AfterFunc(inner, t.finish)
	stopTimer := c.AfterFunc(d, func() {
		cancel(context.DeadlineExceeded)
		t.finish()
	})
	return t, func() {
		stopTimer()
		cancel(context.Canceled)
		stopWatch()
		t.finish()
	}
}

// timeoutCtx is the context of Clock.WithTimeout. It has its own Done
// channel, so that contexts derived from it take their error from Err,
// which reports context.DeadlineExceeded rather than context.Canceled when
// the fake deadline passed. Cause comes from the inner context.
type timeoutCtx struct {
	context.Context // inner, canceled with the cause
	deadline        time.Time
	once            sync.Once
	done            chan struct{}
	err             error
}

func (t *timeoutCtx) finish() {
	t.once.Do(func() {
		t.err = t.Context.Err()
		if errors.Is(context.Cause(t.Context), context.DeadlineExceeded) {
			t.err = context.DeadlineExceeded
		}
		close(t.done)
	})
}

func (t *timeoutCtx) Deadline() (time.Time, bool) { return t.deadline, true }

func (t *timeoutCtx) Done() <-chan struct{} { return t.done }

func (t *timeoutCtx) Err() error {
	if t.Context.Err() != nil {
		t.finish()
	}
	select {
	case <-t.done:
		return t.err
	default:
		return nil
	}
}

// Advance moves the clock forward by d and starts, in time order, the
// callbacks of all timers that are due. It does not wait for them.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*timer
	c.timers = slices.DeleteFunc(c.timers, func(t *timer) bool {
		if t.at.After(c.now) {
			return false
		}
		due = append(due, t)
		return true
	})
	c.mu.Unlock()
	slices.SortFunc(due, func(a, b *timer) int {
		if r := a.at.Compare(b.at); r != 0 {
			return r
		}
		return cmp.Compare(a.seq, b.seq)
	})
	for _, t := range due {
		go t.f()
	}
}

// Pending returns the number of scheduled timers.
func (c *Clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
