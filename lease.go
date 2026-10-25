package upstream

// A Lease is a backend picked for one request or connection. It holds one
// of the backend's in-flight slots until Release returns it with the
// request's outcome.
//
// Every lease returned without error must be released exactly once, or the
// backend's in-flight count stays raised and a drain never completes;
// Pool.Do and Pool.DoKey do this for the caller. Release zeroes the lease,
// so releasing the same variable again does nothing. Copies of a lease
// share one slot: release only one of them. Making every copy idempotent
// would need state shared by the copies, one allocation per pick, so the
// pool instead detects what it can without cost: releasing a copy after
// another copy has been released panics if the backend has no slot left to
// return, like a negative sync.WaitGroup counter; if other leases are in
// flight, the extra release is not detected and frees one of their slots.
type Lease[T any] struct {
	b *Backend[T]
	t Ticket // t.reported is set by Report
}

// Backend returns the leased backend, or nil after Release.
func (l *Lease[T]) Backend() *Backend[T] { return l.b }

// Value returns the leased backend's value, or the zero value after
// Release.
func (l *Lease[T]) Value() T {
	if l.b == nil {
		var zero T
		return zero
	}
	return l.b.value
}

// Report records the outcome for passive health checking now, while the
// lease keeps its in-flight slot. Use it when the outcome is known long
// before the lease ends, as for a connection that was established and then
// stays open: a half-open backend's trial ends with the report instead of
// with the connection. Only the first Report counts; Release then returns
// the slot and ignores its outcome. Report does nothing on a released or
// zero Lease.
func (l *Lease[T]) Report(o Outcome) {
	if l.b == nil || l.t.reported {
		return
	}
	l.t.reported = true
	l.b.core.report(l.t, o)
}

// Release returns the lease's slot and records the outcome for passive
// health checking, unless Report already recorded one. It does nothing on
// a released or zero Lease. If the outcome ejects or restores the backend,
// Release also rebuilds the pool's view and may deliver events to
// subscribers on the calling goroutine; the slot is returned first, so a
// panicking subscriber cannot leak it.
func (l *Lease[T]) Release(o Outcome) {
	b, t := l.b, l.t
	if b == nil {
		return
	}
	*l = Lease[T]{}
	if t.reported {
		b.core.leave()
		return
	}
	b.core.release(t, o)
}

// run calls fn with the leased backend and releases the lease with the
// outcome of fn's error, or with Failure if fn panics.
func (l *Lease[T]) run(fn func(*Backend[T]) error) (err error) {
	o := Failure
	defer func() { l.Release(o) }()
	err = fn(l.b)
	o = OutcomeOf(err)
	return err
}
