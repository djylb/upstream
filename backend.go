package upstream

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// State is the routing state of a backend, combining its membership, its
// active health and its passive health. When several conditions hold, the
// state is the first that applies in this order: removed, draining,
// unhealthy, ejected, half-open, healthy.
type State uint8

const (
	// StateHealthy means the backend takes new traffic.
	StateHealthy State = iota

	// StateHalfOpen means the backend's ejection is over and it takes a
	// limited number of trial requests.
	StateHalfOpen

	// StateEjected means passive health checking ejected the backend.
	StateEjected

	// StateUnhealthy means active health checks report the backend down.
	StateUnhealthy

	// StateDraining means the backend takes no new traffic but its leases
	// may still be in flight.
	StateDraining

	// StateRemoved means the backend is no longer a member of its pool.
	StateRemoved
)

// String returns the name of the state.
func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateHalfOpen:
		return "half-open"
	case StateEjected:
		return "ejected"
	case StateUnhealthy:
		return "unhealthy"
	case StateDraining:
		return "draining"
	case StateRemoved:
		return "removed"
	}
	return "State(" + strconv.Itoa(int(s)) + ")"
}

// MarshalText encodes the state as its name.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Health is the verdict of active health checking on a backend.
type Health uint8

const (
	// HealthUnknown means no check has decided yet, or checks are not
	// running for the backend. Unknown backends take traffic.
	HealthUnknown Health = iota

	// HealthUp means checks pass.
	HealthUp

	// HealthDown means checks fail; the backend takes no traffic.
	HealthDown
)

// String returns the name of the verdict.
func (h Health) String() string {
	switch h {
	case HealthUnknown:
		return "unknown"
	case HealthUp:
		return "up"
	case HealthDown:
		return "down"
	}
	return "Health(" + strconv.Itoa(int(h)) + ")"
}

// MarshalText encodes the verdict as its name.
func (h Health) MarshalText() ([]byte, error) { return []byte(h.String()), nil }

// CheckResult is the result of one active health check.
type CheckResult struct {
	// Time is when the check started.
	Time time.Time

	// Err is why the check failed, or nil.
	Err error

	// Latency is how long the check took.
	Latency time.Duration
}

// Status is a snapshot of one backend.
type Status struct {
	// LastCheck is the latest active health check result; its Time is zero
	// before the first check.
	LastCheck CheckResult

	// Outlier is the passive health snapshot. Its State is always
	// OutlierOK when passive health checking is disabled.
	Outlier OutlierStatus

	// ID identifies the backend.
	ID string

	// Weight is the backend's weight.
	Weight int

	// InFlight is the number of leases not yet released.
	InFlight int64

	// Successes, Failures and Neutrals count the outcomes of leases, as
	// reported by Lease.Report or Lease.Release. Together with InFlight
	// they count every pick: each lease is in flight until released and
	// has one outcome.
	Successes uint64
	Failures  uint64
	Neutrals  uint64

	// State is the routing state.
	State State

	// Health is the latest active health verdict, whatever the State.
	Health Health

	// Draining reports whether the backend is draining or removed.
	Draining bool
}

const (
	flagDraining uint32 = 1 << iota
	flagRemoved
	flagUnhealthy // active health is HealthDown
	flagUp        // active health is HealthUp
)

func healthOf(f uint32) Health {
	switch {
	case f&flagUnhealthy != 0:
		return HealthDown
	case f&flagUp != 0:
		return HealthUp
	}
	return HealthUnknown
}

func healthFlags(h Health) uint32 {
	switch h {
	case HealthUp:
		return flagUp
	case HealthDown:
		return flagUnhealthy
	case HealthUnknown:
	}
	return 0
}

// A Backend is one member of a Pool: a Target as the pool currently routes
// to it. Changing a target's weight or value in the pool creates a new
// Backend that shares the old one's counters and health state, so leases
// keep the version they were issued with.
type Backend[T any] struct {
	core   *core
	id     string
	weight int
	value  T
}

// ID returns the backend's ID.
func (b *Backend[T]) ID() string { return b.id }

// Value returns the value of the target the backend was created from.
func (b *Backend[T]) Value() T { return b.value }

// Weight returns the backend's weight, which is at least 1.
func (b *Backend[T]) Weight() int { return b.weight }

// InFlight returns the number of leases on the backend not yet released.
func (b *Backend[T]) InFlight() int64 { return b.core.inflight.Load() }

// State returns the backend's routing state.
func (b *Backend[T]) State() State { return b.core.state() }

// Status returns a snapshot of the backend.
func (b *Backend[T]) Status() Status {
	c := b.core
	f := c.flags.Load()
	s := Status{
		ID:        b.id,
		Weight:    b.weight,
		InFlight:  c.inflight.Load(),
		Successes: c.successes.Load(),
		Failures:  c.failures.Load(),
		Neutrals:  c.neutrals.Load(),
		State:     stateOf(f, c.det.State()),
		Health:    healthOf(f),
		Draining:  f&(flagDraining|flagRemoved) != 0,
	}
	if r := c.check.Load(); r != nil {
		s.LastCheck = *r
	}
	if c.passive {
		s.Outlier = c.det.Status()
	}
	return s
}

// core is the state shared by every version of a backend. Hot fields come
// first; flags change only under the pool's mutex but are read without it.
type core struct {
	inflight  atomic.Int64
	successes atomic.Uint64
	failures  atomic.Uint64
	flags     atomic.Uint32
	passive   bool // det is enabled
	det       OutlierDetector
	drain     atomic.Pointer[Drain]
	check     atomic.Pointer[CheckResult]
	rampStart atomic.Int64 // slow start: 0 none, -1 pending, else unix nanoseconds
	neutrals  atomic.Uint64
	reported  State // last state announced in an event; guarded by the pool's mutex
}

func stateOf(f uint32, o OutlierState) State {
	switch {
	case f&flagRemoved != 0:
		return StateRemoved
	case f&flagDraining != 0:
		return StateDraining
	case f&flagUnhealthy != 0:
		return StateUnhealthy
	}
	switch o {
	case OutlierOK:
		return StateHealthy
	case OutlierEjected:
		return StateEjected
	case OutlierHalfOpen:
		return StateHalfOpen
	}
	return StateHealthy
}

func (c *core) state() State { return stateOf(c.flags.Load(), c.det.State()) }

// cause returns the error behind state s, if any.
func (c *core) cause(s State) error {
	switch s {
	case StateUnhealthy:
		if r := c.check.Load(); r != nil {
			return r.Err
		}
	case StateEjected, StateHalfOpen:
		return c.det.Status().Cause
	case StateHealthy, StateDraining, StateRemoved:
	}
	return nil
}

// eligible reports whether the backend belongs in the routing view.
func (c *core) eligible() bool {
	return c.flags.Load()&^flagUp == 0 && c.det.State() != OutlierEjected
}

const blockAlways = flagDraining | flagRemoved

// acquire takes an in-flight slot. With ignoreHealth it admits any backend
// that is neither draining nor removed.
//
// The slot is counted before draining is checked a second time: drain sets
// the flag before it reads the count, so either drain sees the slot or
// acquire sees the flag and gives the slot back.
func (c *core) acquire(ignoreHealth bool) (Ticket, bool) {
	f := c.flags.Load()
	if f&blockAlways != 0 {
		return Ticket{}, false
	}
	var t Ticket
	if !ignoreHealth {
		if f&flagUnhealthy != 0 {
			return Ticket{}, false
		}
		if c.passive {
			var ok bool
			if t, ok = c.det.Admit(); !ok {
				return Ticket{}, false
			}
		}
	}
	c.inflight.Add(1)
	if c.flags.Load()&blockAlways != 0 {
		c.leave()
		if c.passive {
			c.det.Record(t, Neutral)
		}
		return Ticket{}, false
	}
	return t, true
}

// release returns the slot first: recording the outcome may run event
// subscribers and a Balancer, and a panic there must not leak the slot.
func (c *core) release(t Ticket, o Outcome) {
	c.count(o)
	c.leave()
	if c.passive {
		c.det.Record(t, o)
	}
}

// report records an outcome without returning the slot.
func (c *core) report(t Ticket, o Outcome) {
	c.count(o)
	if c.passive {
		c.det.Record(t, o)
	}
}

func (c *core) count(o Outcome) {
	switch o {
	case Success:
		c.successes.Add(1)
	case Failure:
		c.failures.Add(1)
	case Neutral:
		c.neutrals.Add(1)
	}
}

// leave returns a slot. When it returns the last one it closes the drain
// signal current at that moment. The signal is loaded before the flags: a
// drain that starts later stores a new signal and finds the count at zero
// itself, so it must not be closed here by a slot returned before it began.
func (c *core) leave() {
	n := c.inflight.Add(-1)
	if n < 0 {
		panic("upstream: lease released more times than acquired")
	}
	if n == 0 {
		// The count is read again: a pick and a new drain may both have
		// happened since it dropped to zero, and that drain's lease is in
		// flight. A pick that starts after the flags are set backs out.
		if d := c.drain.Load(); d != nil && c.flags.Load()&blockAlways != 0 && c.inflight.Load() == 0 {
			d.finish(false)
		}
	}
}

// drainLocked returns the backend's drain signal, creating it if needed,
// and sets the flags. The caller holds the pool's mutex; once it has
// released it, it calls checkDrained. The signal is stored before the
// flags so that a leave that sees the flags also sees the signal.
func (c *core) drainLocked(flags uint32) *Drain {
	d := c.drain.Load()
	if d == nil {
		d = &Drain{ch: make(chan struct{})}
		c.drain.Store(d)
	}
	c.flags.Or(flags)
	return d
}

// checkDrained closes d if nothing is in flight. Together with leave, which
// closes the signal when the count drops to zero, it closes d exactly when
// the last slot is gone: the flags are set before the count is read here,
// and the count drops before the flags are read in leave.
func (c *core) checkDrained(d *Drain) {
	if c.inflight.Load() == 0 {
		d.finish(false)
	}
}

// A Drain follows a backend that takes no new picks until its leases in
// flight are released.
type Drain struct {
	ch       chan struct{}
	once     sync.Once
	canceled bool // set before ch is closed
}

// Done returns a channel that is closed when the drain ends: when the
// backend's last lease is released, or when Resume cancels the drain.
func (d *Drain) Done() <-chan struct{} { return d.ch }

// Canceled reports whether Resume ended the drain before the backend's
// leases were all released. It is meaningful once Done is closed, and
// false before.
func (d *Drain) Canceled() bool {
	select {
	case <-d.ch:
		return d.canceled
	default:
		return false
	}
}

// ErrDrainCanceled is returned by Drain.Wait when Resume ended the drain
// before the backend's leases were all released.
var ErrDrainCanceled = errors.New("upstream: drain canceled")

// Wait waits until the drain ends. It returns nil once the backend's last
// lease is released, ErrDrainCanceled if Resume ended the drain first, and
// ctx's error if ctx is done first.
func (d *Drain) Wait(ctx context.Context) error {
	select {
	case <-d.ch:
		if d.canceled {
			return ErrDrainCanceled
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Drain) finish(canceled bool) {
	d.once.Do(func() {
		d.canceled = canceled
		close(d.ch)
	})
}
