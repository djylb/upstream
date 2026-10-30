package upstream

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Errors returned by Pool.
var (
	// ErrNoBackends means the pool has no members.
	ErrNoBackends = errors.New("upstream: no backends")

	// ErrUnavailable means no member can take the pick now: every one is
	// unhealthy, ejected, draining, offline, out of half-open trial slots
	// or, for a retry, already tried. ErrSaturated is an ErrUnavailable
	// whose cause is capacity.
	ErrUnavailable = errors.New("upstream: no backend available")

	// ErrSaturated means that members that could take the pick refused it
	// because they were at their Target.MaxInFlight: the pick reached a
	// full tier and Config.Overflow is off, or every tier it could go on to
	// was full or unavailable too. It clears as leases are released, so it
	// calls for backpressure rather than for an alarm.
	// errors.Is(ErrSaturated, ErrUnavailable) holds.
	ErrSaturated = fmt.Errorf("upstream: backends at capacity: %w", ErrUnavailable)

	// ErrClosed means the pool was closed.
	ErrClosed = errors.New("upstream: pool closed")

	// ErrInvalidTarget means a Target has an empty or duplicate ID or a
	// weight out of range.
	ErrInvalidTarget = errors.New("upstream: invalid target")
)

// MaxWeight is the largest Target weight. It keeps the products of loads
// and weights that balancers compare far from overflowing.
const MaxWeight = math.MaxInt32

// A Target describes a backend to a Pool.
type Target[T any] struct {
	// ID identifies the backend. It must be unique within the pool. Health
	// state and counters follow the ID across updates, so give a backend a
	// new ID when its value stops meaning the same server.
	ID string

	// Weight is the backend's relative capacity within its tier, from 0 to
	// MaxWeight. Zero means 1; to stop sending traffic to a backend, drain
	// it.
	Weight int

	// Priority is the backend's tier. Picks go to the tier with the lowest
	// Priority that has an eligible member, balanced among that tier's
	// members, and move to the next tier only when no member of the tier
	// can take the pick; see Config.Overflow for tiers that are full.
	// Members of different priorities make an active-backup setup. The
	// default, 0, puts every member in one tier.
	Priority int

	// MaxInFlight, if positive, caps the backend's leases in flight: a
	// backend at its cap refuses picks, in panic mode too, until a lease
	// is released. Zero means no cap. The in-flight count may read above
	// the cap for a moment while a pick that lost a race for the last slot
	// backs out; the leases never exceed it.
	MaxInFlight int

	// Value is what callers connect to, such as an address.
	Value T
}

// Fallback says what a Pool does when too few backends are eligible.
type Fallback uint8

const (
	// FallbackNone makes Pick fail with ErrUnavailable.
	FallbackNone Fallback = iota

	// FallbackAll enters panic mode: Pick chooses among all members that
	// are not draining, ignoring active and passive health. A health signal
	// that marks every backend down is more often wrong than right, and
	// degraded service beats none. The pool panics when no member is
	// eligible, when fewer than Config.PanicThreshold percent of the
	// members that are not draining are eligible, and, for a single pick,
	// when every eligible member refuses it, as half-open members out of
	// trial slots do. Leases picked in panic mode still report outcomes.
	FallbackAll
)

// String returns the name of the fallback, as ParseFallback accepts it.
//
//goland:noinspection GoMixedReceiverTypes
func (f Fallback) String() string {
	switch f {
	case FallbackNone:
		return "none"
	case FallbackAll:
		return "all"
	}
	return "Fallback(" + strconv.Itoa(int(f)) + ")"
}

// MarshalText encodes the fallback as its name.
//
//goland:noinspection GoMixedReceiverTypes
func (f Fallback) MarshalText() ([]byte, error) { return []byte(f.String()), nil }

// ParseFallback returns the Fallback named s: "none" or "all".
func ParseFallback(s string) (Fallback, error) { return parseName(s, FallbackAll+1, "fallback") }

// Config configures a Pool. The zero value is a pool with smooth weighted
// round-robin, no passive health checking, no slow start, no retries and
// FallbackNone. Pool.SetConfig changes everything but the Clock while the
// pool runs.
type Config struct {
	// Balancer chooses backends. Nil means WeightedRoundRobin.
	Balancer Balancer

	// Clock is the time source. Nil means SystemClock.
	Clock Clock

	// Equal reports whether two target values mean the same backend, so
	// that Sync and Put keep a member whose weight and value did not
	// change: same Backend, no EventUpdated, no rebuild. Nil compares
	// values with ==, through reflection, when they are comparable, and
	// treats other values, such as slices, as always changed.
	Equal func(a, b any) bool

	// Outlier configures passive health checking, which is disabled by
	// default.
	Outlier OutlierConfig

	// Retry configures how Do, DoKey and Request retry on other
	// backends. There are no retries by default.
	Retry RetryConfig

	// SlowStart, if positive, ramps up the traffic of a backend that joins
	// the pool or becomes healthy again: its weight grows linearly from a
	// small fraction to its full value over SlowStart, so a cold backend is
	// not flooded. The pool rebuilds its view 16 times over the ramp. With
	// ConsistentHash the ramp moves keys to the backend gradually. Slow
	// start works through weights within a tier, so it has no effect with
	// RoundRobin, which ignores weights, on a backend alone in its tier, or
	// between tiers.
	SlowStart time.Duration

	// PanicThreshold, from 1 to 100, makes a pool with FallbackAll panic
	// while fewer than this percentage of its members that are not
	// draining are eligible, instead of only when none is: a few survivors
	// would otherwise take the whole load and fail in turn. Zero panics
	// only when no member is eligible.
	PanicThreshold int

	// Fallback says what to do when too few backends are eligible.
	Fallback Fallback

	// Overflow lets a pick move to the next tier when every member of its
	// tier that it may use is at its Target.MaxInFlight. Without Overflow
	// such a pick fails with ErrSaturated, so that a lower tier, such as
	// a costly backup, takes traffic only when the tier above is down, not
	// when it is busy. Members that are down, ejected, out of half-open
	// trial slots or already tried by the request always pass the pick on.
	Overflow bool
}

// settings are the parts of a Config that the pool reads under its mutex.
type settings[T any] struct {
	balancer  Balancer
	equal     func(a, b T) bool
	outlier   *outlierParams
	slowStart time.Duration
	threshold int // panic threshold in percent
	fallback  Fallback
	overflow  bool
	gen       uint64 // changes with the balancer
}

// rampScale multiplies weights while a backend is ramping up, so that
// backends of weight 1 can ramp too.
const rampScale = 16

// A Pool is a set of backends with a balancer, health state and
// accounting. Pick and Release are lock-free and do not allocate; updates
// to membership and health build a new immutable routing view and publish
// it atomically, so picks in progress are never disturbed.
//
// A backend is eligible for picks when it is not draining, not reported
// down by active checks and not ejected by passive checks; a half-open
// backend is eligible for its trial requests. When too few backends are
// eligible, Config.Fallback decides.
//
// Rebuilds are coalesced: an update that comes while another goroutine
// builds the view leaves the next build to that goroutine and returns. A
// method that changes the pool has published its change when it returns,
// unless such a build is in progress, which then publishes it. Removals,
// drains and health verdicts take effect for picks at once either way,
// because a pick checks the backend's state itself; only new members and
// changed weights wait for the view.
//
// The pool starts no goroutines. It owns the passive health timers and the
// slow-start timer, which Close stops. Pick never locks; Release locks only
// when its outcome ejects or restores a backend, to rebuild the view.
//
// Locking: mu guards membership and settings and is never held while
// calling a Balancer, a Clock or an event subscriber. Inside mu the pool
// may take qmu (the event queue) and an OutlierDetector's mutex to read
// its status; neither calls out while held. rampMu (the slow-start timer)
// is taken alone.
type Pool[T any] struct {
	view      atomic.Pointer[view[T]]
	retry     atomic.Pointer[retryParams]
	budget    window // retry budget: first attempts and retries
	guard     ejectionGuard
	clock     Clock
	isClosed  atomic.Bool
	rampArmed atomic.Bool

	mu       sync.Mutex
	cfg      settings[T]
	byID     map[string]*Backend[T]
	order    []*Backend[T]
	subs     []*subscriber[T] // copy on write
	dirty    bool             // the view is out of date
	building bool             // a goroutine is building the view
	closed   bool

	// Owned by the building goroutine.
	built    map[int]builtTier // the latest Picker built for each priority
	builtGen uint64            // settings.gen of built

	qmu      sync.Mutex
	queue    []delivery[T]
	flushing bool

	rampMu   sync.Mutex
	rampStop func() bool

	closedCh chan struct{} // closed by Close
}

// view is an immutable routing snapshot.
type view[T any] struct {
	err   error     // returned when nothing can be picked
	tiers []tier[T] // by ascending priority; none is empty
	// fallback, under FallbackAll outside panic mode, holds the members
	// that are not draining or offline, by tier, for a pick that every
	// member refuses. Its tiers have no Picker.
	fallback     []tier[T]
	eligible     int  // members of tiers, for Available
	ignoreHealth bool // panic mode: tiers hold all that are not draining or offline
	overflow     bool // Config.Overflow
}

// A tier is the members of one priority and their Picker.
type tier[T any] struct {
	picker   Picker
	members  []*Backend[T] // the candidates of picker
	priority int
}

// builtTier is a Picker a build made, kept for the next build of its
// priority to reuse or rebuild.
type builtTier struct {
	picker Picker
	cands  []Candidate
}

// New returns an empty pool.
func New[T any](cfg Config) *Pool[T] {
	p := &Pool[T]{
		clock:    clockOrSystem(cfg.Clock),
		byID:     make(map[string]*Backend[T]),
		closedCh: make(chan struct{}),
	}
	epoch := p.clock.Now()
	p.cfg.outlier = newOutlierParams(cfg.Outlier, p.clock, epoch)
	p.retry.Store(newRetryParams(cfg.Retry, p.clock, epoch))
	p.setLocked(cfg)
	p.view.Store(&view[T]{err: ErrNoBackends})
	return p
}

// setLocked applies cfg except Clock, Outlier and Retry.
func (p *Pool[T]) setLocked(cfg Config) {
	s := &p.cfg
	b := cfg.Balancer
	if b == nil {
		b = WeightedRoundRobin()
	}
	if !sameBalancer(s.balancer, b) {
		s.gen++ // the previous Picker is not this Balancer's to rebuild
	}
	s.balancer = b
	s.equal = equalFunc[T](cfg.Equal)
	s.slowStart = max(cfg.SlowStart, 0)
	s.threshold = min(max(cfg.PanicThreshold, 0), 100)
	s.fallback = cfg.Fallback
	s.overflow = cfg.Overflow
}

// sameBalancer reports whether a and b are the same comparable Balancer,
// such as the same pointer or equal values of ConsistentHash.
func sameBalancer(a, b Balancer) bool {
	if a == nil || reflect.TypeOf(a) != reflect.TypeOf(b) || !reflect.ValueOf(a).Comparable() || !reflect.ValueOf(b).Comparable() {
		return false
	}
	return a == b
}

// SetConfig replaces the pool's configuration, except Clock, keeping its
// members with their counters, health and leases. A new Balancer builds a
// new view from scratch; the same Balancer, such as the same pointer or an
// equal ConsistentHash value, goes on rebuilding its Pickers; new outlier
// settings apply to the next request of every member, and disabling
// passive health checking clears all ejections; new retry settings apply
// to the next Request.Next. It returns ErrClosed if the pool is closed.
func (p *Pool[T]) SetConfig(cfg Config) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	p.setLocked(cfg)
	old := p.cfg.outlier
	op := newOutlierParams(cfg.Outlier, p.clock, old.epoch)
	op.seq = old.seq + 1
	p.cfg.outlier = op
	rp := newRetryParams(cfg.Retry, p.clock, p.retry.Load().epoch)
	if p.retry.Swap(rp).window != rp.window {
		p.budget.restart(p.clock.Now().Sub(rp.epoch), rp.window)
	}
	members := slices.Clone(p.order)
	p.unlockAndCommit()

	// The detectors are updated without mu: disabling one resets it, which
	// rebuilds the view. A concurrent SetConfig cannot leave older
	// parameters behind, as a detector keeps the latest seq.
	for _, b := range members {
		b.core.det.setParams(op)
	}
	return nil
}

// equalFunc returns the comparison of target values for cfg.Equal.
func equalFunc[T any](eq func(a, b any) bool) func(a, b T) bool {
	if eq != nil {
		return func(a, b T) bool { return eq(a, b) }
	}
	return func(a, b T) bool {
		va, vb := reflect.ValueOf(&a).Elem(), reflect.ValueOf(&b).Elem()
		return va.Comparable() && vb.Comparable() && va.Equal(vb)
	}
}

func validate[T any](t Target[T]) error {
	switch {
	case t.ID == "":
		return fmt.Errorf("%w: empty ID", ErrInvalidTarget)
	case t.Weight < 0 || t.Weight > MaxWeight:
		return fmt.Errorf("%w: %q: weight %d out of range", ErrInvalidTarget, t.ID, t.Weight)
	case t.MaxInFlight < 0:
		return fmt.Errorf("%w: %q: negative MaxInFlight %d", ErrInvalidTarget, t.ID, t.MaxInFlight)
	}
	return nil
}

// newBackendLocked returns a backend for t sharing c, or with a new core
// if c is nil.
func (p *Pool[T]) newBackendLocked(t Target[T], c *core) *Backend[T] {
	if c == nil {
		c = new(core)
		c.det.init(p.cfg.outlier, p.refresh, &p.guard)
		if p.cfg.slowStart > 0 {
			c.rampStart.Store(-1)
		}
	}
	return &Backend[T]{core: c, id: t.ID, weight: max(t.Weight, 1), priority: t.Priority, limit: int64(t.MaxInFlight), value: t.Value}
}

// Sync replaces the pool's membership with targets, in order. Members
// whose ID is in targets keep their health state and counters and take
// the new weight and value; new IDs join as healthy; members missing from
// targets are removed gracefully: they take no new picks while their
// leases finish. Sync emits EventRemoved, then EventAdded or EventUpdated
// for each target that is new or changed, in order. A Sync that changes
// nothing, order included, does nothing. On error nothing changes.
func (p *Pool[T]) Sync(targets []Target[T]) error {
	seen := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		if err := validate(t); err != nil {
			return err
		}
		if _, dup := seen[t.ID]; dup {
			return fmt.Errorf("%w: duplicate ID %q", ErrInvalidTarget, t.ID)
		}
		seen[t.ID] = struct{}{}
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	var removed []removal
	for _, b := range p.order {
		if _, keep := seen[b.id]; !keep {
			removed = append(removed, p.removeLocked(b))
		}
	}
	order := make([]*Backend[T], 0, len(targets))
	for _, t := range targets {
		order = append(order, p.putLocked(t))
	}
	if len(removed) == 0 && slices.Equal(order, p.order) {
		p.mu.Unlock()
		return nil
	}
	p.order = order
	defer finishRemovals(removed)
	p.unlockAndCommit()
	return nil
}

// Put adds a target, or replaces the member with the same ID, keeping its
// position, health state and counters. It emits EventAdded or
// EventUpdated; a Put that changes none of the weight, priority, cap and
// value of a member does nothing.
func (p *Pool[T]) Put(t Target[T]) error {
	if err := validate(t); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	old := p.byID[t.ID]
	b := p.putLocked(t)
	switch {
	case b == old:
		p.mu.Unlock()
		return nil
	case old == nil:
		p.order = append(p.order, b)
	default:
		p.order[slices.Index(p.order, old)] = b
	}
	p.unlockAndCommit()
	return nil
}

// putLocked creates or replaces the backend for t in byID and emits the
// event, or returns the current backend if t changes nothing; the caller
// places it in order.
func (p *Pool[T]) putLocked(t Target[T]) *Backend[T] {
	if old := p.byID[t.ID]; old != nil {
		if old.weight == max(t.Weight, 1) && old.priority == t.Priority && old.limit == int64(t.MaxInFlight) && p.cfg.equal(old.value, t.Value) {
			return old
		}
		b := p.newBackendLocked(t, old.core)
		p.byID[t.ID] = b
		p.emitLocked(Event[T]{Kind: EventUpdated, Backend: b, From: old.core.reported, To: old.core.reported})
		return b
	}
	b := p.newBackendLocked(t, nil)
	p.byID[t.ID] = b
	p.emitLocked(Event[T]{Kind: EventAdded, Backend: b, From: StateHealthy, To: StateHealthy})
	return b
}

// A removal is cleanup left for after a removed backend's commit.
type removal struct {
	c *core
	d *Drain
}

// finishRemovals stops the detectors of removed backends and closes their
// drain signals if nothing is in flight. Callers defer it before
// committing, so it runs even if a subscriber panics.
func finishRemovals(rs []removal) {
	for _, r := range rs {
		r.c.det.Close()
		r.c.checkDrained(r.d)
	}
}

// removeLocked marks b removed and emits EventRemoved; the caller fixes
// order and runs finishRemovals after unlocking.
func (p *Pool[T]) removeLocked(b *Backend[T]) removal {
	delete(p.byID, b.id)
	from := b.core.reported
	d := b.core.drainLocked(flagRemoved | flagDraining)
	b.core.reported = StateRemoved
	p.emitLocked(Event[T]{Kind: EventRemoved, Backend: b, From: from, To: StateRemoved})
	return removal{c: b.core, d: d}
}

// Remove removes the backend with the given ID gracefully: new picks avoid
// it at once, and leases already issued finish normally. The returned
// Drain is done when the last of them is released. Remove reports false if
// there is no such member.
func (p *Pool[T]) Remove(id string) (*Drain, bool) {
	p.mu.Lock()
	b := p.byID[id]
	if b == nil {
		p.mu.Unlock()
		return nil, false
	}
	r := p.removeLocked(b)
	p.order = slices.DeleteFunc(p.order, func(x *Backend[T]) bool { return x == b })
	defer finishRemovals([]removal{r})
	p.unlockAndCommit()
	return r.d, true
}

// Drain stops new picks of the backend with the given ID while it stays a
// member, for maintenance or before removal. The returned Drain is done
// when the backend has no leases in flight, or canceled when Resume ends
// the drain first. Draining a backend that is already draining returns the
// same Drain. Drain reports false if there is no such member.
func (p *Pool[T]) Drain(id string) (*Drain, bool) {
	p.mu.Lock()
	b := p.byID[id]
	if b == nil {
		p.mu.Unlock()
		return nil, false
	}
	d := b.core.drainLocked(flagDraining)
	defer b.core.checkDrained(d)
	p.unlockAndCommit()
	return d, true
}

// Resume ends a drain started by Drain; the Drain reports Canceled unless
// it had already completed. Resume reports false if there is no such
// member.
func (p *Pool[T]) Resume(id string) bool {
	p.mu.Lock()
	b := p.byID[id]
	if b == nil {
		p.mu.Unlock()
		return false
	}
	b.core.flags.And(^flagDraining)
	if d := b.core.drain.Swap(nil); d != nil {
		defer d.finish(true)
	}
	p.unlockAndCommit()
	return true
}

// ReportCheck records the result of an active health check of b and sets
// its health. A health checker calls it after every check; the checker,
// not the pool, applies rise and fall thresholds. HealthDown takes the
// backend out of rotation; HealthUp and HealthUnknown keep it in, so a
// change between them only updates the health and rebuilds nothing.
// Reports for a backend that has been removed, or replaced by a new
// member with the same ID, are ignored; ReportCheck reports whether b was
// a current member of an open pool.
func (p *Pool[T]) ReportCheck(b *Backend[T], h Health, r CheckResult) bool {
	c := b.core
	want := healthFlags(h)
	if !p.lockMember(b) {
		return false
	}
	c.check.Store(&r)
	for {
		old := c.flags.Load()
		f := old&^(flagUp|flagUnhealthy) | want
		if old == f {
			p.mu.Unlock()
			return true
		}
		if c.flags.CompareAndSwap(old, f) {
			if (old^f)&flagUnhealthy == 0 {
				p.mu.Unlock()
				return true
			}
			break
		}
	}
	p.unlockAndCommit()
	return true
}

// lockMember locks mu if b is the current version, or an earlier one, of a
// member of the open pool, and reports whether it did.
func (p *Pool[T]) lockMember(b *Backend[T]) bool {
	p.mu.Lock()
	if cur := p.byID[b.id]; p.closed || cur == nil || cur.core != b.core {
		p.mu.Unlock()
		return false
	}
	return true
}

// MarkOffline takes b out of rotation on the caller's word, for example
// because the session the backend is reached through dropped, until
// MarkOnline. It is one of three inputs that each veto picks: an offline
// backend, one that active checks report down and one that passive checks
// ejected take no new picks, and MarkOnline lifts only its own veto. While
// several apply, the backend's State is the first in the order
// StateDraining, StateOffline, StateUnhealthy, StateEjected,
// StateHalfOpen. Unlike the health verdicts, the mark also holds in panic
// mode: FallbackAll never picks an offline backend, which also does not
// count towards Config.PanicThreshold.
//
// Taking b offline emits EventStateChanged with cause as its Err, or
// ErrOffline if cause is nil, and rebuilds the view like any other change;
// marking an offline backend again only replaces the cause, which the next
// event reports. The mark follows the member's ID across updates. A call
// for a backend that has been removed, or replaced by a new member with the
// same ID, is ignored; MarkOffline reports whether b was a current member
// of an open pool.
func (p *Pool[T]) MarkOffline(b *Backend[T], cause error) bool {
	if !p.lockMember(b) {
		return false
	}
	c := b.core
	c.offline.Store(&cause)
	if c.flags.Or(flagOffline)&flagOffline != 0 {
		p.mu.Unlock()
		return true
	}
	p.unlockAndCommit()
	return true
}

// MarkOnline lifts the mark of MarkOffline; a backend that comes back
// online ramps up under Config.SlowStart. Like MarkOffline, it ignores
// backends that are no longer members, and reports whether b was a current
// member of an open pool.
func (p *Pool[T]) MarkOnline(b *Backend[T]) bool {
	if !p.lockMember(b) {
		return false
	}
	c := b.core
	if c.flags.And(^flagOffline)&flagOffline == 0 {
		p.mu.Unlock()
		return true
	}
	c.offline.Store(nil)
	p.unlockAndCommit()
	return true
}

// ResetOutlier clears the passive health ejection of the backend with the
// given ID, if any. It reports false if there is no such member.
func (p *Pool[T]) ResetOutlier(id string) bool {
	b, ok := p.Backend(id)
	if ok {
		b.core.det.Reset()
	}
	return ok
}

// refresh rebuilds the view after a passive health transition or a
// slow-start step.
func (p *Pool[T]) refresh() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.unlockAndCommit()
}

// unlockAndCommit announces state changes under mu and marks the view out
// of date, releases mu, then builds and publishes the view, unless another
// goroutine is building it, and delivers events.
func (p *Pool[T]) unlockAndCommit() {
	p.dirty = true
	for _, b := range p.order {
		c := b.core
		if s := c.state(); s != c.reported {
			if p.cfg.slowStart > 0 && s == StateHealthy {
				c.rampStart.Store(-1)
			}
			p.emitLocked(Event[T]{Kind: EventStateChanged, Backend: b, From: c.reported, To: s, Err: c.cause(s)})
			c.reported = s
		}
	}
	defer p.flush()
	if p.building {
		p.mu.Unlock()
		return
	}
	p.building = true
	p.buildLoop(commitBuilds)
}

// commitBuilds bounds the views a committer builds: its own and one more
// for the commits that coalesced into it while it built. Commits that
// come during the second build are handed off to a goroutine of the pool,
// so that a request whose Lease.Release commits an ejection is not held
// up by other goroutines' changes.
const commitBuilds = 2

// snapshot is what a build needs from the pool's state.
type snapshot[T any] struct {
	v         *view[T]
	balancer  Balancer
	gen       uint64
	slowStart time.Duration
	members   []*Backend[T] // what v.tiers are made of, in membership order
}

// buildLoop builds and publishes views while the view is out of date, at
// most rounds times if rounds is positive. It is entered with mu held and
// building set, and returns with mu released, and building cleared unless
// it handed the remaining work off to handOff. Only the goroutine with
// building set builds, so Balancer calls never overlap, and a commit that
// finds building set returns at once: it set dirty under mu, and the
// builder sees it before it clears building, by taking another snapshot
// or by handing off.
//
// A build whose Balancer panics publishes nothing and emits
// EventBalancerPanic; the panic does not reach the caller. If commits came
// in during that build, the loop goes on and builds them; otherwise it
// stops with dirty set, so the next commit builds again, and a Balancer
// that panics on every build is tried once per commit, never in a loop.
func (p *Pool[T]) buildLoop(rounds int) {
	for {
		if p.closed || !p.dirty {
			p.building = false
			p.mu.Unlock()
			return
		}
		if rounds == 0 {
			p.mu.Unlock()
			go p.handOff()
			return
		}
		rounds--
		s := p.snapshotLocked()
		p.dirty = false
		p.mu.Unlock()
		pe := p.buildRound(s)
		p.mu.Lock()
		if pe != nil && !p.closed {
			p.emitLocked(Event[T]{Kind: EventBalancerPanic, Err: pe})
			if !p.dirty {
				p.dirty = true // the view is still out of date
				p.building = false
				p.mu.Unlock()
				return
			}
		}
	}
}

// buildRound builds and publishes s. It returns the recovered panic of
// the Balancer, if any, in which case it publishes nothing.
// If the Balancer calls runtime.Goexit instead, which cannot be stopped,
// it gives up building with the view marked out of date, so that the next
// commit builds again.
func (p *Pool[T]) buildRound(s snapshot[T]) (pe *BalancerPanicError) {
	returned := false
	defer func() {
		if r := recover(); r != nil {
			pe = &BalancerPanicError{Value: r, Stack: debug.Stack()}
		} else if !returned { // runtime.Goexit
			p.mu.Lock()
			p.dirty = true
			p.building = false
			p.mu.Unlock()
		}
	}()
	p.build(s)
	p.mu.Lock()
	if !p.closed {
		p.view.Store(s.v)
	}
	p.mu.Unlock()
	returned = true
	return nil
}

// handOff builds the views of commits that their committer left, until
// the view is current or the pool closes, and delivers the events of
// those builds. It runs in its own goroutine, off any request path.
func (p *Pool[T]) handOff() {
	defer p.flush()
	p.mu.Lock()
	p.buildLoop(-1)
}

// snapshotLocked selects the members of the next view.
func (p *Pool[T]) snapshotLocked() snapshot[T] {
	s := snapshot[T]{v: &view[T]{err: ErrUnavailable, overflow: p.cfg.overflow}, balancer: p.cfg.balancer, gen: p.cfg.gen, slowStart: p.cfg.slowStart}
	v := s.v
	if len(p.order) == 0 {
		v.err = ErrNoBackends
	}
	p.guard.members.Store(int32(min(len(p.order), math.MaxInt32)))
	var open []*Backend[T] // not draining or offline
	for _, b := range p.order {
		c := b.core
		if c.flags.Load()&blockPick != 0 {
			continue
		}
		open = append(open, b)
		if c.eligible() {
			s.members = append(s.members, b)
		}
	}
	if p.cfg.fallback == FallbackAll && len(open) > 0 {
		if len(s.members) == 0 || len(s.members)*100 < p.cfg.threshold*len(open) {
			s.members = open
			v.ignoreHealth = true
		} else {
			v.fallback = groupTiers(open)
		}
	}
	return s
}

// groupTiers returns bs grouped by ascending priority, keeping their
// order within each priority.
func groupTiers[T any](bs []*Backend[T]) []tier[T] {
	if len(bs) == 0 {
		return nil
	}
	bs = slices.Clone(bs)
	slices.SortStableFunc(bs, func(a, b *Backend[T]) int { return cmp.Compare(a.priority, b.priority) })
	var ts []tier[T]
	for i := 0; i < len(bs); {
		j := i + 1
		for j < len(bs) && bs[j].priority == bs[i].priority {
			j++
		}
		ts = append(ts, tier[T]{members: bs[i:j:j], priority: bs[i].priority})
		i = j
	}
	return ts
}

// build makes the tiers of s.v and their Pickers, without mu. A tier whose
// candidates are those of the Picker built last for its priority by the
// same Balancer keeps that Picker; otherwise a Rebuilder rebuilds that
// Picker and other Balancers build a new one. The Pickers are kept only if
// every tier builds.
func (p *Pool[T]) build(s snapshot[T]) {
	v := s.v
	v.tiers = groupTiers(s.members)
	v.eligible = len(s.members)
	if len(v.tiers) == 0 {
		p.built = nil
		return
	}
	prev := p.built
	if p.builtGen != s.gen {
		prev = nil
	}
	built := make(map[int]builtTier, len(v.tiers))
	for i := range v.tiers {
		t := &v.tiers[i]
		cands := make([]Candidate, len(t.members))
		for k, b := range t.members {
			c := b.core
			cands[k] = Candidate{InFlight: &c.inflight, Latency: &c.latency, Meta: b, ID: b.id, Weight: b.weight}
		}
		if s.slowStart > 0 {
			p.applySlowStart(t.members, cands, s.slowStart)
		}
		old, ok := prev[t.priority]
		switch r, rebuilder := s.balancer.(Rebuilder); {
		case ok && slices.Equal(old.cands, cands):
			t.picker = old.picker
			cands = old.cands
		case ok && rebuilder:
			t.picker = r.Rebuild(old.picker, cands)
		default:
			t.picker = s.balancer.Build(cands)
		}
		built[t.priority] = builtTier{picker: t.picker, cands: cands}
	}
	p.built, p.builtGen = built, s.gen
}

// applySlowStart lowers the candidate weights of backends that are ramping
// up and arms the timer for the next step.
func (p *Pool[T]) applySlowStart(members []*Backend[T], cands []Candidate, ramp time.Duration) {
	now := p.clock.Now().UnixNano()
	ramping := false
	maxWeight := 0
	fraction := make([]float64, len(members))
	for i, b := range members {
		c := b.core
		maxWeight = max(maxWeight, b.weight)
		fraction[i] = 1
		rs := c.rampStart.Load()
		if rs == -1 && c.rampStart.CompareAndSwap(-1, now) {
			rs = now
		}
		if rs <= 0 {
			continue
		}
		// A rebuild may have started the ramp with a later reading of the
		// clock than ours: count that as the start.
		elapsed := max(now-rs, 0)
		if elapsed >= int64(ramp) {
			c.rampStart.CompareAndSwap(rs, 0)
			continue
		}
		fraction[i] = float64(elapsed) / float64(ramp)
		ramping = true
	}
	if !ramping {
		return
	}
	scale := 1
	if maxWeight <= MaxWeight/rampScale {
		scale = rampScale
	}
	for i := range cands {
		cands[i].Weight = max(int(float64(cands[i].Weight*scale)*fraction[i]), 1)
	}
	p.armRamp(max(ramp/rampScale, time.Millisecond))
}

// armRamp schedules one refresh after d unless one is already pending. The
// clock is called without a lock; Close stops the timer.
func (p *Pool[T]) armRamp(d time.Duration) {
	if p.isClosed.Load() || !p.rampArmed.CompareAndSwap(false, true) {
		return
	}
	stop := p.clock.AfterFunc(d, func() {
		p.rampArmed.Store(false)
		p.refresh()
	})
	p.rampMu.Lock()
	if p.isClosed.Load() {
		p.rampMu.Unlock()
		stop()
		return
	}
	p.rampStop = stop
	p.rampMu.Unlock()
}

// Pick leases a backend chosen by the balancer without a key. Key-affine
// balancers pick at random.
func (p *Pool[T]) Pick() (Lease[T], error) { return p.view.Load().pick(rand.Uint64(), nil) }

// PickKey leases a backend chosen for key, such as a client address or a
// session ID. With ConsistentHash the same key keeps mapping to the same
// backend while it is eligible; other balancers ignore the key. An empty
// key carries no affinity: PickKey("") is Pick.
func (p *Pool[T]) PickKey(key string) (Lease[T], error) {
	return p.view.Load().pick(keyHash(key), nil)
}

// PickHash is like PickKey for callers that hash keys themselves.
func (p *Pool[T]) PickHash(hash uint64) (Lease[T], error) {
	return p.view.Load().pick(hash, nil)
}

func keyHash(key string) uint64 {
	if key == "" {
		return rand.Uint64()
	}
	return hashString(key)
}

// pick leases a member of v that is not in tried, which may be nil, from
// the first tier that has one that can take the request, and last, under
// FallbackAll, any member that is not draining or offline, again tier by
// tier. Within a tier it takes the balancer's choices first, then every
// member in turn, for when the balancer kept choosing backends that could
// not take the request, such as half-open ones out of trial slots, full
// ones or ones already tried. A tier that refuses because members are full
// ends the pick with ErrSaturated unless v.overflow lets it go on to the
// next tier; the pick also fails with ErrSaturated if it found members
// full on its way, and with ErrUnavailable otherwise.
func (v *view[T]) pick(hash uint64, tried *Request[T]) (Lease[T], error) {
	if len(v.tiers) == 0 {
		return Lease[T]{}, v.err
	}
	full := false
	for i := range v.tiers {
		l, r := v.tiers[i].pick(hash, tried, v.ignoreHealth)
		if r == admitted {
			return l, nil
		}
		if r == saturated {
			if !v.overflow {
				return Lease[T]{}, ErrSaturated
			}
			full = true
		}
	}
	for i := range v.fallback {
		l, r := scan(v.fallback[i].members, hash, tried, true)
		if r == admitted {
			return l, nil
		}
		if r == saturated {
			if !v.overflow {
				return Lease[T]{}, ErrSaturated
			}
			full = true
		}
	}
	if full {
		return Lease[T]{}, ErrSaturated
	}
	return Lease[T]{}, ErrUnavailable
}

// pick leases a member of t not in tried. It returns saturated if no
// member took the pick and one of them refused because it was full.
func (t *tier[T]) pick(hash uint64, tried *Request[T], ignoreHealth bool) (Lease[T], refusal) {
	n := len(t.members)
	for attempt := range n {
		b := t.members[t.picker.Pick(hash, attempt)]
		if tried != nil && tried.has(b.core) {
			continue
		}
		if tk, r := b.acquire(ignoreHealth); r == admitted {
			return Lease[T]{b: b, t: tk}, admitted
		}
	}
	return scan(t.members, hash, tried, ignoreHealth)
}

// scan tries every backend of bs not in tried once, starting at a place
// that hash chooses. It returns saturated if none took the pick and one of
// them refused because it was full.
func scan[T any](bs []*Backend[T], hash uint64, tried *Request[T], ignoreHealth bool) (Lease[T], refusal) {
	n := len(bs)
	res := refused
	if n == 0 {
		return Lease[T]{}, res
	}
	start := int(hash % uint64(n))
	for k := range n {
		i := start + k
		if i >= n {
			i -= n
		}
		b := bs[i]
		if tried != nil && tried.has(b.core) {
			continue
		}
		t, r := b.acquire(ignoreHealth)
		switch r {
		case admitted:
			return Lease[T]{b: b, t: t}, admitted
		case saturated:
			res = saturated
		case refused:
		}
	}
	return Lease[T]{}, res
}

// Do picks a backend, calls fn with it and releases the lease with
// OutcomeOf of fn's error, also when fn panics, in which case the outcome
// is Failure and the panic continues. When the outcome is Failure and ctx
// is not done, Do retries on another backend as Config.Retry allows. It
// returns nil once fn succeeds, fn's last error if fn failed, and
// otherwise ctx's or the first pick's error.
func (p *Pool[T]) Do(ctx context.Context, fn func(*Backend[T]) error) error {
	return p.DoKey(ctx, "", fn)
}

// DoKey is like Do with backends picked for key, as with PickKey.
func (p *Pool[T]) DoKey(ctx context.Context, key string, fn func(*Backend[T]) error) error {
	r := p.Request(key)
	return r.loop(ctx, func(l Lease[T]) (bool, error) { return runBackend(l, fn) })
}

// Backend returns the member with the given ID.
func (p *Pool[T]) Backend(id string) (*Backend[T], bool) {
	p.mu.Lock()
	b, ok := p.byID[id]
	p.mu.Unlock()
	return b, ok
}

// Backends returns the members in order.
func (p *Pool[T]) Backends() []*Backend[T] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.order)
}

// Len returns the number of members.
func (p *Pool[T]) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.order)
}

// Available returns the number of members that are eligible for new
// picks, as the routing view sees them: not draining, not offline, not
// down and not ejected, in every tier. A half-open member counts although
// it takes only its trial requests, and a full one although it takes no
// pick until a lease is released. Available is zero while the pool is empty or closed and in
// panic mode, when Pick, under FallbackAll, still picks among members that
// are not eligible; a readiness check that should fail in panic mode tests
// Available() > 0.
func (p *Pool[T]) Available() int {
	v := p.view.Load()
	if v.ignoreHealth {
		return 0
	}
	return v.eligible
}

// Status returns a snapshot of every member, in order.
func (p *Pool[T]) Status() []Status {
	bs := p.Backends()
	s := make([]Status, len(bs))
	for i, b := range bs {
		s[i] = b.Status()
	}
	return s
}

// Clock returns the pool's time source, for packages that measure the
// latencies they observe on its leases.
func (p *Pool[T]) Clock() Clock { return p.clock }

// Done returns a channel that is closed when Close is called. The pool
// starts no goroutines but the short-lived one that finishes a rebuild a
// committer handed off, which builds nothing once Close has begun, so
// nothing of it starts user code after that, and
// helpers attached to the pool, such as a health checker started with
// health.Watch, stop with it.
func (p *Pool[T]) Done() <-chan struct{} { return p.closedCh }

// Close stops the passive health and slow-start timers and ends all
// subscriptions: no subscriber call starts once Close has begun, and one
// in progress may finish. Close does not wait: a timer that has already
// fired may still finish its own bookkeeping, but runs no Balancer, Clock
// or subscriber code, and a rebuild already in progress publishes nothing.
// Close may be called from anywhere, including a subscriber or a Balancer.
// Picks fail with ErrClosed once Close returns and updates are refused;
// leases already issued may still be released. Close is idempotent.
func (p *Pool[T]) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.isClosed.Store(true)
	p.view.Store(&view[T]{err: ErrClosed})
	close(p.closedCh)
	for _, s := range p.subs {
		s.off.Store(true) // no event starts after Close, even one already queued
	}
	p.subs = nil
	members := p.order
	p.mu.Unlock()
	for _, b := range members {
		b.core.det.Close()
	}
	p.rampMu.Lock()
	stop := p.rampStop
	p.rampStop = nil
	p.rampMu.Unlock()
	if stop != nil {
		stop()
	}
}
