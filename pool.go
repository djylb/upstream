package upstream

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Errors returned by Pool.
var (
	// ErrNoBackends means the pool has no members.
	ErrNoBackends = errors.New("upstream: no backends")

	// ErrUnavailable means no member can take traffic now: every one is
	// unhealthy, ejected, draining or out of half-open trial slots.
	ErrUnavailable = errors.New("upstream: no backend available")

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

	// Weight is the backend's relative capacity, from 0 to MaxWeight. Zero
	// means 1; to stop sending traffic to a backend, drain it.
	Weight int

	// Value is what callers connect to, such as an address.
	Value T
}

// Fallback says what a Pool does when no backend is eligible.
type Fallback uint8

const (
	// FallbackNone makes Pick fail with ErrUnavailable.
	FallbackNone Fallback = iota

	// FallbackAll makes Pick choose among all members that are not
	// draining, ignoring active and passive health. This is panic mode: a
	// health signal that marks every backend down is more often wrong than
	// right, and degraded service beats none. Leases picked in panic mode
	// still report outcomes.
	FallbackAll
)

// Config configures a Pool. The zero value is a pool with smooth weighted
// round-robin, no passive health checking, no slow start and FallbackNone.
type Config struct {
	// Balancer chooses backends. Nil means WeightedRoundRobin.
	Balancer Balancer

	// Clock is the time source. Nil means SystemClock. Outlier.Clock, if
	// set, takes precedence for passive health checking.
	Clock Clock

	// Outlier configures passive health checking, which is disabled by
	// default.
	Outlier OutlierConfig

	// SlowStart, if positive, ramps up the traffic of a backend that joins
	// the pool or becomes healthy again: its weight grows linearly from a
	// small fraction to its full value over SlowStart, so a cold backend is
	// not flooded. The pool rebuilds its view 16 times over the ramp. With
	// ConsistentHash the ramp moves keys to the backend gradually.
	SlowStart time.Duration

	// Fallback says what to do when no backend is eligible.
	Fallback Fallback
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
// backend is eligible for its trial requests. When no backend is eligible,
// Config.Fallback decides.
//
// The pool starts no goroutines. It owns the passive health timers and the
// slow-start timer, which Close stops. Pick never locks; Release locks only
// when its outcome ejects or restores a backend, to rebuild the view.
//
// Locking: mu guards membership and is never held while calling a
// Balancer, a Clock or an event subscriber. Inside mu the pool may take
// qmu (the event queue) and an OutlierDetector's mutex to read its status;
// neither calls out while held. pubMu (view publication) and rampMu (the
// slow-start timer) are taken alone.
type Pool[T any] struct {
	view      atomic.Pointer[view[T]]
	balancer  Balancer
	clock     Clock
	outlier   outlierParams
	slowStart time.Duration
	isClosed  atomic.Bool
	rampArmed atomic.Bool
	passive   bool

	mu      sync.Mutex
	byID    map[string]*Backend[T]
	order   []*Backend[T]
	subs    []*subscriber[T] // copy on write
	version uint64
	closed  bool

	qmu      sync.Mutex
	queue    []delivery[T]
	flushing bool

	pubMu     sync.Mutex
	published uint64

	rampMu   sync.Mutex
	rampStop func() bool

	closedCh chan struct{} // closed by Close

	fallback Fallback
}

// view is an immutable routing snapshot.
type view[T any] struct {
	picker       Picker
	err          error // returned when members is empty
	members      []*Backend[T]
	version      uint64
	ignoreHealth bool
}

// New returns an empty pool.
func New[T any](cfg Config) *Pool[T] {
	p := &Pool[T]{
		balancer:  cfg.Balancer,
		clock:     clockOrSystem(cfg.Clock),
		slowStart: max(cfg.SlowStart, 0),
		passive:   cfg.Outlier.Enabled(),
		byID:      make(map[string]*Backend[T]),
		fallback:  cfg.Fallback,
		closedCh:  make(chan struct{}),
	}
	if p.balancer == nil {
		p.balancer = WeightedRoundRobin()
	}
	if cfg.Outlier.Clock == nil {
		cfg.Outlier.Clock = p.clock
	}
	p.outlier = newOutlierParams(cfg.Outlier)
	p.view.Store(&view[T]{err: ErrNoBackends})
	return p
}

func validate[T any](t Target[T]) error {
	switch {
	case t.ID == "":
		return fmt.Errorf("%w: empty ID", ErrInvalidTarget)
	case t.Weight < 0 || t.Weight > MaxWeight:
		return fmt.Errorf("%w: %q: weight %d out of range", ErrInvalidTarget, t.ID, t.Weight)
	}
	return nil
}

func (p *Pool[T]) newBackend(t Target[T], c *core) *Backend[T] {
	if c == nil {
		c = new(core)
		if p.passive {
			c.passive = true
			c.det.init(p.outlier, p.refresh)
		}
		if p.slowStart > 0 {
			c.rampStart.Store(-1)
		}
	}
	return &Backend[T]{core: c, id: t.ID, weight: max(t.Weight, 1), value: t.Value}
}

// Update replaces the pool's membership with targets, in order. Members
// whose ID is in targets keep their health state and counters and take
// the new weight and value; new IDs join as healthy; members missing from
// targets are removed gracefully: they take no new picks while their
// leases finish. Update emits EventRemoved, then EventAdded or
// EventUpdated for each target in order. On error nothing changes.
func (p *Pool[T]) Update(targets []Target[T]) error {
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
	p.order = order
	defer finishRemovals(removed)
	p.unlockAndCommit()
	return nil
}

// Put adds a target, or replaces the member with the same ID, keeping its
// position, health state and counters. It emits EventAdded or
// EventUpdated.
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
	if old == nil {
		p.order = append(p.order, b)
	} else {
		p.order[slices.Index(p.order, old)] = b
	}
	p.unlockAndCommit()
	return nil
}

// putLocked creates or replaces the backend for t in byID and emits the
// event; the caller places it in order.
func (p *Pool[T]) putLocked(t Target[T]) *Backend[T] {
	if old := p.byID[t.ID]; old != nil {
		b := p.newBackend(t, old.core)
		p.byID[t.ID] = b
		p.emitLocked(Event[T]{Kind: EventUpdated, Backend: b, From: old.core.reported, To: old.core.reported})
		return b
	}
	b := p.newBackend(t, nil)
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
// committing, so it runs even if a Balancer or subscriber panics.
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
// backend out of rotation; HealthUp and HealthUnknown keep it in. Reports
// for a backend that has been removed, or replaced by a new member with the
// same ID, are ignored.
func (p *Pool[T]) ReportCheck(b *Backend[T], h Health, r CheckResult) {
	c := b.core
	want := healthFlags(h)
	p.mu.Lock()
	if cur := p.byID[b.id]; p.closed || cur == nil || cur.core != c {
		p.mu.Unlock()
		return
	}
	c.check.Store(&r)
	if c.flags.Load()&(flagUp|flagUnhealthy) == want {
		p.mu.Unlock()
		return
	}
	for {
		old := c.flags.Load()
		if c.flags.CompareAndSwap(old, old&^(flagUp|flagUnhealthy)|want) {
			break
		}
	}
	p.unlockAndCommit()
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
	p.unlockAndCommit()
}

// unlockAndCommit snapshots the view and announces state changes under mu,
// releases mu, then builds and publishes the view and delivers events.
func (p *Pool[T]) unlockAndCommit() {
	p.version++
	v := &view[T]{version: p.version, err: ErrUnavailable}
	switch {
	case p.closed:
		v.err = ErrClosed
	case len(p.order) == 0:
		v.err = ErrNoBackends
	}
	for _, b := range p.order {
		c := b.core
		if s := c.state(); s != c.reported {
			if p.slowStart > 0 && s == StateHealthy {
				c.rampStart.Store(-1)
			}
			p.emitLocked(Event[T]{Kind: EventStateChanged, Backend: b, From: c.reported, To: s, Err: c.cause(s)})
			c.reported = s
		}
		if !p.closed && c.eligible() {
			v.members = append(v.members, b)
		}
	}
	if len(v.members) == 0 && !p.closed && p.fallback == FallbackAll {
		for _, b := range p.order {
			if b.core.flags.Load()&blockAlways == 0 {
				v.members = append(v.members, b)
			}
		}
		v.ignoreHealth = true
	}
	p.mu.Unlock()
	defer p.flush()

	if len(v.members) > 0 {
		cands := make([]Candidate, len(v.members))
		for i, b := range v.members {
			cands[i] = Candidate{InFlight: &b.core.inflight, ID: b.id, Weight: b.weight}
		}
		if p.slowStart > 0 {
			p.applySlowStart(v.members, cands)
		}
		v.picker = p.balancer.Build(cands)
	}
	p.pubMu.Lock()
	if v.version > p.published {
		p.published = v.version
		p.view.Store(v)
	}
	p.pubMu.Unlock()
}

// applySlowStart lowers the candidate weights of backends that are ramping
// up and arms the timer for the next step.
func (p *Pool[T]) applySlowStart(members []*Backend[T], cands []Candidate) {
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
		// A concurrent rebuild may have started the ramp with a later
		// reading of the clock than ours: count that as the start.
		elapsed := max(now-rs, 0)
		if elapsed >= int64(p.slowStart) {
			c.rampStart.CompareAndSwap(rs, 0)
			continue
		}
		fraction[i] = float64(elapsed) / float64(p.slowStart)
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
	p.armRamp(max(p.slowStart/rampScale, time.Millisecond))
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
func (p *Pool[T]) Pick() (Lease[T], error) { return p.PickHash(rand.Uint64()) }

// PickKey leases a backend chosen for key, such as a client address or a
// session ID. With ConsistentHash the same key keeps mapping to the same
// backend while it is eligible; other balancers ignore the key.
func (p *Pool[T]) PickKey(key string) (Lease[T], error) { return p.PickHash(hashString(key)) }

// PickHash is like PickKey for callers that hash keys themselves.
func (p *Pool[T]) PickHash(hash uint64) (Lease[T], error) {
	v := p.view.Load()
	n := len(v.members)
	if n == 0 {
		return Lease[T]{}, v.err
	}
	for attempt := range n {
		b := v.members[v.picker.Pick(hash, attempt)]
		if t, ok := b.core.acquire(v.ignoreHealth); ok {
			return Lease[T]{b: b, t: t}, nil
		}
	}
	// The balancer kept choosing backends that could not take the request,
	// such as a half-open one out of trial slots: try every member once.
	start := int(hash % uint64(n))
	for k := range n {
		b := v.members[(start+k)%n]
		if t, ok := b.core.acquire(v.ignoreHealth); ok {
			return Lease[T]{b: b, t: t}, nil
		}
	}
	return Lease[T]{}, ErrUnavailable
}

// Do picks a backend, calls fn with it and releases the lease with
// OutcomeOf of fn's error, also when fn panics, in which case the outcome
// is Failure and the panic continues. It returns Pick's error or fn's.
func (p *Pool[T]) Do(fn func(*Backend[T]) error) error {
	l, err := p.Pick()
	if err != nil {
		return err
	}
	return l.run(fn)
}

// DoKey is like Do with PickKey.
func (p *Pool[T]) DoKey(key string, fn func(*Backend[T]) error) error {
	l, err := p.PickKey(key)
	if err != nil {
		return err
	}
	return l.run(fn)
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

// Status returns a snapshot of every member, in order.
func (p *Pool[T]) Status() []Status {
	bs := p.Backends()
	s := make([]Status, len(bs))
	for i, b := range bs {
		s[i] = b.Status()
	}
	return s
}

// Closed returns a channel that is closed when Close is called, so that
// helpers attached to the pool, such as a health checker started with
// health.Watch, can stop with it.
func (p *Pool[T]) Closed() <-chan struct{} { return p.closedCh }

// Close stops the passive health and slow-start timers and ends all
// subscriptions: no subscriber call starts once Close has begun, and one
// in progress may finish. Close does not wait: a timer that has already
// fired may still finish its own bookkeeping, but runs no Balancer, Clock
// or subscriber code. Close may be called from anywhere, including a
// subscriber or a Balancer. Later
// picks fail with ErrClosed and updates are refused; leases already issued
// may still be released. Close is idempotent.
func (p *Pool[T]) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.isClosed.Store(true)
	close(p.closedCh)
	for _, s := range p.subs {
		s.off.Store(true) // no event starts after Close, even one already queued
	}
	p.subs = nil
	members := slices.Clone(p.order)
	defer func() {
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
	}()
	p.unlockAndCommit()
}
