package upstream

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"
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

	// ErrUnknownBackend means Acquire was given an ID that is not a
	// member.
	ErrUnknownBackend = errors.New("upstream: unknown backend")

	// ErrInvalidTarget means a Target has an empty or duplicate ID or a
	// weight out of range.
	ErrInvalidTarget = errors.New("upstream: invalid target")
)

// MaxWeight bounds a Target or Candidate weight.
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

	// Cost is what reaching the backend costs the caller, in the caller's
	// units, such as a path latency in microseconds; lower is preferred.
	// Within a tier, picks go to the members whose cost is close to the
	// lowest cost of the members that can take them, by
	// Config.CostTolerance, and are balanced among those; the dearer
	// members take picks only when none of the cheaper ones can, as with
	// tiers, except that a full cost band always passes the pick on. The
	// default, 0 for every member, leaves the tier one band. A cost is
	// part of the target: Put or Sync a new one when it changes.
	Cost uint64

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

	// Clock is the time source. Nil means timeshim.Real.
	Clock timeshim.Clock

	// Equal reports whether two target values mean the same backend, so
	// that Sync and Put keep a member whose weight and value did not
	// change: same Backend, no EventUpdated, no rebuild. Nil compares
	// values with ==, through reflection, when they are comparable, and
	// treats other values, such as slices, as always changed. Equal runs
	// outside the pool lock. Replacing the compared backend or installing
	// a comparator with SetConfig can repeat that comparison.
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

	// CostTolerance, in percent, is how much dearer than the cheapest
	// member of a band a member of the same tier may be and still share
	// its band (Target.Cost): a band takes the cheapest remaining member
	// and every member whose cost is at most CostTolerance percent above
	// it, and the members left form the next bands the same way. Zero
	// puts only equal costs in one band; negative values mean zero.
	CostTolerance int
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
	costTol   uint64 // CostTolerance
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
// A health, drain or offline change updates only the tier of the backend
// that changed, by a [Delta] that an [Updater] applies to its Picker; the
// other tiers keep their member lists and Pickers. Membership changes
// compare the pool once, in linear time.
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
	pendingRamp  time.Time // address marks a pending slow start
	view         atomic.Pointer[view[T]]
	retry        atomic.Pointer[retryParams]
	equalVersion uint64 // changes when SetConfig installs a comparator
	guard        ejectionGuard
	clock        timeshim.Clock

	mu       sync.Mutex
	cfg      settings[T]
	byID     idIndex[T] // written under mu, read without
	order    []*Backend[T]
	subs     []*subscriber[T]  // copy on write
	groups   map[int]*group[T] // members by priority
	prios    []int             // the keys of groups, ascending
	touched  []*Backend[T]     // members whose state may have changed since the last commit
	dirty    bool              // the view is out of date
	building bool              // a goroutine is building the view
	closed   bool
	all      bool // announce the state of every member: settings or membership changed

	isClosed  atomic.Bool // Close has been called; read without mu
	rampArmed atomic.Bool // a slow-start timer is pending
	flushing  bool        // guarded by qmu

	// Owned by the building goroutine.
	built    []builtTier[T] // the latest Picker built for each priority, by ascending priority
	builtGen uint64         // settings.gen of built

	qmu   sync.Mutex
	queue []delivery[T]

	rampMu    sync.Mutex
	rampTimer timeshim.Timer

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

// A tier is the members of one cost band of one priority and their
// Picker.
type tier[T any] struct {
	picker   Picker
	ranking  *tierRanking
	members  []*Backend[T] // the candidates of picker
	priority int
	band     int // the cost band within the priority, from 0
}

// A custom Ranker may use the tier's workspace for one call. Overlapping
// calls follow the same order through Picker.Pick.
type tierRanking struct {
	ranker Ranker
	order  []int
	busy   atomic.Bool
}

// builtTier is a Picker a build made, kept for the next build of its
// priority to reuse or rebuild.
type builtTier[T any] struct {
	picker   Picker
	ranking  *tierRanking
	cands    []Candidate
	priority int
	band     int
	members  []*Backend[T] // the tier's members, which the next build compares by identity
	ramping  bool          // slow start lowered weights: rebuild at the next step
}

// A group is the members of one priority, with the lists a view takes
// from them. A list is replaced, never modified, so views share it; each
// update also records the Delta from the list before it, so that a build
// can update the tier's candidates and Picker instead of making them
// again. A state change updates the lists through pend, in time
// O(changes · log n) plus copying; a membership change compares the whole
// group once, in time O(n), like the change itself.
type group[T any] struct {
	members   []*Backend[T] // in membership order; their slot is their index once placed
	elig      []*Backend[T] // eligible members
	open      []*Backend[T] // members that are not draining or offline
	eligPrev  []*Backend[T] // elig before the last update
	openPrev  []*Backend[T] // open before the last update
	eligDelta *Delta        // from eligPrev to elig; nil if not known
	openDelta *Delta        // from openPrev to open; nil if not known
	pend      []*Backend[T] // members whose state may have changed
	rediff    bool          // members changed: compare every member
	reweigh   bool          // a member starts a slow start: make the tier again
	eligBands bandCache[T]  // elig split by cost
	openBands bandCache[T]  // open split by cost
}

// A bandCache holds the cost bands of a group list, kept while the list
// and the tolerance stay the same, so that the bands, like the list, are
// the same slices from one view to the next and their Pickers are kept.
type bandCache[T any] struct {
	src   []*Backend[T]
	tol   uint64
	bands [][]*Backend[T]
	one   [1][]*Backend[T] // bands of a list of one cost, without allocating
}

// get returns the cost bands of list under tolerance tol.
func (c *bandCache[T]) get(list []*Backend[T], tol uint64) [][]*Backend[T] {
	if c.bands != nil && c.tol == tol && sameList(c.src, list) {
		return c.bands
	}
	c.src, c.tol = list, tol
	if uniformCost(list) {
		c.one[0] = list
		c.bands = c.one[:min(len(list), 1)]
		return c.bands
	}
	c.one[0] = nil
	c.bands = costBands(list, tol)
	return c.bands
}

// uniformCost reports whether the members of list have one cost.
func uniformCost[T any](list []*Backend[T]) bool {
	return len(list) == 0 || !slices.ContainsFunc(list[1:], func(b *Backend[T]) bool { return b.cost != list[0].cost })
}

// costBands splits list into cost bands, cheapest first, each in the
// order of list: a band starts at the cheapest member not yet placed and
// takes the members whose cost is at most tol percent above it. A list
// of one cost is one band, list itself; an empty list has none.
func costBands[T any](list []*Backend[T], tol uint64) [][]*Backend[T] {
	if len(list) == 0 {
		return nil
	}
	if uniformCost(list) {
		return [][]*Backend[T]{list}
	}
	costs := make([]uint64, len(list))
	for i, b := range list {
		costs[i] = b.cost
	}
	slices.Sort(costs)
	// starts holds the lowest cost of every band.
	starts := costs[:1]
	limit := bandLimit(costs[0], tol)
	for _, c := range costs[1:] {
		if c > limit {
			starts = append(starts, c)
			limit = bandLimit(c, tol)
		}
	}
	bands := make([][]*Backend[T], len(starts))
	for _, b := range list {
		k, found := slices.BinarySearch(starts, b.cost)
		if !found {
			k--
		}
		bands[k] = append(bands[k], b)
	}
	return bands
}

// bandLimit is the highest cost of the band that starts at c: c plus tol
// percent of it, saturating.
func bandLimit(c, tol uint64) uint64 {
	hi, lo := bits.Mul64(c, tol)
	if hi >= 100 {
		return math.MaxUint64
	}
	extra, _ := bits.Div64(hi, lo, 100)
	sum, carry := bits.Add64(c, extra, 0)
	if carry != 0 {
		return math.MaxUint64
	}
	return sum
}

func isOpen[T any](b *Backend[T]) bool { return b.core.flags.Load()&blockPick == 0 }

func isElig[T any](b *Backend[T]) bool { return isOpen(b) && b.core.eligible() }

// updateLocked brings the lists of g up to date, recording their deltas.
func (g *group[T]) updateLocked() {
	switch {
	case g.rediff:
		open := make([]*Backend[T], 0, len(g.members))
		all := true // every open member is eligible: one list serves both
		for _, b := range g.members {
			if isOpen(b) {
				open = append(open, b)
				all = all && b.core.eligible()
			}
		}
		elig := open
		if !all {
			elig = nil
			for _, b := range open {
				if b.core.eligible() {
					elig = append(elig, b)
				}
			}
		}
		switch {
		case slices.Equal(elig, g.elig):
			elig = g.elig // unchanged: the tier keeps its Picker
		case all && slices.Equal(open, g.open):
			elig = g.open
		}
		if all {
			open = elig
		} else if slices.Equal(open, g.open) {
			open = g.open
		}
		eligDelta := diffLists(g.elig, elig)
		openDelta := eligDelta
		if !sameList(g.open, g.elig) || !sameList(open, elig) {
			openDelta = diffLists(g.open, open)
		}
		g.eligPrev, g.eligDelta, g.elig = g.elig, eligDelta, elig
		g.openPrev, g.openDelta, g.open = g.open, openDelta, open
		for i, b := range g.members {
			b.slot = i
		}
		g.rediff = false
	case len(g.pend) > 0:
		slices.SortFunc(g.pend, func(a, b *Backend[T]) int { return a.slot - b.slot })
		g.pend = slices.Compact(g.pend)
		g.eligPrev, g.eligDelta = g.elig, nil
		g.openPrev, g.openDelta = g.open, nil
		g.elig, g.eligDelta = applyPending(g.elig, g.pend, isElig[T])
		g.open, g.openDelta = applyPending(g.open, g.pend, isOpen[T])
		if len(g.open) == len(g.elig) {
			g.open = g.elig // equal contents: one list serves both, openDelta still applies
		}
	default:
		return
	}
	clear(g.pend)
	g.pend = g.pend[:0]
}

// applyPending returns list with the members of pend, sorted by slot, in
// or out of it as want says, and the Delta from list; list itself if
// nothing changed.
func applyPending[T any](list, pend []*Backend[T], want func(*Backend[T]) bool) ([]*Backend[T], *Delta) {
	var d *Delta
	var adds []*Backend[T]
	removed := 0
	for _, b := range pend {
		q, _ := slices.BinarySearchFunc(list, b.slot, func(x *Backend[T], slot int) int { return x.slot - slot })
		in := q < len(list) && list[q] == b
		if in == want(b) {
			continue
		}
		if d == nil {
			d = new(Delta)
		}
		if in {
			d.Removed = append(d.Removed, q)
			removed++
		} else {
			d.Added = append(d.Added, q-removed+len(adds))
			adds = append(adds, b)
		}
	}
	if d == nil {
		return list, &Delta{}
	}
	k := 0
	return splice(list, d, func(int) *Backend[T] { k++; return adds[k-1] }), d
}

// diffLists returns the Delta from a to b, two lists of one group, each in
// slot order with slots as they were before a membership change; a member
// new to the group has slot -1. It returns nil if b does not keep the
// order of a, as after a Sync that reorders members.
//
// An empty first list has no Picker to update, so diffLists returns nil.
func diffLists[T any](a, b []*Backend[T]) *Delta {
	if len(a) == 0 {
		return nil
	}
	d := new(Delta)
	i, last := 0, -1
	for j, x := range b {
		if x.slot < 0 {
			d.Added = append(d.Added, j)
			continue
		}
		if x.slot < last {
			return nil
		}
		last = x.slot
		for i < len(a) && a[i].slot < x.slot {
			d.Removed = append(d.Removed, i)
			i++
		}
		if i < len(a) && a[i] == x {
			i++
			continue
		}
		d.Added = append(d.Added, j)
	}
	for ; i < len(a); i++ {
		d.Removed = append(d.Removed, i)
	}
	return d
}

// New returns an empty pool.
func New[T any](cfg Config) *Pool[T] {
	p := &Pool[T]{
		clock:    timeshim.Or(cfg.Clock),
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
	s.costTol = uint64(max(cfg.CostTolerance, 0))
	for _, g := range p.groups {
		g.rediff = true
	}
	p.all = true
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
	p.equalVersion++
	old := p.cfg.outlier
	op := newOutlierParams(cfg.Outlier, p.clock, old.epoch)
	op.seq = old.seq + 1
	p.cfg.outlier = op
	rp := newRetryParams(cfg.Retry, p.clock, p.retry.Load().epoch)
	if oldRetry := p.retry.Load(); oldRetry.window == rp.window {
		rp.counts = oldRetry.counts
	}
	p.retry.Store(rp)
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
		id := t.ID
		c.det.init(p.cfg.outlier, func() { p.refreshID(id) }, &p.guard)
		if p.cfg.slowStart > 0 {
			c.rampStart.Store(&p.pendingRamp)
		}
	}
	return &Backend[T]{core: c, id: t.ID, weight: max(t.Weight, 1), priority: t.Priority, slot: -1, limit: int64(t.MaxInFlight), cost: t.Cost, value: t.Value}
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
	same, err := p.compareAndLock(targets)
	if err != nil {
		return err
	}
	var removed []removal
	for _, b := range p.order {
		if _, keep := seen[b.id]; !keep {
			removed = append(removed, p.removeLocked(b))
		}
	}
	order := make([]*Backend[T], 0, len(targets))
	for i, t := range targets {
		order = append(order, p.putLocked(t, same[i]))
	}
	if len(removed) == 0 && slices.Equal(order, p.order) {
		p.mu.Unlock()
		return nil
	}
	p.order = order
	p.regroupLocked()
	defer finishRemovals(removed)
	p.unlockAndCommit()
	return nil
}

// Put adds a target, or replaces the member with the same ID, keeping its
// position, health state and counters. It emits EventAdded or
// EventUpdated; a Put that changes none of the weight, priority, cap,
// cost and value of a member does nothing.
func (p *Pool[T]) Put(t Target[T]) error {
	if err := validate(t); err != nil {
		return err
	}
	same, err := p.compareAndLock([]Target[T]{t})
	if err != nil {
		return err
	}
	old := p.byID.load(t.ID)
	b := p.putLocked(t, same[0])
	switch {
	case b == old:
		p.mu.Unlock()
		return nil
	case old == nil:
		p.order = append(p.order, b)
	default:
		p.order[slices.Index(p.order, old)] = b
	}
	p.regroupLocked()
	p.unlockAndCommit()
	return nil
}

// compareAndLock compares immutable versions outside mu, then keeps mu
// locked only if the inputs to the comparison are still current.
func (p *Pool[T]) compareAndLock(ts []Target[T]) ([]bool, error) {
	olds := make([]*Backend[T], len(ts))
	same := make([]bool, len(ts))
	var pending []int
	p.mu.Lock()
	version := p.equalVersion
	for !p.closed {
		pending = pending[:0]
		for i, t := range ts {
			old := p.byID.load(t.ID)
			if version == p.equalVersion && old == olds[i] {
				continue
			}
			olds[i], same[i] = old, false
			if old != nil && old.weight == max(t.Weight, 1) && old.priority == t.Priority && old.limit == int64(t.MaxInFlight) && old.cost == t.Cost {
				pending = append(pending, i)
			}
		}
		if len(pending) == 0 {
			return same, nil
		}
		version = p.equalVersion
		equal := p.cfg.equal
		p.mu.Unlock()
		for _, i := range pending {
			same[i] = equal(olds[i].value, ts[i].Value)
		}
		p.mu.Lock()
	}
	p.mu.Unlock()
	return nil, ErrClosed
}

// putLocked creates or replaces the backend for t in byID and emits the
// event, or returns the current backend if t changes nothing; the caller
// places it in order.
func (p *Pool[T]) putLocked(t Target[T], same bool) *Backend[T] {
	if old := p.byID.load(t.ID); old != nil {
		if same {
			return old
		}
		b := p.newBackendLocked(t, old.core)
		p.byID.store(b)
		p.emitLocked(Event[T]{Kind: EventUpdated, Backend: b, From: old.core.reported, To: old.core.reported})
		return b
	}
	b := p.newBackendLocked(t, nil)
	p.byID.store(b)
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
	p.byID.delete(b.id)
	from := b.core.reported
	d := b.core.drainLocked(flagRemoved | flagDraining)
	b.core.reported = StateRemoved
	p.emitLocked(Event[T]{Kind: EventRemoved, Backend: b, From: from, To: StateRemoved})
	return removal{c: b.core, d: d}
}

// Remove removes the backend with the given ID gracefully: new picks avoid
// it at once, and leases already issued finish normally. The returned
// Drain is done when the last of them is released. Remove reports false if
// the pool is closed or there is no such member.
func (p *Pool[T]) Remove(id string) (*Drain, bool) {
	p.mu.Lock()
	b := p.byID.load(id)
	if p.closed || b == nil {
		p.mu.Unlock()
		return nil, false
	}
	r := p.removeLocked(b)
	p.order = slices.DeleteFunc(p.order, func(x *Backend[T]) bool { return x == b })
	p.regroupLocked()
	defer finishRemovals([]removal{r})
	p.unlockAndCommit()
	return r.d, true
}

// Drain stops new picks of the backend with the given ID while it stays a
// member, for maintenance or before removal. The returned Drain is done
// when the backend has no leases in flight, or canceled when Resume ends
// the drain first. Draining a backend that is already draining returns the
// same Drain. Drain reports false for a closed pool or an unknown ID.
func (p *Pool[T]) Drain(id string) (*Drain, bool) {
	p.mu.Lock()
	b := p.byID.load(id)
	if p.closed || b == nil {
		p.mu.Unlock()
		return nil, false
	}
	d := b.core.drainLocked(flagDraining)
	b.core.checkDrained(d)
	p.touchLocked(b)
	p.unlockAndCommit()
	return d, true
}

// Resume ends a drain started by Drain; the Drain reports Canceled unless
// it had already completed. Resume reports false for a closed pool or an unknown ID.
func (p *Pool[T]) Resume(id string) bool {
	p.mu.Lock()
	b := p.byID.load(id)
	if p.closed || b == nil {
		p.mu.Unlock()
		return false
	}
	b.core.flags.And(^flagDraining)
	if d := b.core.drain.Swap(nil); d != nil {
		defer d.finish(true)
	}
	p.touchLocked(b)
	p.unlockAndCommit()
	return true
}

// ReportCheck records the result of an active health check of b and sets
// the default source's verdict. The checker applies its rise and fall
// thresholds before reporting. A Down verdict excludes b from normal
// rotation. FallbackAll may bypass health. Other sources retain their verdicts.
// Reports for a backend that has been removed, or replaced by a new
// member with the same ID, are ignored; ReportCheck reports whether b was
// a current member of an open pool.
func (p *Pool[T]) ReportCheck(b *Backend[T], h Health, r CheckResult) bool {
	return p.reportFrom(nil, b, h, &r)
}

// CheckSource identifies one independent active health writer. Its zero
// value is ready to use. Use the same pointer for the writer's lifetime.
type CheckSource struct{ _ byte }

// ReportCheckFrom records source's verdict and the latest observation.
// Any Down excludes b from normal rotation. FallbackAll may bypass health.
// Otherwise, any Up takes precedence over Unknown.
// An unrecognized health value withdraws the source verdict.
// A nil source identifies the writer used by ReportCheck.
func (p *Pool[T]) ReportCheckFrom(source *CheckSource, b *Backend[T], h Health, r CheckResult) bool {
	return p.reportFrom(source, b, h, &r)
}

// ClearCheckFrom withdraws only source's verdict, keeping the last observation.
func (p *Pool[T]) ClearCheckFrom(source *CheckSource, b *Backend[T]) bool {
	return p.reportFrom(source, b, HealthUnknown, nil)
}

func (p *Pool[T]) reportFrom(source *CheckSource, b *Backend[T], h Health, r *CheckResult) bool {
	if !p.lockMember(b) {
		return false
	}
	c := b.core
	if h != HealthUp && h != HealthDown {
		h = HealthUnknown
	}
	switch {
	case source == nil:
		c.directHealth = h
	case h == HealthUnknown:
		delete(c.checkSources, source)
	default:
		if c.checkSources == nil {
			c.checkSources = make(map[*CheckSource]Health)
		}
		c.checkSources[source] = h
	}
	h = c.directHealth
	for _, v := range c.checkSources {
		if v == HealthDown {
			h = HealthDown
			break
		}
		if h == HealthUnknown {
			h = v
		}
	}
	want := healthFlags(h)
	if r != nil {
		c.check.Store(r)
	}
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
	p.touchLocked(b)
	p.unlockAndCommit()
	return true
}

// lockMember locks mu if b is the current version, or an earlier one, of a
// member of the open pool, and reports whether it did.
func (p *Pool[T]) lockMember(b *Backend[T]) bool {
	if b == nil {
		return false
	}
	p.mu.Lock()
	if cur := p.byID.load(b.id); p.closed || cur == nil || cur.core != b.core {
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
	p.touchLocked(b)
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
	p.touchLocked(b)
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

// refreshID rebuilds the view after a passive health transition of the
// member with the given ID.
func (p *Pool[T]) refreshID(id string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if b := p.byID.load(id); b != nil {
		p.touchLocked(b)
	}
	p.unlockAndCommit()
}

// touchLocked records that the state of b, or of the member with its ID,
// may have changed: the next commit announces it and recomputes its tier.
func (p *Pool[T]) touchLocked(b *Backend[T]) {
	p.touched = append(p.touched, b)
}

// regroupLocked brings the groups in line with order after a membership
// change, marking dirty the groups whose members changed. It costs O(n),
// like the membership change itself.
func (p *Pool[T]) regroupLocked() {
	if p.groups == nil {
		p.groups = make(map[int]*group[T])
	}
	var next map[int][]*Backend[T]
	if uniform := len(p.order) > 0 && !slices.ContainsFunc(p.order, func(b *Backend[T]) bool { return b.priority != p.order[0].priority }); uniform {
		next = map[int][]*Backend[T]{p.order[0].priority: slices.Clone(p.order)} // one tier: no regrouping
	} else {
		next = make(map[int][]*Backend[T], len(p.groups))
		for _, b := range p.order {
			next[b.priority] = append(next[b.priority], b)
		}
	}
	for prio := range p.groups {
		if _, ok := next[prio]; !ok {
			delete(p.groups, prio)
		}
	}
	for prio, ms := range next {
		g := p.groups[prio]
		if g == nil {
			g = new(group[T])
			p.groups[prio] = g
		}
		if !slices.Equal(g.members, ms) {
			g.members = ms
			g.rediff = true
		}
	}
	p.prios = p.prios[:0]
	for prio := range p.groups {
		p.prios = append(p.prios, prio)
	}
	slices.Sort(p.prios)
	p.all = true // states are announced for every member, in order
}

// refresh rebuilds the view after a slow-start step.
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
	if p.all {
		for _, b := range p.order {
			p.announceLocked(b)
		}
	}
	for _, t := range p.touched {
		if b := p.byID.load(t.id); b != nil {
			g := p.groups[b.priority]
			g.pend = append(g.pend, b)
			if !p.all {
				p.announceLocked(b)
			}
		}
	}
	clear(p.touched)
	p.touched, p.all = p.touched[:0], false
	defer p.flush()
	if p.building {
		p.mu.Unlock()
		return
	}
	p.building = true
	p.buildLoop(commitBuilds)
}

// announceLocked emits EventStateChanged if the state of b differs from
// the one last announced.
func (p *Pool[T]) announceLocked(b *Backend[T]) {
	c := b.core
	if s := c.state(); s != c.reported {
		if p.cfg.slowStart > 0 && s == StateHealthy {
			c.rampStart.Store(&p.pendingRamp)
			// The member's weight changes while its list may not: make
			// the tier again.
			p.groups[b.priority].reweigh = true
		}
		p.emitLocked(Event[T]{Kind: EventStateChanged, Backend: b, From: c.reported, To: s, Err: c.cause(s)})
		c.reported = s
	}
}

// commitBuilds bounds the views a committer builds: its own and one more
// for the commits that coalesced into it while it built. Commits that
// come during the second build are handed off to a goroutine of the pool,
// so that a request whose Lease.Release commits an ejection is not held
// up by other goroutines' changes.
const commitBuilds = 2

// snapshot is what a build needs from the pool's state.
type snapshot[T any] struct {
	v         *view[T]        // its tiers have members, and the Pickers to build
	changes   []tierChange[T] // how each tier of v changed
	balancer  Balancer
	gen       uint64
	slowStart time.Duration
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

// snapshotLocked selects the members of the next view. It recomputes the
// lists of the dirty groups only; the tiers of the others are those of the
// previous view.
func (p *Pool[T]) snapshotLocked() snapshot[T] {
	s := snapshot[T]{v: &view[T]{err: ErrUnavailable, overflow: p.cfg.overflow}, balancer: p.cfg.balancer, gen: p.cfg.gen, slowStart: p.cfg.slowStart}
	v := s.v
	if len(p.order) == 0 {
		v.err = ErrNoBackends
	}
	p.guard.members.Store(int32(min(len(p.order), math.MaxInt32)))
	elig, open := 0, 0
	for _, prio := range p.prios {
		g := p.groups[prio]
		g.updateLocked()
		elig += len(g.elig)
		open += len(g.open)
	}
	v.eligible = elig
	if p.cfg.fallback == FallbackAll && open > 0 && (elig == 0 || int64(elig)*100 < int64(p.cfg.threshold)*int64(open)) {
		v.ignoreHealth = true
		v.eligible = open
	}
	v.tiers = make([]tier[T], 0, len(p.prios))
	tol := p.cfg.costTol
	for _, prio := range p.prios {
		g := p.groups[prio]
		ch := tierChange[T]{base: g.eligPrev, d: g.eligDelta, remake: g.reweigh}
		ms, bands := g.elig, &g.eligBands
		if v.ignoreHealth {
			ms, bands, ch = g.open, &g.openBands, tierChange[T]{base: g.openPrev, d: g.openDelta, remake: g.reweigh}
		}
		g.eligPrev, g.openPrev = nil, nil
		g.eligDelta, g.openDelta = nil, nil
		if v.ignoreHealth {
			g.eligBands = bandCache[T]{}
		} else if p.cfg.fallback != FallbackAll {
			g.openBands = bandCache[T]{}
		}
		g.reweigh = false
		bs := bands.get(ms, tol)
		if len(bs) > 1 {
			// The group's delta is from its whole list, not a band's.
			ch.base, ch.d = nil, nil
		}
		for k, b := range bs {
			v.tiers = append(v.tiers, tier[T]{members: b, priority: prio, band: k})
			s.changes = append(s.changes, ch)
		}
		if p.cfg.fallback == FallbackAll && !v.ignoreHealth {
			fb := g.openBands.get(g.open, tol)
			for k, b := range fb {
				v.fallback = append(v.fallback, tier[T]{members: b, priority: prio, band: k})
			}
		}
	}
	return s
}

// A tierChange is how a tier's members changed: base with d applied is
// the tier's list. d is nil when the change is not known.
type tierChange[T any] struct {
	base   []*Backend[T]
	d      *Delta
	remake bool // weights changed: make the candidates again
}

// sameList reports whether a and b are the same list of a group, which is
// never modified once made.
func sameList[T any](a, b []*Backend[T]) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

// build makes the Pickers of the tiers of s.v, without mu. A tier whose
// member list is the one the Picker built last for its priority by the
// same Balancer was made from keeps that Picker, unless slow start is
// ramping its weights. A tier whose list derives from that one by a known
// Delta updates the candidates by copying, and an Updater updates its
// Picker with the Delta, so a change costs time in proportion to what
// changed, plus copying. Otherwise, the candidates are made again; a tier
// whose candidates are equal to the Picker's keeps it, an Updater updates
// it without a Delta, and other Balancers build a new one. Under slow
// start, changed tiers are made again. The Pickers are kept only if every
// tier builds.
func (p *Pool[T]) build(s snapshot[T]) {
	v := s.v
	if len(v.tiers) == 0 {
		p.built = nil
		return
	}
	prev := p.built
	if p.builtGen != s.gen {
		prev = nil
	}
	upd, updater := s.balancer.(Updater)
	built := make([]builtTier[T], len(v.tiers))
	for i := range v.tiers {
		t := &v.tiers[i]
		for len(prev) > 0 && (prev[0].priority < t.priority || prev[0].priority == t.priority && prev[0].band < t.band) {
			prev = prev[1:]
		}
		ok := len(prev) > 0 && prev[0].priority == t.priority && prev[0].band == t.band
		var old builtTier[T]
		if ok {
			old = prev[0]
		}
		ch := s.changes[i]
		if ok && !old.ramping && !ch.remake && sameList(old.members, t.members) {
			t.picker = old.picker
			built[i] = old
			continue
		}
		if ok && s.slowStart <= 0 && !old.ramping && ch.d != nil && sameList(old.members, ch.base) {
			cands := splice(old.cands, ch.d, func(k int) Candidate { return candidateOf(t.members[k]) })
			if updater {
				t.picker = upd.Update(old.picker, cands, ch.d)
			} else {
				t.picker = s.balancer.Build(cands)
			}
			built[i] = builtTier[T]{picker: t.picker, cands: cands, members: t.members, priority: t.priority, band: t.band}
			continue
		}
		cands := make([]Candidate, len(t.members))
		for k, b := range t.members {
			cands[k] = candidateOf(b)
		}
		ramping := s.slowStart > 0 && p.applySlowStart(t.members, cands, s.slowStart)
		switch {
		case ok && slices.Equal(old.cands, cands):
			t.picker = old.picker
			t.ranking = old.ranking
			cands = old.cands
		case ok && updater:
			t.picker = upd.Update(old.picker, cands, nil)
		default:
			t.picker = s.balancer.Build(cands)
		}
		built[i] = builtTier[T]{picker: t.picker, ranking: t.ranking, cands: cands, members: t.members, priority: t.priority, band: t.band, ramping: ramping}
	}
	for i := range v.tiers {
		t := &v.tiers[i]
		if r := built[i].ranking; r != nil {
			t.ranking = r
			continue
		}
		switch rk := t.picker.(type) {
		case *rendezvousPicker, *ringPicker:
		case Ranker:
			t.ranking = &tierRanking{ranker: rk, order: make([]int, len(t.members))}
			built[i].ranking = t.ranking
		}
	}
	p.built, p.builtGen = built, s.gen
}

func candidateOf[T any](b *Backend[T]) Candidate {
	c := b.core
	return Candidate{InFlight: &c.inflight, Latency: &c.latency, Meta: b, ID: b.id, Weight: b.weight}
}

// applySlowStart lowers the candidate weights of backends that are ramping
// up and arms the timer for the next step. It reports whether any is.
func (p *Pool[T]) applySlowStart(members []*Backend[T], cands []Candidate, ramp time.Duration) bool {
	now := p.clock.Now()
	ramping := false
	maxWeight := 0
	fraction := make([]float64, len(members))
	for i, b := range members {
		c := b.core
		maxWeight = max(maxWeight, b.weight)
		fraction[i] = 1
		rs := c.rampStart.Load()
		if rs == &p.pendingRamp && c.rampStart.CompareAndSwap(rs, &now) {
			rs = &now
		}
		if rs == nil || rs == &p.pendingRamp {
			continue
		}
		// A rebuild may have started the ramp with a later reading of the
		// clock than ours: count that as the start.
		elapsed := max(now.Sub(*rs), 0)
		if elapsed >= ramp {
			c.rampStart.CompareAndSwap(rs, nil)
			continue
		}
		fraction[i] = float64(elapsed) / float64(ramp)
		ramping = true
	}
	if !ramping {
		return false
	}
	scale := 1
	if maxWeight <= MaxWeight/rampScale {
		scale = rampScale
	}
	for i := range cands {
		cands[i].Weight = max(int(float64(cands[i].Weight*scale)*fraction[i]), 1)
	}
	p.armRamp(max(ramp/rampScale, time.Millisecond))
	return true
}

// armRamp schedules one refresh after d unless one is already pending. The
// clock is called without a lock; Close stops the timer.
func (p *Pool[T]) armRamp(d time.Duration) {
	if p.isClosed.Load() || !p.rampArmed.CompareAndSwap(false, true) {
		return
	}
	timer := p.clock.AfterFunc(d, func() {
		p.rampArmed.Store(false)
		p.refresh()
	})
	p.rampMu.Lock()
	if p.isClosed.Load() {
		p.rampMu.Unlock()
		timer.Stop()
		return
	}
	p.rampTimer = timer
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
// tier. Within a tier it takes the cost bands in order and, within a
// band, the balancer's choices first, then every member in turn, for when
// the balancer kept choosing backends that could not take the request,
// such as half-open ones out of trial slots, full ones or ones already
// tried. A tier that refuses because members are full ends the pick with
// ErrSaturated unless v.overflow lets it go on to the next tier; the pick
// also fails with ErrSaturated if it found members full on its way, and
// with ErrUnavailable otherwise.
func (v *view[T]) pick(hash uint64, tried *Request[T]) (Lease[T], error) {
	if len(v.tiers) == 0 {
		return Lease[T]{}, v.err
	}
	full := false
	l, r := pickTiers(v.tiers, hash, tried, v.ignoreHealth, v.overflow, false)
	switch r {
	case admitted:
		return l, nil
	case saturated:
		if !v.overflow {
			return Lease[T]{}, ErrSaturated
		}
		full = true
	case refused:
	}
	if l, r = pickTiers(v.fallback, hash, tried, true, v.overflow, true); r == admitted {
		return l, nil
	}
	if full || r == saturated {
		return Lease[T]{}, ErrSaturated
	}
	return Lease[T]{}, ErrUnavailable
}

// pickTiers leases a member of the first of ts that has one that can
// take the pick, with scan for tiers without a Picker. Without overflow,
// it stops at the end of a priority in which a band was full and returns
// saturated; the bands of one priority always pass a pick on.
func pickTiers[T any](ts []tier[T], hash uint64, tried *Request[T], ignoreHealth, overflow, scanOnly bool) (Lease[T], refusal) {
	res := refused
	for i := range ts {
		t := &ts[i]
		if res == saturated && !overflow && t.priority != ts[i-1].priority {
			return Lease[T]{}, saturated
		}
		var l Lease[T]
		var r refusal
		if scanOnly {
			l, r = scan(t.members, hash, tried, ignoreHealth)
		} else {
			l, r = t.pick(hash, tried, ignoreHealth)
		}
		switch r {
		case admitted:
			return l, admitted
		case saturated:
			res = saturated
		case refused:
		}
	}
	return Lease[T]{}, res
}

// pick leases a member of t not in tried. It returns saturated if no
// member took the pick and one of them refused because it was full.
func (t *tier[T]) pick(hash uint64, tried *Request[T], ignoreHealth bool) (Lease[T], refusal) {
	n := len(t.members)
	if l, ok := t.try(t.picker.Pick(hash, 0), tried, ignoreHealth); ok {
		return l, admitted
	}
	switch p := t.picker.(type) {
	case *ringPicker:
		var lease Lease[T]
		p.visit(p.position(hash), func(i int) bool {
			var ok bool
			lease, ok = t.try(i, tried, ignoreHealth)
			return ok
		})
		if lease.b != nil {
			return lease, admitted
		}
	case *rendezvousPicker:
		// Rank the remaining candidates once. If the winner's admission
		// changes during scoring, the final scan still makes progress.
		best := p.best(hash, func(i int) bool {
			b := t.members[i]
			return (tried == nil || !tried.has(b.id)) && !b.full() && b.core.canRoute(ignoreHealth)
		})
		if best >= 0 {
			if l, ok := t.try(best, tried, ignoreHealth); ok {
				return l, admitted
			}
		}
	default:
		if rk := t.ranking; rk != nil && rk.busy.CompareAndSwap(false, true) {
			if l, ok := t.ranked(rk, hash, tried, ignoreHealth); ok {
				return l, admitted
			}
		} else {
			for attempt := 1; attempt < n; attempt++ {
				if l, ok := t.try(t.picker.Pick(hash, attempt), tried, ignoreHealth); ok {
					return l, admitted
				}
			}
		}
	}
	return scan(t.members, hash, tried, ignoreHealth)
}

// ranked holds the tier's scratch space until all indices have been consumed.
func (t *tier[T]) ranked(rk *tierRanking, hash uint64, tried *Request[T], ignoreHealth bool) (Lease[T], bool) {
	defer rk.busy.Store(false)
	rk.ranker.Rank(hash, rk.order)
	for _, i := range rk.order[1:] {
		if l, ok := t.try(i, tried, ignoreHealth); ok {
			return l, true
		}
	}
	return Lease[T]{}, false
}

// try leases member i of t unless tried has it or it refuses.
func (t *tier[T]) try(i int, tried *Request[T], ignoreHealth bool) (Lease[T], bool) {
	b := t.members[i]
	if tried != nil && tried.has(b.id) {
		return Lease[T]{}, false
	}
	tk, r := b.acquire(ignoreHealth)
	if r != admitted {
		return Lease[T]{}, false
	}
	return Lease[T]{b: b, t: tk}, true
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
	start := index(hash % u64(n))
	for k := range n {
		i := start + k
		if i >= n {
			i -= n
		}
		b := bs[i]
		if tried != nil && tried.has(b.id) {
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

// Backend returns the member with the given ID. It does not lock.
func (p *Pool[T]) Backend(id string) (*Backend[T], bool) {
	b := p.byID.load(id)
	return b, b != nil
}

// Acquire leases the member with the given ID, bypassing the balancer, for
// requests that must reach one backend, such as the continuation of a
// session. The member passes the checks of a pick: one that is draining,
// offline, down, ejected or out of half-open trial slots refuses with
// ErrUnavailable, and one at its Target.MaxInFlight with ErrSaturated. In
// panic mode it ignores health as Pick does. Acquire returns
// ErrUnknownBackend if there is no such member and ErrClosed once the pool
// is closed. Like Pick, it does not lock or allocate.
func (p *Pool[T]) Acquire(id string) (Lease[T], error) {
	v := p.view.Load()
	if errors.Is(v.err, ErrClosed) {
		return Lease[T]{}, ErrClosed
	}
	b := p.byID.load(id)
	if b == nil {
		return Lease[T]{}, ErrUnknownBackend
	}
	switch t, r := b.acquire(v.ignoreHealth); r {
	case admitted:
		return Lease[T]{b: b, t: t}, nil
	case saturated:
		return Lease[T]{}, ErrSaturated
	case refused:
	}
	return Lease[T]{}, ErrUnavailable
}

// index maps member IDs to backends. Writes happen under the pool's mutex;
// reads, by Backend and Acquire, take no lock.
type idIndex[T any] struct{ m sync.Map }

func (x *idIndex[T]) load(id string) *Backend[T] {
	b, _ := x.m.Load(id)
	bk, _ := b.(*Backend[T])
	return bk
}

func (x *idIndex[T]) store(b *Backend[T]) { x.m.Store(b.id, b) }

func (x *idIndex[T]) delete(id string) { x.m.Delete(id) }

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
func (p *Pool[T]) Clock() timeshim.Clock { return p.clock }

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
	members := slices.Clone(p.order)
	p.mu.Unlock()
	for _, b := range members {
		b.core.det.Close()
	}
	p.rampMu.Lock()
	timer := p.rampTimer
	p.rampTimer = nil
	p.rampMu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}
