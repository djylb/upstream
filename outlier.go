package upstream

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Causes recorded when an OutlierDetector ejects its backend.
var (
	// ErrConsecutiveFailures means the backend failed
	// OutlierConfig.ConsecutiveFailures requests in a row.
	ErrConsecutiveFailures = errors.New("upstream: too many consecutive failures")

	// ErrFailureRate means the share of failed requests in the window
	// reached OutlierConfig.FailurePercent.
	ErrFailureRate = errors.New("upstream: failure rate too high")

	// ErrTrialFailed means a half-open trial request failed.
	ErrTrialFailed = errors.New("upstream: half-open trial failed")
)

// OutlierConfig configures passive health checking, which judges a backend
// by the outcomes of real traffic. The zero value disables it; set
// ConsecutiveFailures, FailurePercent or both to enable it. Zero durations
// and counts take the defaults documented on each field.
//
// An ejected backend receives no traffic for an ejection time that starts
// at BaseEjection and doubles with every ejection that is not followed by a
// recovery, up to MaxEjection. When the time is up the backend becomes
// half-open: it admits at most HalfOpenRequests concurrent trial requests.
// HalfOpenSuccesses successful trials restore it and reset the ejection
// time; one failed trial ejects it again.
type OutlierConfig struct {
	// Window is the span over which FailurePercent is measured. The
	// default is 10s. The rate is a sliding estimate over the current and
	// the previous window.
	Window time.Duration

	// BaseEjection is the first ejection time. The default is 30s.
	BaseEjection time.Duration

	// MaxEjection caps the ejection time. The default is 5m; a value below
	// BaseEjection is raised to it.
	MaxEjection time.Duration

	// ConsecutiveFailures ejects the backend after this many failures in a
	// row. Degraded outcomes neither count towards the run nor end it.
	// Zero disables the rule.
	ConsecutiveFailures int

	// FailurePercent ejects the backend when at least this percentage of
	// the requests in Window failed or were Degraded, once MinRequests
	// requests have been seen. Zero disables the rule; values above 100 mean 100.
	FailurePercent int

	// MinRequests is the request volume the failure-rate rule needs before
	// it judges the backend. The default is 20.
	MinRequests int

	// HalfOpenRequests is the number of trial requests a half-open backend
	// admits at the same time. The default is 1.
	HalfOpenRequests int

	// HalfOpenSuccesses is the number of successful trials that restore a
	// half-open backend. The default is 1.
	HalfOpenSuccesses int

	// MaxEjectionPercent, from 1 to 100, caps the share of a pool's members
	// that passive health checking may hold ejected or half-open at once,
	// so that a fault shared by every backend, such as a database they all
	// depend on, cannot eject the whole pool. One member may always be
	// ejected. A failure that would eject a member beyond the cap leaves it
	// in rotation and is counted in OutlierStatus.Refused. Zero means no
	// cap. It applies to the detectors of a Pool, not to an
	// OutlierDetector on its own.
	MaxEjectionPercent int
}

// Enabled reports whether the configuration enables passive health
// checking.
func (c OutlierConfig) Enabled() bool {
	return c.ConsecutiveFailures > 0 || c.FailurePercent > 0
}

// outlierParams is a normalized OutlierConfig. It is immutable once
// published; a change publishes a new one.
type outlierParams struct {
	counts     *window        // private to one detector once installed
	streak     *atomic.Uint32 // consecutive failures in this reset generation
	clock      Clock
	epoch      time.Time
	window     time.Duration
	base       time.Duration
	max        time.Duration
	minReq     float64
	percent    float64
	consec     uint32
	trials     int32
	trialsOK   int32
	maxEjected int32  // percent; 0 means no cap
	seq        uint64 // orders the parameters published to one detector
}

// newOutlierParams normalizes c. epoch is the origin of the failure-rate
// window. The caller sets seq.
func newOutlierParams(c OutlierConfig, clock Clock, epoch time.Time) *outlierParams {
	p := &outlierParams{
		clock:    clockOrSystem(clock),
		window:   c.Window,
		base:     c.BaseEjection,
		max:      c.MaxEjection,
		minReq:   float64(c.MinRequests),
		percent:  float64(min(max(c.FailurePercent, 0), 100)),
		consec:   uint32(min(max(c.ConsecutiveFailures, 0), math.MaxInt32)),
		trials:   i32(min(c.HalfOpenRequests, 1<<30)),
		trialsOK: i32(min(c.HalfOpenSuccesses, 1<<30)),
		epoch:    epoch,
	}
	if c.MaxEjectionPercent > 0 && c.MaxEjectionPercent < 100 {
		p.maxEjected = int32(c.MaxEjectionPercent)
	}

	if p.window <= 0 {
		p.window = 10 * time.Second
	}
	if p.base <= 0 {
		p.base = 30 * time.Second
	}
	if p.max <= 0 {
		p.max = 5 * time.Minute
	}
	p.max = max(p.max, p.base)
	if c.MinRequests <= 0 {
		p.minReq = 20
	}
	if p.trials <= 0 {
		p.trials = 1
	}
	if p.trialsOK <= 0 {
		p.trialsOK = 1
	}
	return p
}

// backoff returns the ejection time for the given number of ejections
// without recovery.
func (p *outlierParams) backoff(level uint) time.Duration {
	if level >= 62 || p.base > p.max>>level {
		return p.max
	}
	return p.base << level
}

// OutlierState is the passive health state of a backend.
type OutlierState uint32

const (
	// OutlierOK means the backend is not ejected.
	OutlierOK OutlierState = iota

	// OutlierEjected means the backend is ejected and admits no traffic
	// until its ejection time is up.
	OutlierEjected

	// OutlierHalfOpen means the ejection time is up and the backend admits
	// a limited number of trial requests.
	OutlierHalfOpen
)

// String returns the name of the state.
//
//goland:noinspection GoMixedReceiverTypes
func (s OutlierState) String() string {
	switch s {
	case OutlierOK:
		return "ok"
	case OutlierEjected:
		return "ejected"
	case OutlierHalfOpen:
		return "half-open"
	}
	return "OutlierState(" + strconv.Itoa(int(s)) + ")"
}

// MarshalText encodes the state as its name.
//
//goland:noinspection GoMixedReceiverTypes
func (s OutlierState) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// OutlierStatus is a snapshot of an OutlierDetector.
type OutlierStatus struct {
	// EjectedUntil is when the current ejection ends; it is zero unless
	// State is OutlierEjected.
	EjectedUntil time.Time

	// Cause is why the backend was last ejected; it is nil while State is
	// OutlierOK.
	Cause error

	// Ejections counts every ejection since the detector was created.
	Ejections uint64

	// Refused counts the failures that would have ejected the backend but
	// that OutlierConfig.MaxEjectionPercent refused.
	Refused uint64

	// ConsecutiveFailures is the current run of failures.
	ConsecutiveFailures uint32

	// State is the passive health state.
	State OutlierState
}

// A Ticket is the admission returned by OutlierDetector.Admit. Pass it back
// to Record exactly once when the request ends.
type Ticket struct {
	gen      uint32
	trial    bool
	admitted bool // used by Pool: the detector is to record the outcome
	reported bool // used by Lease, which embeds the ticket of its pick
}

// An OutlierDetector tracks the passive health of one backend. Callers ask
// it for admission before each request and report each request's outcome
// afterwards. It is safe for concurrent use. Admit and Record are lock-free
// apart from state transitions, which are rare.
//
// The detector owns one timer, which ends an ejection. Close stops it.
type OutlierDetector struct {
	ejections atomic.Uint64
	state     atomic.Uint32 // OutlierState
	gen       atomic.Uint32 // changes on every transition
	trials    atomic.Uint64 // half-open period<<32 | trial requests in flight
	trialsOK  atomic.Uint64 // half-open period<<32 | successful trials
	refused   atomic.Uint64
	params    atomic.Pointer[outlierParams]
	onChange  func()
	guard     *ejectionGuard // the pool's cap on ejections, or nil

	mu     sync.Mutex
	until  time.Time   // guarded by mu
	stop   func() bool // ends the pending reopen timer; guarded by mu
	cause  error       // guarded by mu
	level  uint        // ejections without recovery; guarded by mu
	held   bool        // holds one of guard's ejections; guarded by mu
	closed bool        // guarded by mu

	enabled atomic.Bool // params.enabled(); set with params under mu
}

// ejectionGuard caps the share of a pool's members that are ejected or
// half-open. Each detector holds at most one of its slots, from its
// ejection until it recovers, is reset or is closed.
type ejectionGuard struct {
	held    atomic.Int32
	members atomic.Int32
}

// acquire takes a slot unless that would put more than percent of the
// members out; one member may always be out.
func (g *ejectionGuard) acquire(percent int32) bool {
	for {
		n := g.held.Load()
		if percent > 0 && n > 0 && int64(n+1)*100 > int64(percent)*int64(g.members.Load()) {
			return false
		}
		if g.held.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (g *ejectionGuard) release() { g.held.Add(-1) }

// NewOutlierDetector returns a detector for one backend that reads time
// from clock; nil means SystemClock. onChange, if not nil, is called after every state transition, without any lock held, from
// the goroutine that caused the transition: one calling Record, Reset or
// Close, or the detector's timer goroutine. Calls may overlap, so onChange
// should read State instead of assuming an order.
//
// A detector whose configuration is not Enabled admits everything and never
// ejects.
func NewOutlierDetector(cfg OutlierConfig, clock Clock, onChange func()) *OutlierDetector {
	d := new(OutlierDetector)
	clock = clockOrSystem(clock)
	d.init(newOutlierParams(cfg, clock, clock.Now()), onChange, nil)
	return d
}

func (d *OutlierDetector) init(p *outlierParams, onChange func(), g *ejectionGuard) {
	params := *p
	params.streak = new(atomic.Uint32)
	if params.percent > 0 {
		params.counts = new(window)
	}
	p = &params
	d.params.Store(p)
	d.enabled.Store(p.enabled())
	d.onChange = onChange
	d.guard = g
}

// SetConfig replaces the detector's configuration. The current state and
// any ejection in progress stay; the new thresholds and times apply from
// the next request and the next ejection. A configuration that is not
// Enabled also clears any ejection, as Reset does.
func (d *OutlierDetector) SetConfig(cfg OutlierConfig) {
	old := d.params.Load()
	d.setParams(newOutlierParams(cfg, old.clock, old.epoch))
}

// setParams also clears a disabled detector in the same critical section.
func (d *OutlierDetector) setParams(p *outlierParams) {
	d.mu.Lock()
	old := d.params.Load()
	if p.seq != 0 && old.seq >= p.seq {
		d.mu.Unlock()
		return
	}
	params := *p
	params.streak = old.streak
	params.counts = old.counts
	switch {
	case p.percent == 0:
		params.counts = nil
	case old.counts == nil || old.window != p.window:
		params.counts = new(window)
	}
	p = &params
	d.params.Store(p)
	d.enabled.Store(p.enabled())
	var stop func() bool
	changed := false
	if !p.enabled() && !d.closed {
		changed = d.State() != OutlierOK
		d.gen.Add(1)
		d.resetLocked()
		stop, d.stop = d.stop, nil
	}
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
	if changed {
		d.notify()
	}
}

// Admit reports whether the backend may take a new request now. A healthy
// backend always admits; an ejected one never does; a half-open one admits
// while fewer than HalfOpenRequests trials are in flight. When Admit
// returns true the caller must pass the Ticket to Record exactly once.
func (d *OutlierDetector) Admit() (Ticket, bool) {
	switch OutlierState(d.state.Load()) {
	case OutlierOK:
		return Ticket{}, true
	case OutlierEjected:
		return Ticket{}, false
	case OutlierHalfOpen:
		return d.admitTrial()
	}
	return Ticket{}, false
}

func (d *OutlierDetector) trialsFull() bool {
	return d.State() == OutlierHalfOpen && periodCount(d.trials.Load(), d.gen.Load()) >= u32(d.params.Load().trials)
}

// admitTrial takes a trial slot of the current half-open period. Every
// transition changes gen, so a period that ends while the slot is taken is
// detected by reading gen again, and the slot is given back.
func (d *OutlierDetector) admitTrial() (Ticket, bool) {
	gen := d.gen.Load()
	if OutlierState(d.state.Load()) != OutlierHalfOpen {
		return Ticket{}, false
	}
	for {
		v := d.trials.Load()
		if d.gen.Load() != gen {
			return Ticket{}, false
		}
		n := periodCount(v, gen)
		if n >= u32(d.params.Load().trials) {
			return Ticket{}, false
		}
		if d.trials.CompareAndSwap(v, uint64(gen)<<32|uint64(n+1)) {
			break
		}
	}
	if d.gen.Load() != gen {
		d.releaseTrial(gen)
		return Ticket{}, false
	}
	return Ticket{gen: gen, trial: true}, true
}

// periodCount returns the count packed in v if v belongs to half-open
// period gen, and zero otherwise.
func periodCount(v uint64, gen uint32) uint32 {
	if uint32(v>>32) != gen {
		return 0
	}
	return low32(v)
}

// releaseTrial returns the trial slot of a ticket from period gen. A
// ticket from an earlier period holds no slot of the current one.
func (d *OutlierDetector) releaseTrial(gen uint32) {
	for {
		v := d.trials.Load()
		n := periodCount(v, gen)
		if n == 0 || d.trials.CompareAndSwap(v, v-1) {
			return
		}
	}
}

// Record reports the outcome of a request admitted with t.
func (d *OutlierDetector) Record(t Ticket, o Outcome) {
	if t.trial {
		// The slot goes back before any transition, so a panicking
		// onChange cannot leak it.
		d.releaseTrial(t.gen)
		d.recordTrial(t.gen, o)
		return
	}
	p := d.params.Load()
	switch o {
	case Success:
		if p.streak.Load() != 0 {
			p.streak.Store(0)
		}
		if p.percent > 0 {
			p.counts.add(p.clock.Now().Sub(p.epoch), p.window, 0)
		}
	case Failure, Degraded:
		if OutlierState(d.state.Load()) != OutlierOK {
			return // judged by trials, not by requests admitted earlier
		}
		// At or beyond the threshold, as a refused ejection leaves the
		// run going.
		if o == Failure && p.consec > 0 {
			if n := p.streak.Add(1); n >= p.consec {
				d.eject(p, 0, ErrConsecutiveFailures, func(current *outlierParams) bool {
					return current.consec > 0 && n >= current.consec
				})
				return
			}
		}
		if p.percent > 0 {
			passed, failed := p.counts.add(p.clock.Now().Sub(p.epoch), p.window, 1)
			total := passed + failed
			if total >= p.minReq && failed*100 >= p.percent*total {
				d.eject(p, 0, ErrFailureRate, func(current *outlierParams) bool {
					return current.counts == p.counts && total >= current.minReq && failed*100 >= current.percent*total
				})
			}
		}
	case Neutral:
	}
}

func (d *OutlierDetector) recordTrial(gen uint32, o Outcome) {
	switch o {
	case Success:
		if d.gen.Load() != gen || OutlierState(d.state.Load()) != OutlierHalfOpen {
			return // a trial from an earlier period
		}
		for {
			v := d.trialsOK.Load()
			if d.gen.Load() != gen {
				return
			}
			n := periodCount(v, gen) + 1
			if d.trialsOK.CompareAndSwap(v, uint64(gen)<<32|uint64(n)) {
				if n >= u32(d.params.Load().trialsOK) {
					d.recover(gen)
				}
				return
			}
		}
	case Failure, Degraded:
		d.eject(d.params.Load(), gen, ErrTrialFailed, nil)
	case Neutral:
	}
}

// eject revalidates the observation against the live rule before committing.
// A nil rule identifies a failed trial from the given half-open period.
func (d *OutlierDetector) eject(p *outlierParams, gen uint32, cause error, rule func(*outlierParams) bool) {
	if !p.enabled() {
		return
	}
	now := p.clock.Now()
	d.mu.Lock()
	current := d.params.Load()
	valid := d.State() == OutlierHalfOpen && d.gen.Load() == gen
	if rule != nil {
		valid = d.State() == OutlierOK && p.streak == current.streak && rule(current)
	}
	if d.closed || !current.enabled() || !valid {
		d.mu.Unlock()
		return
	}
	p = current
	if !d.held && d.guard != nil {
		if !d.guard.acquire(p.maxEjected) {
			d.mu.Unlock()
			d.refused.Add(1)
			return
		}
		d.held = true
	}
	wait := p.backoff(d.level)
	d.level++
	ejectGen := d.gen.Add(1)
	d.cause = cause
	d.resetCountsLocked()
	d.until = now.Add(wait)
	d.ejections.Add(1)
	d.state.Store(uint32(OutlierEjected))
	old := d.stop
	d.stop = nil
	d.mu.Unlock()
	if old != nil {
		old()
	}

	// The clock is called without the lock; the generation check below
	// handles a timer that fires, or a Close that runs, in between.
	stop := p.clock.AfterFunc(wait, func() { d.reopen(ejectGen) })
	d.mu.Lock()
	if d.closed || d.gen.Load() != ejectGen {
		d.mu.Unlock()
		stop()
	} else {
		d.stop = stop
		d.mu.Unlock()
	}
	d.notify()
}

func (d *OutlierDetector) reopen(gen uint32) {
	d.mu.Lock()
	if d.closed || d.gen.Load() != gen || OutlierState(d.state.Load()) != OutlierEjected {
		d.mu.Unlock()
		return
	}
	d.gen.Add(1)
	d.stop = nil
	d.until = time.Time{}
	d.state.Store(uint32(OutlierHalfOpen))
	d.mu.Unlock()
	d.notify()
}

func (d *OutlierDetector) recover(gen uint32) {
	d.mu.Lock()
	if d.closed || d.gen.Load() != gen || OutlierState(d.state.Load()) != OutlierHalfOpen {
		d.mu.Unlock()
		return
	}
	d.gen.Add(1)
	d.resetLocked()
	d.mu.Unlock()
	d.notify()
}

func (d *OutlierDetector) resetCountsLocked() {
	old := d.params.Load()
	if old == nil {
		return
	}
	p := *old
	p.streak = new(atomic.Uint32)
	if old.counts != nil {
		p.counts = new(window)
	}
	d.params.Store(&p)
}

func (d *OutlierDetector) resetLocked() {
	d.releaseGuardLocked()
	d.level = 0
	d.cause = nil
	d.resetCountsLocked()
	d.until = time.Time{}
	d.state.Store(uint32(OutlierOK))
}

// Reset clears any ejection and the ejection backoff, as if the backend had
// just recovered. It is meant for operators overriding the detector.
func (d *OutlierDetector) Reset() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	changed := OutlierState(d.state.Load()) != OutlierOK
	d.gen.Add(1)
	d.resetLocked()
	stop := d.stop
	d.stop = nil
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
	if changed {
		d.notify()
	}
}

// Close stops the detector's timer. The detector keeps its current state
// and still answers Admit and Record, but it no longer changes state.
// Close is idempotent.
func (d *OutlierDetector) Close() {
	d.mu.Lock()
	d.closed = true
	d.releaseGuardLocked()
	stop := d.stop
	d.stop = nil
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (d *OutlierDetector) releaseGuardLocked() {
	if d.held {
		d.held = false
		d.guard.release()
	}
}

// State returns the current passive health state.
func (d *OutlierDetector) State() OutlierState {
	return OutlierState(d.state.Load())
}

// Status returns a snapshot of the detector.
func (d *OutlierDetector) Status() OutlierStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	var failures uint32
	if p := d.params.Load(); p != nil {
		failures = p.streak.Load()
	}
	return OutlierStatus{
		Cause:               d.cause,
		EjectedUntil:        d.until,
		Ejections:           d.ejections.Load(),
		Refused:             d.refused.Load(),
		ConsecutiveFailures: failures,
		State:               OutlierState(d.state.Load()),
	}
}

func (d *OutlierDetector) notify() {
	if d.onChange != nil {
		d.onChange()
	}
}

func (p *outlierParams) enabled() bool { return p.consec > 0 || p.percent > 0 }

// Two alternating buckets count two event streams. Retry reservations have a
// third, independent stream for refunds, which belong to the original bucket.
type window struct {
	latest  atomic.Uint64
	buckets [2]windowBucket
}

type windowBucket struct {
	counts  [2]windowCounter
	refunds windowCounter
}

// A counter's serial increases on every event and every rotation. The count
// is the distance from the serial at the start of its window. Even serials
// accept increments; an odd serial freezes the old count during rotation.
// The low bit of generation marks a rotation in progress. Any caller can
// finish it, even if the caller that started it is suspended.
type windowCounter struct {
	generation atomic.Uint64
	serial     atomic.Uint64
	base       atomic.Uint64
}

// advance publishes the new origin before opening the new generation.
// Every helper of this rotation sees the same frozen serial. A delayed
// helper can never move the origin back or thaw a later generation.
func (c *windowCounter) advance(generation, serial uint64) {
	if generation != c.generation.Load() {
		return
	}
	if generation&1 == 0 {
		// The generation is open but its first increment still needs to thaw it.
		c.serial.CompareAndSwap(serial, serial+1)
		return
	}
	if serial&1 == 0 {
		c.serial.CompareAndSwap(serial, serial+1)
		return
	}
	raiseTo(&c.base, serial/2+1)
	c.generation.CompareAndSwap(generation, generation-1)
}

// increment adds one if this generation still accepts events and the
// resulting count fits the limit. A rotation consumes a serial, so an
// increment paused just before its CAS cannot land in a reused bucket.
func (c *windowCounter) increment(tag uint64, limit float64) bool {
	for {
		gen := c.generation.Load()
		if gen&^1 > tag {
			return false
		}
		serial := c.serial.Load()
		switch {
		case gen&1 != 0 || serial&1 != 0:
			c.advance(gen, serial)
		case gen != tag:
			c.generation.CompareAndSwap(gen, tag|1)
		default:
			base := c.base.Load()
			if gen != c.generation.Load() {
				continue
			}
			if float64(serial/2-base)+1 > limit {
				return false
			}
			if c.serial.CompareAndSwap(serial, serial+2) {
				return true
			}
		}
	}
}

func (c *windowCounter) count(tag uint64) float64 {
	if c.generation.Load() != tag {
		return 0
	}
	serial := c.serial.Load()
	base := c.base.Load()
	if c.generation.Load() != tag || serial&1 != 0 {
		return 0
	}
	return float64(serial/2 - base)
}

// raiseTo returns the larger of the stored value and next, advancing the
// atomic to next if needed.
func raiseTo(value *atomic.Uint64, next uint64) uint64 {
	for {
		old := value.Load()
		if old >= next {
			return old
		}
		if value.CompareAndSwap(old, next) {
			return next
		}
	}
}

// Count a delayed caller at the latest observed time, both when choosing
// its bucket and when weighting the preceding one.
func (w *window) at(now time.Duration, span uint64) (uint64, float64) {
	elapsed := raiseTo(&w.latest, uint64(max(now, 0)))
	return elapsed / span, 1 - float64(elapsed%span)/float64(span)
}

func (w *window) estimate(idx uint64, weight float64, stream int) float64 {
	current := w.buckets[idx&1].counts[stream].count(idx << 1)
	if idx == 0 {
		return current
	}
	return current + weight*w.buckets[(idx-1)&1].counts[stream].count((idx-1)<<1)
}

func (w *window) add(now, size time.Duration, stream int) (float64, float64) {
	span := uint64(max(size, 1))
	for {
		idx, weight := w.at(now, span)
		if w.buckets[idx&1].counts[stream].increment(idx<<1, math.Inf(1)) {
			return w.estimate(idx, weight, 0), w.estimate(idx, weight, 1)
		}
	}
}

type reservation struct {
	credits *windowCounter
	tag     uint64
}

func (r reservation) refund() { r.credits.increment(r.tag, math.Inf(1)) }

func (w *window) reserve(now, size time.Duration, base, ratio float64) reservation {
	span := uint64(max(size, 1))
	for {
		idx, weight := w.at(now, span)
		b := &w.buckets[idx&1]
		tag := idx << 1
		limit := base + ratio*w.estimate(idx, weight, 0)
		if idx > 0 {
			prev := &w.buckets[(idx-1)&1]
			prevTag := (idx - 1) << 1
			// Sample credits first: a concurrent issue and refund may only
			// make this estimate more conservative.
			credits := prev.refunds.count(prevTag)
			limit -= weight * (prev.counts[1].count(prevTag) - credits)
		}
		ok := b.counts[1].increment(tag, limit+b.refunds.count(tag))
		r := reservation{credits: &b.refunds, tag: tag}
		if w.latest.Load()/span == idx {
			if ok {
				return r
			}
			return reservation{}
		}
		if ok {
			r.refund()
		}
	}
}
