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
	// row. Zero disables the rule.
	ConsecutiveFailures int

	// FailurePercent ejects the backend when at least this percentage of
	// the requests in Window failed, once MinRequests requests have been
	// seen. Zero disables the rule; values above 100 mean 100.
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
}

// Enabled reports whether the configuration enables passive health
// checking.
func (c OutlierConfig) Enabled() bool {
	return c.ConsecutiveFailures > 0 || c.FailurePercent > 0
}

// outlierParams is a normalized OutlierConfig.
type outlierParams struct {
	clock    Clock
	epoch    time.Time
	window   time.Duration
	base     time.Duration
	max      time.Duration
	minReq   float64
	percent  float64
	consec   uint32
	trials   int32
	trialsOK int32
}

func newOutlierParams(c OutlierConfig, clock Clock) outlierParams {
	p := outlierParams{
		clock:    clockOrSystem(clock),
		window:   c.Window,
		base:     c.BaseEjection,
		max:      c.MaxEjection,
		minReq:   float64(c.MinRequests),
		percent:  float64(min(max(c.FailurePercent, 0), 100)),
		consec:   uint32(min(max(c.ConsecutiveFailures, 0), math.MaxInt32)),
		trials:   int32(min(c.HalfOpenRequests, 1<<30)),
		trialsOK: int32(min(c.HalfOpenSuccesses, 1<<30)),
	}
	p.epoch = p.clock.Now()
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
	reported bool // used by Lease, which embeds the ticket of its pick
}

// An OutlierDetector tracks the passive health of one backend. Callers ask
// it for admission before each request and report each request's outcome
// afterwards. It is safe for concurrent use. Admit and Record are lock-free
// apart from state transitions, which are rare.
//
// The detector owns one timer, which ends an ejection. Close stops it.
type OutlierDetector struct {
	win       window
	until     atomic.Int64 // unix nanoseconds; 0 unless ejected
	ejections atomic.Uint64
	state     atomic.Uint32 // OutlierState
	consec    atomic.Uint32
	gen       atomic.Uint32 // changes on every transition
	trials    atomic.Uint64 // half-open period<<32 | trial requests in flight
	trialsOK  atomic.Uint64 // half-open period<<32 | successful trials
	onChange  func()
	params    outlierParams

	mu     sync.Mutex
	stop   func() bool // ends the pending reopen timer; guarded by mu
	cause  error       // guarded by mu
	level  uint        // ejections without recovery; guarded by mu
	closed bool        // guarded by mu
}

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
	d.init(newOutlierParams(cfg, clock), onChange)
	return d
}

func (d *OutlierDetector) init(p outlierParams, onChange func()) {
	d.params = p
	d.onChange = onChange
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
		n := periodCount(v, gen)
		if n >= uint32(d.params.trials) {
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
	return uint32(v)
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
	p := &d.params
	switch o {
	case Success:
		if d.consec.Load() != 0 {
			d.consec.Store(0)
		}
		if p.percent > 0 {
			d.win.add(p.clock.Now().Sub(p.epoch), p.window, windowTotal)
		}
	case Failure:
		if OutlierState(d.state.Load()) != OutlierOK {
			return // judged by trials, not by requests admitted earlier
		}
		if p.consec > 0 && d.consec.Add(1) == p.consec {
			d.eject(OutlierOK, 0, ErrConsecutiveFailures)
			return
		}
		if p.percent > 0 {
			total, failed := d.win.add(p.clock.Now().Sub(p.epoch), p.window, windowTotal|windowPart)
			if total >= p.minReq && failed*100 >= p.percent*total {
				d.eject(OutlierOK, 0, ErrFailureRate)
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
			n := periodCount(v, gen) + 1
			if d.trialsOK.CompareAndSwap(v, uint64(gen)<<32|uint64(n)) {
				if n >= uint32(d.params.trialsOK) {
					d.recover(gen)
				}
				return
			}
		}
	case Failure:
		d.eject(OutlierHalfOpen, gen, ErrTrialFailed)
	case Neutral:
	}
}

// eject moves the detector from state from (and, for half-open, period
// gen) to ejected. It does nothing if the detector moved on meanwhile.
func (d *OutlierDetector) eject(from OutlierState, gen uint32, cause error) {
	if !d.params.enabled() {
		return
	}
	now := d.params.clock.Now()
	d.mu.Lock()
	if d.closed || OutlierState(d.state.Load()) != from || (from == OutlierHalfOpen && d.gen.Load() != gen) {
		d.mu.Unlock()
		return
	}
	wait := d.params.backoff(d.level)
	d.level++
	ejectGen := d.gen.Add(1)
	d.cause = cause
	d.consec.Store(0)
	d.win.reset()
	d.until.Store(now.Add(wait).UnixNano())
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
	stop := d.params.clock.AfterFunc(wait, func() { d.reopen(ejectGen) })
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
	d.until.Store(0)
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

func (d *OutlierDetector) resetLocked() {
	d.level = 0
	d.cause = nil
	d.consec.Store(0)
	d.win.reset()
	d.until.Store(0)
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
	stop := d.stop
	d.stop = nil
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// State returns the current passive health state.
func (d *OutlierDetector) State() OutlierState {
	return OutlierState(d.state.Load())
}

// Status returns a snapshot of the detector.
func (d *OutlierDetector) Status() OutlierStatus {
	d.mu.Lock()
	cause := d.cause
	d.mu.Unlock()
	s := OutlierStatus{
		Cause:               cause,
		Ejections:           d.ejections.Load(),
		ConsecutiveFailures: d.consec.Load(),
		State:               OutlierState(d.state.Load()),
	}
	if u := d.until.Load(); u != 0 {
		s.EjectedUntil = time.Unix(0, u)
	}
	return s
}

func (d *OutlierDetector) notify() {
	if d.onChange != nil {
		d.onChange()
	}
}

func (p *outlierParams) enabled() bool { return p.consec > 0 || p.percent > 0 }

// window estimates two counts over a sliding window, a total (such as
// requests) and a part of it or a second count (such as failures or
// retries), from two fixed buckets: the current one, and the previous one
// weighted by how much of it still overlaps the sliding window. Each bucket
// packs the total in the high 32 bits and the part in the low 32 bits so
// one atomic add updates both.
type window struct {
	idx  atomic.Int64
	cur  atomic.Uint64
	prev atomic.Uint64
}

// Deltas for window.add.
const (
	windowTotal uint64 = 1 << 32
	windowPart  uint64 = 1
)

// add adds delta, a sum of windowTotal and windowPart, to the bucket of
// now and returns the estimates after it.
func (w *window) add(now, size time.Duration, delta uint64) (total, part float64) {
	w.rotate(now, size)
	c := w.cur.Add(delta)
	prev := w.prev.Load()
	weight := 1 - float64(now%size)/float64(size)
	total = float64(c>>32) + float64(prev>>32)*weight
	part = float64(uint32(c)) + float64(uint32(prev))*weight
	return total, part
}

// rotate moves the buckets on to the one of now.
func (w *window) rotate(now, size time.Duration) {
	i := int64(now / size)
	if old := w.idx.Load(); i > old && w.idx.CompareAndSwap(old, i) {
		c := w.cur.Swap(0)
		if i == old+1 {
			w.prev.Store(c)
		} else {
			w.prev.Store(0)
		}
	}
}

// addPartWithin adds one to the part if the estimate of the part then
// stays within base plus ratio times the total, and reports whether it
// did. The check and the add are one atomic step.
func (w *window) addPartWithin(now, size time.Duration, base, ratio float64) bool {
	w.rotate(now, size)
	weight := 1 - float64(now%size)/float64(size)
	for {
		c := w.cur.Load()
		prev := w.prev.Load()
		total := float64(c>>32) + float64(prev>>32)*weight
		part := float64(uint32(c)) + float64(uint32(prev))*weight
		if part+1 > base+ratio*total {
			return false
		}
		if w.cur.CompareAndSwap(c, c+windowPart) {
			return true
		}
	}
}

// removePart takes back one from the part of the current bucket, if it
// has any.
func (w *window) removePart() {
	for {
		c := w.cur.Load()
		if uint32(c) == 0 || w.cur.CompareAndSwap(c, c-windowPart) {
			return
		}
	}
}

func (w *window) reset() {
	w.cur.Store(0)
	w.prev.Store(0)
}
