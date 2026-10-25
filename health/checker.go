package health

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/djylb/upstream"
)

// A Checker checks targets held under keys of type K, which may belong to
// groups of type G.
//
// Ownership: a key is held by a direct Put and by every group it was added
// to. It is checked once however many owners it has, and it leaves the
// Checker when Remove is called or when its last owner lets go of it. Two
// owners that give different Targets for one key share one target: the
// latest Put, AddToGroup or SyncGroup wins. Removing a key cancels its check in
// progress and discards that check's result, stops its timer, releases its
// concurrency slot and forgets its state; adding the key again starts a
// new generation.
//
// Changing a target's options, or the Config, takes effect at once: the
// next check is rescheduled from the end of the previous one. Changing a
// target's network or address starts a new generation as if the target had
// been removed and added, keeping its owners.
//
// Every check runs in its own goroutine, which exists only while the check
// runs, so a slow or hanging target never delays the others;
// Config.Concurrency bounds how many run at once. Callbacks run on one
// dispatching goroutine. The Checker owns all of these goroutines and its
// timers: Close, or the end of the context passed to New, stops them, and
// Done reports when the last has ended.
//
// All methods are safe for concurrent use and from probes and callbacks,
// and none of them waits for a probe or callback. No lock is held while a
// probe, the Clock or a callback runs.
type Checker[K, G comparable] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	stopAfter func() bool
	clock     upstream.Clock
	sem       chan struct{}
	notify    chan struct{} // wakes the dispatcher
	window    int

	done    chan struct{} // closed when closed and nothing is pending or running
	mu      sync.Mutex
	cfg     Config[K]            // guarded by mu; Options not defaulted
	tcp     Probe                // the default check for cfg; guarded by mu
	sinks   []*sink[K]           // reporters of Attach; copy on write, guarded by mu
	entries map[K]*entry[K, G]   // guarded by mu
	groups  map[G]map[K]struct{} // guarded by mu
	queue   []*delivery[K, G]    // callbacks to run, in order; guarded by mu
	pending int                  // timers and goroutines about to start; guarded by mu
	running int                  // goroutines running; guarded by mu
	nextGen uint64               // guarded by mu
	closed  bool                 // guarded by mu
	ended   bool                 // done is closed; guarded by mu
}

// entry is the state of one generation of a key. All fields are guarded
// by the Checker's mutex.
type entry[K, G comparable] struct {
	last       Result
	lastEnd    time.Time
	target     Target
	lat        latencyWindow
	stop       func() bool        // pending timer
	cancel     context.CancelFunc // check in progress
	groups     map[G]struct{}
	queued     *delivery[K, G] // a queued report that changes nothing, which a newer one replaces
	key        K
	generation uint64
	timer      uint64 // identifies the pending timer
	checks     uint64
	failures   uint64
	ok, fail   int
	health     upstream.Health
	direct     bool
	running    bool // a check is in progress
	now        bool // CheckNow asked for the next check at once
	gone       bool
}

// A sink receives every report after the Config's callbacks, on the
// dispatching goroutine. Attach registers one per attached pool.
type sink[K comparable] struct {
	fn func(Report[K])
}

// A delivery is a queued callback: a report, or the removal of e.
type delivery[K, G comparable] struct {
	rep     Report[K]
	e       *entry[K, G]
	removed bool
	replay  bool // the entry's current state, for the sinks only
}

// New returns a Checker that runs until ctx is done or Close is called.
func New[K, G comparable](ctx context.Context, cfg Config[K]) *Checker[K, G] {
	c := &Checker[K, G]{
		clock:   cfg.Clock,
		window:  cfg.LatencyWindow,
		notify:  make(chan struct{}, 1),
		entries: make(map[K]*entry[K, G]),
		groups:  make(map[G]map[K]struct{}),
		done:    make(chan struct{}),
	}
	if c.clock == nil {
		c.clock = upstream.SystemClock()
	}
	if c.window <= 0 {
		c.window = 16
	}
	n := cfg.Concurrency
	if n <= 0 {
		n = 16
	}
	c.sem = make(chan struct{}, n)
	c.setConfigLocked(cfg)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.pending = 2 // the dispatcher and the shutdown hook
	c.stopAfter = context.AfterFunc(c.ctx, func() {
		c.enter()
		defer c.leave()
		c.shutdown()
	})
	go c.dispatch()
	return c
}

// enter turns a goroutine that a timer or New announced in pending into a
// running one.
func (c *Checker[K, G]) enter() {
	c.mu.Lock()
	c.pending--
	c.running++
	c.mu.Unlock()
}

func (c *Checker[K, G]) leave() {
	c.mu.Lock()
	c.running--
	c.endLocked()
	c.mu.Unlock()
}

// unannounce withdraws a pending timer or goroutine that will not start.
func (c *Checker[K, G]) unannounce() {
	c.mu.Lock()
	c.pending--
	c.endLocked()
	c.mu.Unlock()
}

// endLocked closes done once the Checker is closed and has nothing left.
func (c *Checker[K, G]) endLocked() {
	if c.closed && c.pending == 0 && c.running == 0 && !c.ended {
		c.ended = true
		close(c.done)
	}
}

func (c *Checker[K, G]) setConfigLocked(cfg Config[K]) {
	c.cfg = cfg
	c.tcp = TCP(cfg.TCP)
}

// SetConfig replaces the configuration, except Clock, Concurrency and
// LatencyWindow, and reschedules every target under the new settings.
func (c *Checker[K, G]) SetConfig(cfg Config[K]) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.setConfigLocked(cfg)
	var w work[K, G]
	for _, e := range c.entries {
		w.schedule = append(w.schedule, e)
	}
	c.mu.Unlock()
	c.finish(&w)
}

// work collects what an operation does after releasing the mutex.
type work[K, G comparable] struct {
	schedule []*entry[K, G]
	stops    []func() bool
	cancels  []context.CancelFunc
}

// finish runs the deferred part of an operation without the mutex.
func (c *Checker[K, G]) finish(w *work[K, G]) {
	for _, stop := range w.stops {
		if stop() {
			c.unannounce()
		}
	}
	for _, cancel := range w.cancels {
		cancel()
	}
	for _, e := range w.schedule {
		c.schedule(e)
	}
}

// enqueueLocked queues a callback for the dispatcher. A report that
// changes nothing replaces the entry's previous such report if that one
// is still queued and no transition was queued after it, which bounds the
// queue while a callback blocks.
func (c *Checker[K, G]) enqueueLocked(d *delivery[K, G]) {
	if c.closed {
		return
	}
	if !d.removed && !d.replay {
		if !d.rep.Changed() && d.e.queued != nil {
			d.e.queued.rep = d.rep
			return
		}
		d.e.queued = nil
		if !d.rep.Changed() {
			d.e.queued = d
		}
	}
	c.queue = append(c.queue, d)
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// dispatch runs callbacks until the Checker closes. Whether a callback runs
// is decided under the mutex, so a target removed before that point gets
// none, and no callback starts once Close has begun.
func (c *Checker[K, G]) dispatch() {
	c.enter()
	defer c.leave()
	for {
		c.mu.Lock()
		for len(c.queue) == 0 && !c.closed {
			c.mu.Unlock()
			<-c.notify
			c.mu.Lock()
		}
		if c.closed {
			c.queue = nil
			c.mu.Unlock()
			return
		}
		d := c.queue[0]
		c.queue[0] = nil
		c.queue = c.queue[1:]
		if d.e.queued == d {
			d.e.queued = nil
		}
		run := d.removed || !d.e.gone
		rep := d.rep
		if d.replay {
			rep = c.currentLocked(d.e)
		}
		onResult, onChange, onRemove, sinks := c.cfg.OnResult, c.cfg.OnChange, c.cfg.OnRemove, c.sinks
		c.mu.Unlock()
		if !run {
			continue
		}
		if rep.Time.IsZero() {
			rep.Time = c.clock.Now()
		}
		if d.removed {
			if onRemove != nil {
				onRemove(d.e.key, d.e.generation)
			}
			continue
		}
		if d.replay {
			for _, s := range sinks {
				s.fn(rep)
			}
			continue
		}
		// The sinks come last: they report to pools, which may deliver
		// pool events that remove the key.
		if onResult != nil {
			onResult(rep)
		}
		if rep.Changed() && onChange != nil {
			onChange(rep)
		}
		for _, s := range sinks {
			s.fn(rep)
		}
	}
}

// currentLocked returns a report of e's current state that changes
// nothing.
func (c *Checker[K, G]) currentLocked(e *entry[K, G]) Report[K] {
	r := e.last
	r.Health, r.Previous = e.health, e.health
	return Report[K]{Result: r, Stats: e.lat.stats(), Target: e.target, Key: e.key, Generation: e.generation}
}

func (c *Checker[K, G]) reportLocked(e *entry[K, G], r Result) {
	c.enqueueLocked(&delivery[K, G]{rep: Report[K]{Result: r, Stats: e.lat.stats(), Target: e.target, Key: e.key, Generation: e.generation}, e: e})
}

func (c *Checker[K, G]) newEntryLocked(key K, groups map[G]struct{}) *entry[K, G] {
	c.nextGen++
	e := &entry[K, G]{key: key, groups: groups, generation: c.nextGen}
	c.entries[key] = e
	return e
}

// putLocked adds or updates the entry for key and queues its scheduling.
func (c *Checker[K, G]) putLocked(key K, t Target, w *work[K, G]) *entry[K, G] {
	e := c.entries[key]
	if e != nil && (e.target.Network != t.Network || e.target.Address != t.Address) {
		// A different address is a different target: start a new
		// generation with the same owners, and announce that health is
		// unknown again.
		old := e
		c.dropLocked(old, w, false)
		e = c.newEntryLocked(key, old.groups)
		e.direct = old.direct
		e.target = t
		if old.health != upstream.HealthUnknown {
			c.reportLocked(e, Result{Health: upstream.HealthUnknown, Previous: old.health}) // timed by the dispatcher
		}
	}
	if e == nil {
		e = c.newEntryLocked(key, make(map[G]struct{}))
	}
	e.target = t
	w.schedule = append(w.schedule, e)
	return e
}

// dropLocked removes e. With unlink it also leaves its groups and is
// announced to OnRemove.
func (c *Checker[K, G]) dropLocked(e *entry[K, G], w *work[K, G], unlink bool) {
	e.gone = true
	delete(c.entries, e.key)
	if unlink {
		for g := range e.groups {
			c.unlinkLocked(g, e.key)
		}
		c.enqueueLocked(&delivery[K, G]{e: e, removed: true})
	}
	if e.stop != nil {
		w.stops = append(w.stops, e.stop)
		e.stop = nil
	}
	if e.cancel != nil {
		w.cancels = append(w.cancels, e.cancel)
		e.cancel = nil
	}
}

func (c *Checker[K, G]) unlinkLocked(g G, key K) {
	if keys := c.groups[g]; keys != nil {
		delete(keys, key)
		if len(keys) == 0 {
			delete(c.groups, g)
		}
	}
}

// releaseLocked drops group g's hold on e and removes e if nothing holds
// it any more.
func (c *Checker[K, G]) releaseLocked(g G, e *entry[K, G], w *work[K, G]) {
	delete(e.groups, g)
	c.unlinkLocked(g, e.key)
	if len(e.groups) == 0 && !e.direct {
		c.dropLocked(e, w, true)
	}
}

// Put adds a target under key, or updates the target held under key, and
// holds it directly: it stays until Remove, whatever its groups do. It
// returns the target's generation, or 0 if the Checker is closed.
func (c *Checker[K, G]) Put(key K, t Target) uint64 {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	var w work[K, G]
	e := c.putLocked(key, t, &w)
	e.direct = true
	gen := e.generation
	c.mu.Unlock()
	c.finish(&w)
	return gen
}

// AddToGroup adds a target under key to group g, or updates the target
// held under key. It returns the target's generation, or 0 if the Checker
// is closed.
func (c *Checker[K, G]) AddToGroup(g G, key K, t Target) uint64 {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	var w work[K, G]
	gen := c.addLocked(g, key, t, &w).generation
	c.mu.Unlock()
	c.finish(&w)
	return gen
}

func (c *Checker[K, G]) addLocked(g G, key K, t Target, w *work[K, G]) *entry[K, G] {
	e := c.putLocked(key, t, w)
	e.groups[g] = struct{}{}
	keys := c.groups[g]
	if keys == nil {
		keys = make(map[K]struct{})
		c.groups[g] = keys
	}
	keys[key] = struct{}{}
	return e
}

// SyncGroup makes the keys of group g exactly those of targets in one
// step: keys not in targets leave the group (and the Checker, if nothing
// else holds them), keys new to the group join it, and every target is
// updated. Unchanged targets keep their health and schedule.
func (c *Checker[K, G]) SyncGroup(g G, targets map[K]Target) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var w work[K, G]
	for key := range c.groups[g] {
		if _, keep := targets[key]; !keep {
			c.releaseLocked(g, c.entries[key], &w)
		}
	}
	for key, t := range targets {
		c.addLocked(g, key, t, &w)
	}
	c.mu.Unlock()
	c.finish(&w)
}

// Remove removes key from the Checker and from all its groups. It reports
// false if there is no such key.
func (c *Checker[K, G]) Remove(key K) bool {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil {
		c.mu.Unlock()
		return false
	}
	var w work[K, G]
	c.dropLocked(e, &w, true)
	c.mu.Unlock()
	c.finish(&w)
	return true
}

// CheckNow starts a check of the target held under key at once instead of
// at its scheduled time, for example after real traffic to it failed. It
// does nothing if a check of the target is already running, and reports
// false if there is no such key or the target is not checked.
func (c *Checker[K, G]) CheckNow(key K) bool {
	c.mu.Lock()
	e := c.entries[key]
	if c.closed || e == nil || !c.checkableLocked(e) {
		c.mu.Unlock()
		return false
	}
	if e.running {
		c.mu.Unlock()
		return true
	}
	e.now = true
	c.mu.Unlock()
	c.schedule(e)
	return true
}

// addSink registers s unless the Checker is closed, and then also
// announces a goroutine that will call enter.
func (c *Checker[K, G]) addSink(s *sink[K]) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.sinks = append(slices.Clone(c.sinks), s)
	c.pending++
	return true
}

// replay passes the current state of key to the sinks, in order with the
// reports: a pool member that joins a key, such as one of a pool attached
// after the key's first check, learns its verdict now rather than at the
// next check.
func (c *Checker[K, G]) replay(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.enqueueLocked(&delivery[K, G]{e: e, replay: true})
	}
}

func (c *Checker[K, G]) removeSink(s *sink[K]) {
	c.mu.Lock()
	if i := slices.Index(c.sinks, s); i >= 0 {
		sinks := make([]*sink[K], 0, len(c.sinks)-1)
		sinks = append(sinks, c.sinks[:i]...)
		c.sinks = append(sinks, c.sinks[i+1:]...)
	}
	c.mu.Unlock()
}

// RemoveFromGroup drops group g's hold on key, removing key if nothing
// else holds it. It reports false if key is not in g.
func (c *Checker[K, G]) RemoveFromGroup(g G, key K) bool {
	c.mu.Lock()
	e := c.entries[key]
	if _, in := c.groups[g][key]; !in || e == nil {
		c.mu.Unlock()
		return false
	}
	var w work[K, G]
	c.releaseLocked(g, e, &w)
	c.mu.Unlock()
	c.finish(&w)
	return true
}

// RemoveGroup drops group g's hold on all its keys, removing those that
// nothing else holds, and returns the number of keys the group had.
func (c *Checker[K, G]) RemoveGroup(g G) int {
	c.mu.Lock()
	keys := c.groups[g]
	n := len(keys)
	var w work[K, G]
	for key := range keys {
		c.releaseLocked(g, c.entries[key], &w)
	}
	c.mu.Unlock()
	c.finish(&w)
	return n
}

// GroupKeys returns the keys of group g, in no particular order.
func (c *Checker[K, G]) GroupKeys(g G) []K {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]K, 0, len(c.groups[g]))
	for k := range c.groups[g] {
		keys = append(keys, k)
	}
	return keys
}

// Groups returns the groups that hold at least one key, in no particular
// order.
func (c *Checker[K, G]) Groups() []G {
	c.mu.Lock()
	defer c.mu.Unlock()
	gs := make([]G, 0, len(c.groups))
	for g := range c.groups {
		gs = append(gs, g)
	}
	return gs
}

// GroupsOf returns the groups that hold key, in no particular order.
func (c *Checker[K, G]) GroupsOf(key K) []G {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return nil
	}
	gs := make([]G, 0, len(e.groups))
	for g := range e.groups {
		gs = append(gs, g)
	}
	return gs
}

// Len returns the number of keys.
func (c *Checker[K, G]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Status returns a snapshot of the target held under key.
func (c *Checker[K, G]) Status(key K) (Status[K], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return Status[K]{}, false
	}
	return c.statusLocked(e), true
}

// Statuses returns a snapshot of every target, in no particular order.
func (c *Checker[K, G]) Statuses() []Status[K] {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := make([]Status[K], 0, len(c.entries))
	for _, e := range c.entries {
		s = append(s, c.statusLocked(e))
	}
	return s
}

func (c *Checker[K, G]) statusLocked(e *entry[K, G]) Status[K] {
	return Status[K]{
		Last:       e.last,
		Stats:      e.lat.stats(),
		Target:     e.target,
		Key:        e.key,
		Generation: e.generation,
		Checks:     e.checks,
		Failures:   e.failures,
		Health:     e.health,
		Checking:   c.checkableLocked(e),
	}
}

// Close stops the Checker: it cancels every check in progress, stops every
// timer, discards queued callbacks and forgets all targets. No callback
// starts once Close has begun, and no check result is reported. Close
// does not wait; use Done or Wait for that. It may be called from anywhere,
// including probes and callbacks, any number of times.
func (c *Checker[K, G]) Close() {
	if c.stopAfter() {
		c.unannounce()
	}
	c.cancel()
	c.shutdown()
}

// Done returns a channel that is closed once the Checker has been closed,
// by Close or by its context, and every goroutine it started has ended: no
// probe or callback of the Checker is running then. A probe or callback
// that waits on Done waits for itself.
func (c *Checker[K, G]) Done() <-chan struct{} { return c.done }

// Wait waits until Done is closed or ctx is done, and returns ctx's error
// in the latter case.
func (c *Checker[K, G]) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Checker[K, G]) shutdown() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	var w work[K, G]
	for _, e := range c.entries {
		c.dropLocked(e, &w, false)
	}
	clear(c.groups)
	c.queue = nil
	c.sinks = nil
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	c.cancel() // also ends the attachments of Attach
	c.finish(&w)
	c.mu.Lock()
	c.endLocked()
	c.mu.Unlock()
}

// optionsLocked merges the target's options over the Config's as given,
// then applies the defaults, so that defaults derived from Interval follow
// the target's interval.
func (c *Checker[K, G]) optionsLocked(e *entry[K, G]) Options {
	return e.target.Options.over(c.cfg.Options).withDefaults()
}

func (c *Checker[K, G]) probeLocked(t Target) Probe {
	if t.Probe != nil {
		return t.Probe
	}
	if isTCP(t.Network) {
		return c.tcp
	}
	return nil
}

func (c *Checker[K, G]) checkableLocked(e *entry[K, G]) bool {
	return !c.optionsLocked(e).Disabled && c.probeLocked(e.target) != nil
}

// schedule arranges the next check of e, replacing any pending one, or,
// if e is not to be checked, stops its timer and forgets its health. While
// a check of e runs, schedule only cancels it if e should no longer be
// checked; the check schedules e again when it ends.
//
// pending counts a timer from here until either its check starts or a stop
// cancels it. The clock is called without the lock; the timer number
// catches a Remove, Close or newer schedule that runs in between.
func (c *Checker[K, G]) schedule(e *entry[K, G]) {
	now := c.clock.Now()
	c.mu.Lock()
	if c.closed || e.gone || e.running {
		var cancel context.CancelFunc
		if e.cancel != nil && !c.closed && !e.gone && !c.checkableLocked(e) {
			cancel, e.cancel = e.cancel, nil
		}
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	old := e.stop
	e.stop = nil
	e.timer++
	timer := e.timer
	if !c.checkableLocked(e) {
		if e.health != upstream.HealthUnknown {
			prev := e.health
			e.health, e.ok, e.fail = upstream.HealthUnknown, 0, 0
			c.reportLocked(e, Result{Time: now, Health: upstream.HealthUnknown, Previous: prev})
		}
		c.mu.Unlock()
		if old != nil && old() {
			c.unannounce()
		}
		return
	}
	d := c.delayLocked(e, now)
	c.pending++
	c.mu.Unlock()
	if old != nil && old() {
		c.unannounce()
	}

	stop := c.clock.AfterFunc(d, func() { c.run(e, timer) })

	c.mu.Lock()
	if c.closed || e.gone || e.timer != timer {
		c.mu.Unlock()
		if stop() {
			c.unannounce()
		}
		return
	}
	e.stop = stop
	c.mu.Unlock()
}

func (c *Checker[K, G]) delayLocked(e *entry[K, G], now time.Time) time.Duration {
	if e.lastEnd.IsZero() || e.now {
		return 0
	}
	o := c.optionsLocked(e)
	d := o.Interval
	if e.health == upstream.HealthDown {
		d = o.UnhealthyInterval
	}
	if j := min(o.Jitter, d); j > 0 {
		d += rand.N(2*j+1) - j
	}
	return max(e.lastEnd.Add(d).Sub(now), 0)
}

func (c *Checker[K, G]) run(e *entry[K, G], timer uint64) {
	c.enter()
	defer c.leave()
	c.mu.Lock()
	if c.closed || e.gone || e.timer != timer || e.running || !c.checkableLocked(e) {
		c.mu.Unlock()
		return
	}
	e.stop = nil
	e.running = true
	e.now = false
	t := e.target
	o := c.optionsLocked(e)
	probe := c.probeLocked(t)
	ctx, cancel := context.WithCancel(c.ctx)
	e.cancel = cancel
	c.mu.Unlock()

	r, end, ok := c.check(ctx, probe, t, o)
	canceled := ctx.Err() != nil // by Remove, an address change, disabling or Close
	cancel()

	c.mu.Lock()
	e.cancel = nil
	e.running = false
	if ok && !canceled && !c.closed && !e.gone && c.checkableLocked(e) {
		e.checks++
		if r.Err != nil {
			e.failures++
		} else {
			e.lat.add(r.Latency, c.window)
		}
		r.Previous = e.health
		r.Health = e.observe(r.Err == nil, o.Rise, o.Fall)
		e.last = r
		e.lastEnd = end
		c.enqueueLocked(&delivery[K, G]{rep: Report[K]{Result: r, Stats: e.lat.stats(), Target: t, Key: e.key, Generation: e.generation}, e: e})
	}
	c.mu.Unlock()
	c.schedule(e)
}

// check runs one probe under the concurrency bound and returns its result
// and when it ended. ok is false if ctx ended first. The timeout starts
// once a slot is free.
func (c *Checker[K, G]) check(ctx context.Context, probe Probe, t Target, o Options) (r Result, end time.Time, ok bool) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return r, end, false
	}
	defer func() { <-c.sem }()
	start := c.clock.Now()
	pctx, cancel := context.WithTimeout(ctx, o.Timeout)
	obs, err := probe(pctx, t)
	cancel()
	elapsed := c.clock.Now().Sub(start)
	if obs.Latency <= 0 {
		obs.Latency = elapsed
	}
	return Result{Time: start, Err: err, Addr: obs.Addr, Latency: obs.Latency, Reason: Classify(err)}, start.Add(elapsed), true
}

// observe applies one result to the rise and fall counters. Health changes
// only when a run of results reaches its threshold while the target is in
// the opposite state, so a long run of failures changes it once.
func (e *entry[K, G]) observe(pass bool, rise, fall int) upstream.Health {
	if pass {
		e.fail = 0
		e.ok++
		if e.health == upstream.HealthUnknown || (e.health == upstream.HealthDown && e.ok >= rise) {
			e.health = upstream.HealthUp
		}
	} else {
		e.ok = 0
		e.fail++
		if e.health == upstream.HealthUnknown || (e.health == upstream.HealthUp && e.fail >= fall) {
			e.health = upstream.HealthDown
		}
	}
	return e.health
}

// latencyWindow keeps the latency statistics of one target.
type latencyWindow struct {
	ring    []time.Duration
	last    time.Duration
	average time.Duration
	next    int
}

func (w *latencyWindow) add(d time.Duration, size int) {
	if w.ring == nil {
		w.ring = make([]time.Duration, 0, size)
		w.average = d
	} else {
		w.average += (d - w.average) / 4
	}
	w.last = d
	if len(w.ring) < size {
		w.ring = append(w.ring, d)
	} else {
		w.ring[w.next] = d
		w.next = (w.next + 1) % size
	}
}

func (w *latencyWindow) stats() LatencyStats {
	if len(w.ring) == 0 {
		return LatencyStats{}
	}
	return LatencyStats{
		Last:    w.last,
		Average: w.average,
		Min:     slices.Min(w.ring),
		Max:     slices.Max(w.ring),
		Samples: len(w.ring),
	}
}
