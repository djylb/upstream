package health

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/djylb/timeshim"
	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/latency"
)

// A Checker checks targets held under keys of type K, which may belong to
// groups of type G.
//
// Ownership: a key is held by a direct Put and by every group it was added
// to. It is checked once however many owners it has, and it leaves the
// Checker when Remove is called or when its last owner lets go of it. Two
// owners that give different Targets for one key share one target: the
// latest Put, AddToGroup or SyncGroup wins. Removing a key cancels its check in
// progress, discards its result and forgets its schedule and state. The
// probe holds its concurrency slot until it returns. Adding the key again
// starts a new generation.
//
// Changing a target's options, or the Config, takes effect at once: the
// next check is rescheduled from the end of the previous one. Changing a
// target's network or address starts a new generation as if the target had
// been removed and added, keeping its owners; an empty network and "tcp"
// are the same network.
//
// Pending checks and damping releases wait in time order behind a shared
// Clock timer, so scheduling costs O(log n) and the Checker holds one
// scheduling timer however many targets it has. Every check runs
// in its own goroutine, which exists only while the check runs.
// Config.Concurrency bounds how many run at once. When all slots are
// occupied, due checks stay queued. Callbacks run on one
// dispatching goroutine. The Checker owns all of these goroutines and its
// timers: Close, or the end of the context passed to New, stops them, and
// Done reports when the last has ended.
//
// All methods are safe for concurrent use and from probes and callbacks,
// and none of them waits for a probe or callback. No lock is held while a
// probe, the Clock or a callback runs.
type Checker[K, G comparable] struct {
	ctx           context.Context //nolint:containedctx // the Checker's lifetime, canceled by Close; probes and callbacks derive from it
	cancel        context.CancelFunc
	stopAfter     func() bool
	clock         timeshim.Clock
	limit, active int           // probe slots, guarded by mu
	notify        chan struct{} // wakes the dispatcher
	window        int

	done    chan struct{} // closed when closed and nothing is pending or running
	mu      sync.Mutex
	cfg     Config[K]            // guarded by mu; Options not defaulted
	tcp     Probe                // the default check for cfg; guarded by mu
	sinks   []*sink[K]           // reporters of Attach; copy on write, guarded by mu
	entries map[K]*entry[K, G]   // guarded by mu
	groups  map[G]map[K]struct{} // guarded by mu
	queue   []*delivery[K, G]    // callbacks to run, in order; guarded by mu
	pending int                  // timers and goroutines about to start; guarded by mu
	running int                  // checker goroutines and attachment target calls; guarded by mu
	nextGen uint64               // guarded by mu

	// The schedules share one timer for the earliest eligible deadline.
	sched    schedule[K, G] // checks
	releases schedule[K, G] // damping releases
	armAt    time.Time      // when the armed timer fires
	armTimer timeshim.Timer // nil while the timer is being made
	armSeq   uint64         // identifies the armed timer
	armed    bool           // a timer is armed or being made

	closed bool // guarded by mu
	ended  bool // done is closed; guarded by mu
}

// entry is the state of one generation of a key. All fields are guarded
// by the Checker's mutex.
type entry[K, G comparable] struct {
	lastEnd    time.Time
	touched    time.Time // the latest Touch while the target was up
	key        K
	damp       *damping
	cancel     context.CancelFunc // check in progress
	groups     map[G]struct{}
	queued     *delivery[K, G] // a queued report that changes nothing, which a newer one replaces
	last       Result
	lat        latencyWindow
	target     Target
	generation uint64
	timer      uint64 // identifies the scheduled check
	slot       int    // 1 + index in the Checker's schedule; 0 if not scheduled
	checks     uint64
	failures   uint64
	ok, fail   int
	health     upstream.Health
	hasTouch   bool
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
	e       *entry[K, G]
	replay  func(Report[K]) // deliver current state only to the joining member
	rep     Report[K]
	removed bool
	stamp   bool // timestamp an address change outside the checker lock
}

// New returns a Checker that runs until ctx is done or Close is called.
func New[K, G comparable](ctx context.Context, cfg Config[K]) *Checker[K, G] {
	c := &Checker[K, G]{
		clock:   timeshim.Or(cfg.Clock),
		window:  cfg.LatencyWindow,
		notify:  make(chan struct{}, 1),
		entries: make(map[K]*entry[K, G]),
		groups:  make(map[G]map[K]struct{}),
		done:    make(chan struct{}),
	}
	if c.window <= 0 {
		c.window = 16
	}
	n := cfg.Concurrency
	if n <= 0 {
		n = 16
	}
	c.limit = n
	c.setConfigLocked(cfg)
	c.ctx, c.cancel = context.WithCancel(ctx)       //nolint:gosec // c.cancel is called by shutdown
	c.pending = 2                                   // the dispatcher and the shutdown hook
	c.stopAfter = context.AfterFunc(c.ctx, func() { //nolint:contextcheck // runs after the context ended; shutdown needs no live one
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

// A pool can dispatch target on its own goroutine. Count that work so Done
// also waits for a target function that closes its watcher from inside.
func (c *Checker[K, G]) beginTarget() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.running++
	return true
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
	cfg.Damping = cfg.Damping.withDefaults()
	c.cfg = cfg
	o := cfg.TCP
	o.Clock = c.clock
	c.tcp = TCP(o)
}

// SetConfig replaces the configuration, except Clock, Concurrency and
// LatencyWindow, and reschedules every target under the new settings.
func (c *Checker[K, G]) SetConfig(cfg Config[K]) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	oldDamping := c.cfg.Damping
	c.setConfigLocked(cfg)
	var w work[K, G]
	for _, e := range c.entries {
		if oldDamping != c.cfg.Damping {
			if e.damp != nil && e.damp.slot != 0 {
				c.releaseDampingLocked(e)
			}
			c.clearDampingLocked(e)
		}
		w.schedule = append(w.schedule, e)
	}
	c.stopIdleTimerLocked(&w)
	c.mu.Unlock()
	c.finish(&w)
}

// work collects what an operation does after releasing the mutex.
type work[K, G comparable] struct {
	schedule []*entry[K, G]
	timers   []timeshim.Timer
	cancels  []context.CancelFunc
}

// finish runs the deferred part of an operation without the mutex.
func (c *Checker[K, G]) finish(w *work[K, G]) {
	for _, timer := range w.timers {
		if timer.Stop() {
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
	if d.replay != nil {
		// Later observations must stay on their side of the replay.
		d.e.queued = nil
	} else if !d.removed {
		edge := d.rep.Changed() || d.rep.DampingReleased
		if !edge && d.e.queued != nil {
			d.e.queued.rep = d.rep
			return
		}
		d.e.queued = nil
		if !edge {
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
		onResult, onChange, onRemove, sinks := c.cfg.OnResult, c.cfg.OnChange, c.cfg.OnRemove, c.sinks
		c.mu.Unlock()
		if !run {
			continue
		}
		if d.removed {
			if onRemove != nil && c.admit(nil, false) {
				onRemove(d.e.key, d.e.generation)
			}
			continue
		}
		if d.replay != nil {
			if c.admit(d.e, false) {
				d.replay(rep)
			}
			continue
		}
		if d.stamp {
			rep.Time = c.clock.Now()
		}
		if onResult != nil && c.admit(d.e, false) {
			onResult(rep)
		}
		if (rep.Changed() || rep.DampingReleased) && onChange != nil && c.admit(d.e, false) {
			onChange(rep)
		}
		for _, s := range sinks {
			if !c.admit(d.e, false) {
				break
			}
			s.fn(rep)
		}
	}
}

// admit is the start of a callback for this generation. Removal and Close
// revoke future admissions under the same lock. User code runs after unlock.
// OnRemove uses a nil entry because it reports the removal itself.
func (c *Checker[K, G]) admit(e *entry[K, G], probe bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && (e == nil || !e.gone) && (!probe || e != nil && c.checkableLocked(e))
}

// currentLocked returns a report of e's current state that changes
// nothing.
func (c *Checker[K, G]) currentLocked(e *entry[K, G]) Report[K] {
	r := e.last
	r.Health, r.Previous = e.health, e.health
	return Report[K]{Result: r, Stats: e.lat.stats(), Target: e.target, Key: e.key, Generation: e.generation}
}

func (c *Checker[K, G]) reportLocked(e *entry[K, G], r Result, stamp bool) {
	c.enqueueLocked(&delivery[K, G]{rep: Report[K]{Result: r, Stats: e.lat.stats(), Target: e.target, Key: e.key, Generation: e.generation}, e: e, stamp: stamp})
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
	sameAddress := e != nil && e.target.Address == t.Address && normNetwork(e.target.Network) == normNetwork(t.Network)
	keepSchedule := false
	if sameAddress {
		old := c.optionsLocked(e)
		next := t.Options.over(c.cfg.Options).withDefaults()
		keepSchedule = old.Interval == next.Interval && old.UnhealthyInterval == next.UnhealthyInterval && old.Jitter == next.Jitter && old.Disabled == next.Disabled && (c.probeLocked(t) != nil) == (c.probeLocked(e.target) != nil)
	}
	if e != nil && !sameAddress {
		// A different address is a different target: end the generation,
		// start a new one with the same owners, and announce that health
		// is unknown again.
		old := e
		c.dropLocked(old, w, false)
		c.enqueueLocked(&delivery[K, G]{e: old, removed: true})
		e = c.newEntryLocked(key, old.groups)
		e.direct = old.direct
		e.target = t
		if old.health != upstream.HealthUnknown {
			c.reportLocked(e, Result{Health: upstream.HealthUnknown, Previous: old.health}, true)
		}
	}
	if e == nil {
		e = c.newEntryLocked(key, make(map[G]struct{}))
	}
	e.target = t
	if !keepSchedule {
		w.schedule = append(w.schedule, e)
	}
	return e
}

// dropLocked removes e. With unlink it also leaves its groups and is
// announced to OnRemove; without, the caller announces it if the
// generation ends.
func (c *Checker[K, G]) dropLocked(e *entry[K, G], w *work[K, G], unlink bool) {
	e.gone = true
	delete(c.entries, e.key)
	if unlink {
		for g := range e.groups {
			c.unlinkLocked(g, e.key)
		}
		c.enqueueLocked(&delivery[K, G]{e: e, removed: true})
	}
	c.sched.remove(e.slot)
	c.clearDampingLocked(e)
	c.stopIdleTimerLocked(w)
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

// Touch records that real traffic just reached the target held under key:
// while the target is up, its next check moves to an interval from now, so
// that a target in use is not probed and only an idle one is. It costs a
// lock and no timer, so it suits a hot path. Touch does not change the
// target's health, and a target that is down or not yet judged is checked as
// scheduled. A target with failures below Fall still counts as up, so traffic
// also delays the checks that would confirm it down: call Touch only when the
// traffic proves what the probe would, and CheckNow when traffic fails. It
// reports whether the target was up; false also means there is no such key.
func (c *Checker[K, G]) Touch(key K) bool {
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if c.closed || e == nil || e.health != upstream.HealthUp {
		return false
	}
	if !e.hasTouch || now.After(e.touched) {
		e.touched = now
		e.hasTouch = true
	}
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

// replay passes the current state of key to a joining member, in order with the
// reports: a pool member that joins a key, such as one of a pool attached
// after the key's first check, learns its verdict now rather than at the
// next check.
func (c *Checker[K, G]) replay(key K, deliver func(Report[K])) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.enqueueLocked(&delivery[K, G]{e: e, rep: c.currentLocked(e), replay: deliver})
	}
}

func (c *Checker[K, G]) removeSink(s *sink[K]) {
	c.mu.Lock()
	if i := slices.Index(c.sinks, s); i >= 0 {
		c.sinks = slices.Concat(c.sinks[:i], c.sinks[i+1:])
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
// is admitted after shutdown. An admitted call may finish later. Close
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
	if isTCP(normNetwork(t.Network)) {
		return c.tcp
	}
	return nil
}

func (c *Checker[K, G]) checkableLocked(e *entry[K, G]) bool {
	return !c.optionsLocked(e).Disabled && c.probeLocked(e.target) != nil
}

func (c *Checker[K, G]) delayLocked(e *entry[K, G], now time.Time) time.Duration {
	if e.checks == 0 || e.now {
		return 0
	}
	o := c.optionsLocked(e)
	d := o.Interval
	if e.health == upstream.HealthDown {
		d = o.UnhealthyInterval
	}
	if j := min(o.Jitter, d); j > 0 {
		base := d - j
		offset := rand.Uint64N(uint64(j)*2 + 1)
		d = base + time.Duration(min(offset, math.MaxInt64-uint64(max(base, 0))))
	}
	from := e.lastEnd
	if e.health == upstream.HealthUp && e.hasTouch && e.touched.After(from) {
		from = e.touched
	}
	return max(from.Add(d).Sub(now), 0)
}

// touchedLocked reports whether traffic since the timer was set still
// postpones e's check. It allows for the largest jitter, so a timer that the
// jitter set early does not spin until the interval has passed.
func (c *Checker[K, G]) touchedLocked(e *entry[K, G], now time.Time) bool {
	if e.now || e.health != upstream.HealthUp || !e.hasTouch {
		return false
	}
	o := c.optionsLocked(e)
	return e.touched.Add(o.Interval - max(min(o.Jitter, o.Interval), 0)).After(now)
}

func (c *Checker[K, G]) run(e *entry[K, G], timer uint64) {
	c.enter()
	defer c.leave()
	defer func() {
		now := c.clock.Now()
		c.mu.Lock()
		c.active--
		c.startDueLocked(now)
	}()
	now := c.clock.Now()
	c.mu.Lock()
	if c.closed || e.gone || e.timer != timer || e.running || !c.checkableLocked(e) {
		c.mu.Unlock()
		return
	}
	if c.touchedLocked(e, now) {
		c.mu.Unlock()
		c.schedule(e)
		return
	}
	e.running = true
	e.now = false
	t := e.target
	o := c.optionsLocked(e)
	probe := c.probeLocked(t)
	ctx, cancel := context.WithCancel(c.ctx)
	e.cancel = cancel
	c.mu.Unlock()

	r, ok := c.check(ctx, e, probe, t, o)
	canceled := ctx.Err() != nil // by Remove, an address change, disabling or Close
	cancel()

	c.mu.Lock()
	e.cancel = nil
	e.running = false
	if ok && !canceled && !c.closed && !e.gone && e.timer == timer && c.checkableLocked(e) {
		e.checks++
		if r.Err != nil {
			e.failures++
		}
		e.lastEnd = r.FinishedAt
		c.observeLocked(e, t, r, o)
	}
	c.mu.Unlock()
	c.schedule(e)
}

// check runs one probe under the concurrency bound. ok is false if ctx
// ended first. The timeout starts once a slot is free.
func (c *Checker[K, G]) check(ctx context.Context, e *entry[K, G], probe Probe, t Target, o Options) (r Result, ok bool) {
	start := c.clock.Now()
	pctx, cancel := timeshim.WithTimeout(ctx, c.clock, o.Timeout)
	defer cancel()
	var obs Observation
	err := pctx.Err()
	if ctx.Err() != nil || !c.admit(e, true) {
		return r, false
	}
	if err == nil {
		obs, err = probe(pctx, t)
	}
	reason := Classify(err)
	if errors.Is(pctx.Err(), context.DeadlineExceeded) {
		reason = ReasonTimeout
		if !errors.Is(err, context.DeadlineExceeded) {
			err = errors.Join(context.DeadlineExceeded, err)
		}
	}
	end := c.clock.Now()
	if obs.Latency <= 0 {
		obs.Latency = end.Sub(start)
	}
	cr := upstream.CheckResult{Time: start, Err: err, Addr: obs.Addr, Latency: obs.Latency}
	return Result{CheckResult: cr, FinishedAt: end, Reason: reason}, true
}

// observe applies one result to the rise and fall counters. Health changes
// only when a run of results reaches its threshold while the target is in
// the opposite state, so a long run of failures changes it once.
func (e *entry[K, G]) observe(h upstream.Health, pass bool, rise, fall int) upstream.Health {
	if pass {
		e.fail = 0
		e.ok++
		if h == upstream.HealthUnknown || (h == upstream.HealthDown && e.ok >= rise) {
			h = upstream.HealthUp
		}
	} else {
		e.ok = 0
		e.fail++
		if h == upstream.HealthUnknown || (h == upstream.HealthUp && e.fail >= fall) {
			h = upstream.HealthDown
		}
	}
	return h
}

// latencyWindow keeps the latency statistics of one target.
type latencyWindow struct {
	ring    []time.Duration
	last    time.Duration
	average time.Duration
	next    int
}

func (w *latencyWindow) add(d time.Duration, size int) {
	w.average = latency.Update(w.average, d, 4, w.ring == nil)
	if w.ring == nil {
		w.ring = make([]time.Duration, 0, size)
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
