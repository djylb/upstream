package health

import (
	"context"
	"errors"
	"sync"

	"github.com/djylb/upstream"
)

// ErrClosed means Attach was given a closed Checker.
var ErrClosed = errors.New("health: checker closed")

// Attach makes group g of c follow the membership of pool: every member is
// checked with the Target that target returns for it, and every result is
// reported to the pool with Pool.ReportCheck, which takes members out of
// rotation while they are down. A nil target checks each member's address,
// as upstream.AddressOf finds it, over TCP; for a pool whose values have
// no address, Attach returns upstream.ErrNoAddress. Attach returns
// ErrClosed if c is closed and upstream.ErrClosed if pool is.
//
// The Checker holds the targets under their address, written as
// TargetKey writes it, so that several pools attached to one Checker under
// different groups, as a tunnel client does with one pool per tunnel,
// share the check of an address that more than one of them uses, and so
// that members of different pools with one ID but different addresses
// stay apart. Pools that share an address share its Probe and Options:
// the latest target given for it wins. A pool that joins an address
// already checked takes its current verdict at once.
//
// Reports are matched to members by generation: a report about a target
// that has since been removed and added again never reaches the new
// members, and a report of a newer generation than the attachment has
// seen, which another pool sharing the address started, replaces the
// generation it knows. The pool owns group g; do not change the group by
// other means.
//
// The attachment ends when detach is called, when the pool closes or when
// the Checker closes; detach is idempotent and safe to call from any
// callback. Ending it removes the group from the Checker and, unless the
// pool closed, sets the health of the pool's members back to
// upstream.HealthUnknown, so that no member stays out of rotation on a
// verdict that nothing will revise.
func Attach[G comparable, T any](c *Checker[string, G], g G, pool *upstream.Pool[T], target func(*upstream.Backend[T]) Target) (detach func(), err error) {
	if target == nil {
		if target = addressTarget[T](); target == nil {
			return nil, upstream.ErrNoAddress
		}
	}
	select {
	case <-pool.Done():
		return nil, upstream.ErrClosed
	default:
	}
	a := newAttachment(c, g, pool, target, func(_ string, t Target) string { return TargetKey(t) })
	if !a.start() {
		return nil, ErrClosed
	}
	return a.detach, nil
}

// TargetKey returns the key under which Attach holds t in a Checker: its
// address, preceded by its network and a slash unless that is "tcp" or
// empty, such as "10.0.0.5:22" or "udp/10.0.0.9:53".
func TargetKey(t Target) string {
	if n := normNetwork(t.Network); n != "tcp" {
		return n + "/" + t.Address
	}
	return t.Address
}

// addressTarget returns the target function for a nil target, or nil if
// the pool's values have no address.
func addressTarget[T any]() func(*upstream.Backend[T]) Target {
	if !upstream.Addressable[T]() {
		return nil
	}
	return func(b *upstream.Backend[T]) Target {
		addr, _ := upstream.AddressOf(b.Value())
		return Target{Address: addr}
	}
}

type member[T any] struct {
	b   *upstream.Backend[T]
	key string
}

// keyState is what an attachment knows of one Checker key: the members
// checked under it and its generation.
type keyState struct {
	ids map[string]struct{}
	gen uint64
}

// An attachment connects a pool to a group of a Checker.
//
// Pool events arrive one at a time, so onEvent never overlaps itself;
// reports arrive one at a time on the Checker's dispatcher. Neither holds
// mu while calling the Checker, the pool or target. Ending the attachment
// sets off; whoever leaves last, the ending call or a report in progress,
// resets the members' health, so that no report lands after the reset.
type attachment[G comparable, T any] struct {
	c         *Checker[string, G]
	pool      *upstream.Pool[T]
	target    func(*upstream.Backend[T]) Target
	keyOf     func(id string, t Target) string
	sink      *sink[string]
	stop      chan struct{} // closed when the attachment ends
	onEnd     func()        // run once the attachment has ended, if not nil
	mu        sync.Mutex
	cancel    func()               // ends the pool subscription; guarded by mu
	members   map[string]member[T] // by member ID; guarded by mu
	keys      map[string]*keyState // by Checker key; guarded by mu
	reporting int                  // reports in progress; guarded by mu
	g         G
	off       bool // guarded by mu
	reset     bool // a reset is left to the last report; guarded by mu
}

func newAttachment[G comparable, T any](c *Checker[string, G], g G, pool *upstream.Pool[T], target func(*upstream.Backend[T]) Target, keyOf func(string, Target) string) *attachment[G, T] {
	a := &attachment[G, T]{
		c:       c,
		pool:    pool,
		target:  target,
		keyOf:   keyOf,
		g:       g,
		stop:    make(chan struct{}),
		members: make(map[string]member[T]),
		keys:    make(map[string]*keyState),
	}
	a.sink = &sink[string]{fn: a.report}
	return a
}

// start connects the attachment and reports false if the Checker is
// closed.
func (a *attachment[G, T]) start() bool {
	if !a.c.addSink(a.sink) {
		return false
	}
	cancel := a.pool.Subscribe(a.onEvent)
	a.mu.Lock()
	off := a.off
	if !off {
		a.cancel = cancel
	}
	a.mu.Unlock()
	if off {
		cancel()
	}
	go a.wait()
	return true
}

// detach ends the attachment from the outside.
func (a *attachment[G, T]) detach() { a.end(false) }

// wait ends the attachment with the pool or the Checker. It runs as one
// of the Checker's goroutines, announced by addSink.
func (a *attachment[G, T]) wait() {
	a.c.enter()
	defer a.c.leave()
	select {
	case <-a.pool.Done():
		a.end(true)
	case <-a.c.ctx.Done():
		a.end(false)
	case <-a.stop:
	}
	if a.onEnd != nil {
		a.onEnd()
	}
}

func (a *attachment[G, T]) end(poolClosed bool) {
	a.mu.Lock()
	if a.off {
		a.mu.Unlock()
		return
	}
	a.off = true
	close(a.stop)
	cancel := a.cancel
	a.cancel = nil
	var reset []*upstream.Backend[T]
	if !poolClosed {
		if a.reporting > 0 {
			a.reset = true
		} else {
			reset = a.backendsLocked()
		}
	}
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	a.c.removeSink(a.sink)
	a.c.RemoveGroup(a.g)
	resetHealth(a.pool, reset)
}

func (a *attachment[G, T]) backendsLocked() []*upstream.Backend[T] {
	bs := make([]*upstream.Backend[T], 0, len(a.members))
	for _, m := range a.members {
		bs = append(bs, m.b)
	}
	return bs
}

// keyBackendsLocked returns the members checked under ks.
func (a *attachment[G, T]) keyBackendsLocked(ks *keyState) []*upstream.Backend[T] {
	bs := make([]*upstream.Backend[T], 0, len(ks.ids))
	for id := range ks.ids {
		bs = append(bs, a.members[id].b)
	}
	return bs
}

// resetHealth sets the health of bs back to unknown, keeping their last
// check result.
func resetHealth[T any](pool *upstream.Pool[T], bs []*upstream.Backend[T]) {
	for _, b := range bs {
		pool.ReportCheck(b, upstream.HealthUnknown, b.Status().LastCheck)
	}
}

// onEvent follows the pool's membership.
func (a *attachment[G, T]) onEvent(ev upstream.Event[T]) {
	switch ev.Kind {
	case upstream.EventAdded, upstream.EventUpdated:
		id := ev.Backend.ID()
		t := a.target(ev.Backend)
		key := a.keyOf(id, t)
		if a.isOff() {
			return
		}
		gen := a.c.AddToGroup(a.g, key, t)
		a.mu.Lock()
		if a.off {
			// The attachment ended while the target was added, perhaps
			// after its group was removed: take the target out again.
			a.mu.Unlock()
			a.c.RemoveFromGroup(a.g, key)
			return
		}
		old, had := a.members[id]
		a.members[id] = member[T]{b: ev.Backend, key: key}
		ks := a.keys[key]
		if ks == nil {
			ks = &keyState{ids: make(map[string]struct{})}
			a.keys[key] = ks
		}
		ks.ids[id] = struct{}{}
		fresh := !had || old.key != key || gen > ks.gen
		ks.gen = max(ks.gen, gen)
		drop := ""
		if had && old.key != key && a.leaveKeyLocked(id, old.key) {
			drop = old.key
		}
		a.mu.Unlock()
		if drop != "" {
			a.c.RemoveFromGroup(a.g, drop)
		}
		if fresh {
			// Reports that came before the generation was recorded were
			// dropped, and the key may have a verdict already: the
			// Checker passes on its current state, in order with its
			// reports.
			a.c.replay(key)
		}
	case upstream.EventRemoved:
		id := ev.Backend.ID()
		a.mu.Lock()
		m, had := a.members[id]
		if a.off || !had {
			a.mu.Unlock()
			return
		}
		delete(a.members, id)
		drop := a.leaveKeyLocked(id, m.key)
		a.mu.Unlock()
		if drop {
			a.c.RemoveFromGroup(a.g, m.key)
		}
	case upstream.EventStateChanged:
		// The Checker set the state; nothing to follow.
	case upstream.EventBalancerPanic:
		// Concerns no member; Backend is nil.
	}
}

// leaveKeyLocked takes member id off key and reports whether no member is
// left on it, in which case the group lets go of the key.
func (a *attachment[G, T]) leaveKeyLocked(id, key string) bool {
	ks := a.keys[key]
	if ks == nil {
		return false
	}
	delete(ks.ids, id)
	if len(ks.ids) > 0 {
		return false
	}
	delete(a.keys, key)
	return true
}

func (a *attachment[G, T]) isOff() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.off
}

// report passes a report on to the members checked under its key. A
// report of an older generation than the one recorded is about a target
// that has since been replaced, and is dropped. A newer one is adopted: the
// Checker's generations only grow, and with keys that name the address
// (Attach) or the member (Watch) a newer generation is the same target
// started again, such as by another pool that shares the address or by a
// changed network. A member that joins a key gets the key's current state
// replayed by onEvent.
func (a *attachment[G, T]) report(r Report[string]) {
	a.mu.Lock()
	ks := a.keys[r.Key]
	if a.off || ks == nil || r.Generation < ks.gen {
		a.mu.Unlock()
		return
	}
	ks.gen = r.Generation
	bs := a.keyBackendsLocked(ks)
	a.reporting++
	a.mu.Unlock()
	a.apply(bs, r)
}

// apply reports r to the pool for bs. The caller counted it in reporting.
func (a *attachment[G, T]) apply(bs []*upstream.Backend[T], r Report[string]) {
	defer a.applied()
	for _, b := range bs {
		a.pool.ReportCheck(b, r.Health, r.CheckResult)
	}
}

func (a *attachment[G, T]) applied() {
	a.mu.Lock()
	a.reporting--
	var reset []*upstream.Backend[T]
	if a.reporting == 0 && a.reset {
		a.reset = false
		reset = a.backendsLocked()
	}
	a.mu.Unlock()
	resetHealth(a.pool, reset)
}

// Watch starts a Checker that follows the membership of pool, as Attach
// describes, and returns it as a Watcher. Unlike Attach, it holds each
// member under its ID, so cfg's callbacks, which see every report, and
// the Watcher's methods take member IDs. A nil target checks each member's
// address over TCP, as Attach does; for a pool whose values have no
// address, Watch returns upstream.ErrNoAddress. It returns ctx's error if
// ctx is done and upstream.ErrClosed if pool is closed. The pool then
// announces health transitions as upstream.EventStateChanged events.
//
// The Watcher belongs to the pool: it closes when the pool closes.
// Closing it earlier ends its checks and sets the members' health back to
// upstream.HealthUnknown.
func Watch[T any](ctx context.Context, pool *upstream.Pool[T], target func(*upstream.Backend[T]) Target, cfg Config[string]) (*Watcher, error) {
	if target == nil {
		if target = addressTarget[T](); target == nil {
			return nil, upstream.ErrNoAddress
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-pool.Done():
		return nil, upstream.ErrClosed
	default:
	}
	c := New[string, struct{}](ctx, cfg)
	a := newAttachment(c, struct{}{}, pool, target, func(id string, _ Target) string { return id })
	a.onEnd = c.Close
	if !a.start() {
		// ctx ended in between.
		c.Close()
		return nil, context.Cause(ctx)
	}
	return &Watcher{c: c}, nil
}

// A Watcher is the health checker of one pool, started by Watch. Its
// targets are the pool's members, keyed by ID.
type Watcher struct {
	c *Checker[string, struct{}]
}

// Status returns a snapshot of the checks of the member with the given ID.
func (w *Watcher) Status(id string) (Status[string], bool) { return w.c.Status(id) }

// Statuses returns a snapshot of the checks of every member, in no
// particular order.
func (w *Watcher) Statuses() []Status[string] { return w.c.Statuses() }

// CheckNow checks the member with the given ID at once; see
// Checker.CheckNow.
func (w *Watcher) CheckNow(id string) bool { return w.c.CheckNow(id) }

// SetConfig replaces the configuration; see Checker.SetConfig.
func (w *Watcher) SetConfig(cfg Config[string]) { w.c.SetConfig(cfg) }

// Close stops the checks and sets the members' health back to
// upstream.HealthUnknown. It does not wait: the health is reset once Done
// is closed.
func (w *Watcher) Close() { w.c.Close() }

// Done returns a channel that is closed once the Watcher is closed and
// its goroutines have ended; see Checker.Done.
func (w *Watcher) Done() <-chan struct{} { return w.c.Done() }

// Wait waits until Done is closed or ctx is done, and returns ctx's error
// in the latter case.
func (w *Watcher) Wait(ctx context.Context) error { return w.c.Wait(ctx) }
