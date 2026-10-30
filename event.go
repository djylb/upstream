package upstream

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
)

// EventKind says what an Event reports.
type EventKind uint8

const (
	// EventAdded reports a new member.
	EventAdded EventKind = iota

	// EventUpdated reports a member replaced with a new weight or value;
	// Backend is the new version. Updates that change neither emit none.
	EventUpdated

	// EventRemoved reports a member that left the pool.
	EventRemoved

	// EventStateChanged reports a change of a member's State.
	EventStateChanged

	// EventBalancerPanic reports that the Balancer panicked while the pool
	// built its view. Err is the *BalancerPanicError; Backend is nil. The
	// pool keeps routing with the view it published before, and builds
	// again on its next change.
	EventBalancerPanic
)

// String returns the name of the kind.
//
//goland:noinspection GoMixedReceiverTypes
func (k EventKind) String() string {
	switch k {
	case EventAdded:
		return "added"
	case EventUpdated:
		return "updated"
	case EventRemoved:
		return "removed"
	case EventStateChanged:
		return "state-changed"
	case EventBalancerPanic:
		return "balancer-panic"
	}
	return "EventKind(" + strconv.Itoa(int(k)) + ")"
}

// MarshalText encodes the kind as its name.
//
//goland:noinspection GoMixedReceiverTypes
func (k EventKind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

// An Event reports a change in a Pool. State changes are edge-triggered:
// a backend that keeps failing produces one event when it goes down, not
// one per failure.
type Event[T any] struct {
	// Err is the cause of an EventStateChanged to StateUnhealthy (the last
	// check error) or to StateEjected or StateHalfOpen (the ejection
	// cause), the *BalancerPanicError of an EventBalancerPanic, and nil
	// otherwise.
	Err error

	// Backend is the member concerned; nil for EventBalancerPanic.
	Backend *Backend[T]

	// Kind says what happened.
	Kind EventKind

	// From and To are the backend's states before and after the event.
	From, To State
}

type subscriber[T any] struct {
	fn  func(Event[T])
	off atomic.Bool
}

type delivery[T any] struct {
	ev Event[T]
	to []*subscriber[T]
}

// Subscribe calls fn for every later event, starting with an EventAdded
// for each current member, and returns a function that ends the
// subscription.
//
// Events are delivered one at a time in the order they happened, never
// while the pool holds a lock, on the goroutine that caused them or on one
// already delivering events. fn may call any Pool method; events that call
// causes are delivered after fn returns. fn should be quick, because the
// goroutine delivering events may be one releasing a lease. An event may be
// delivered slightly before picks observe the change it reports. After cancel
// returns, fn may still be running for an event already in progress but is
// not called again.
func (p *Pool[T]) Subscribe(fn func(Event[T])) (cancel func()) {
	s := &subscriber[T]{fn: fn}
	p.mu.Lock()
	if !p.closed {
		p.subs = append(slices.Clone(p.subs), s)
		only := []*subscriber[T]{s}
		p.qmu.Lock()
		for _, b := range p.order {
			p.queue = append(p.queue, delivery[T]{
				ev: Event[T]{Kind: EventAdded, Backend: b, From: b.core.reported, To: b.core.reported},
				to: only,
			})
		}
		p.qmu.Unlock()
	}
	p.mu.Unlock()
	p.flush()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.off.Store(true)
			p.mu.Lock()
			if i := slices.Index(p.subs, s); i >= 0 {
				subs := make([]*subscriber[T], 0, len(p.subs)-1)
				subs = append(subs, p.subs[:i]...)
				p.subs = append(subs, p.subs[i+1:]...)
			}
			p.mu.Unlock()
		})
	}
}

// emitLocked queues ev for the current subscribers. The caller holds mu.
func (p *Pool[T]) emitLocked(ev Event[T]) {
	if len(p.subs) == 0 {
		return
	}
	p.qmu.Lock()
	p.queue = append(p.queue, delivery[T]{ev: ev, to: p.subs})
	p.qmu.Unlock()
}

// flush delivers queued events unless another goroutine is already doing
// so, in which case that goroutine delivers them. No lock is held while a
// subscriber runs. If a subscriber panics, the events not yet delivered go
// back to the front of the queue and the next flush delivers them.
func (p *Pool[T]) flush() {
	p.qmu.Lock()
	if p.flushing {
		p.qmu.Unlock()
		return
	}
	p.flushing = true
	var batch []delivery[T] // being delivered, without qmu
	defer func() {
		if batch != nil { // a subscriber panicked
			p.qmu.Lock()
			p.queue = append(batch, p.queue...)
		}
		p.flushing = false
		p.qmu.Unlock()
	}()
	for len(p.queue) > 0 {
		batch = p.queue
		p.queue = nil
		p.qmu.Unlock()
		for len(batch) > 0 {
			d := &batch[0]
			for len(d.to) > 0 {
				s := d.to[0]
				d.to = d.to[1:] // a panicking subscriber is not called again for d
				if !s.off.Load() {
					s.fn(d.ev)
				}
			}
			batch = batch[1:]
		}
		batch = nil
		p.qmu.Lock()
	}
}

// A BalancerPanicError is the Err of an EventBalancerPanic: what a
// Balancer's Build or Rebuild panicked with, and where.
type BalancerPanicError struct {
	// Value is the value passed to panic.
	Value any

	// Stack is the stack of the panicking goroutine, as runtime/debug.Stack
	// formats it.
	Stack []byte
}

func (e *BalancerPanicError) Error() string {
	return "upstream: balancer panicked: " + fmt.Sprint(e.Value)
}

// Unwrap returns Value if it is an error, such as a runtime.Error.
func (e *BalancerPanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
