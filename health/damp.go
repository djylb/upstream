package health

import (
	"slices"
	"sync"
	"time"

	"github.com/djylb/upstream"
)

// DampConfig configures a Damper. Zero fields take their defaults.
type DampConfig struct {
	// Window is the period over which changes are counted. The default is
	// one minute.
	Window time.Duration

	// Flips is the number of changes within Window that are still
	// reported; one more suppresses the key. The default is 3.
	Flips int

	// Base is the first suppression time. The default is 30s.
	Base time.Duration

	// Max is the longest suppression time. Each suppression that follows
	// another within a Window doubles the time, up to Max. The default is
	// 5 minutes, and never less than Base.
	Max time.Duration
}

func (c DampConfig) withDefaults() DampConfig {
	if c.Window <= 0 {
		c.Window = time.Minute
	}
	if c.Flips <= 0 {
		c.Flips = 3
	}
	if c.Base <= 0 {
		c.Base = 30 * time.Second
	}
	if c.Max <= 0 {
		c.Max = 5 * time.Minute
	}
	c.Max = max(c.Max, c.Base)
	return c
}

// A Damper limits how often the health of keys is reported, so that a key
// that keeps changing between up and down does not flood its consumer. The
// Checker's Rise and Fall decide when a target's health changes; a Damper
// sits after that, on the stream of what is to be reported, and does not
// change any decision.
//
// A key's value is passed to emit when its health differs from the last
// one passed, until the key has changed more than Flips times within
// Window. Then reports for the key are held back for Base, and each
// further suppression doubles the time, up to Max; the latest value offered is passed
// when a suppression ends, even if it equals the one passed before. The
// time of a key that stays quiet is halved for every Window after its
// last suppression ended, and gone below Base. The first value offered for
// a key is always passed.
//
// A Damper is for use as the OnResult or OnChange hook of a Checker, which
// delivers reports one at a time, and is safe for concurrent use anyway.
type Damper[K comparable, V any] struct {
	cfg   DampConfig
	clock upstream.Clock
	emit  func(K, V)

	mu       sync.Mutex
	keys     map[K]*dampKey[K, V]
	queue    []dampOut[K, V]
	draining bool
	closed   bool
}

type dampOut[K comparable, V any] struct {
	key K
	val V
}

type dampKey[K comparable, V any] struct {
	flips    []time.Time
	until    time.Time
	hold     time.Duration
	pending  bool // val holds the latest value, held back; ph is its health
	ph       upstream.Health
	val      V
	has      bool // a value has been passed: reported is valid
	reported upstream.Health
	round    uint64
	timer    func() bool
}

// NewDamper returns a Damper that passes values to emit. A nil clock is
// upstream.SystemClock. emit is called on one goroutine at a time, in the
// order of the decisions, without any lock held; it may call any method of
// the Damper.
func NewDamper[K comparable, V any](clock upstream.Clock, cfg DampConfig, emit func(K, V)) *Damper[K, V] {
	if clock == nil {
		clock = upstream.SystemClock()
	}
	return &Damper[K, V]{cfg: cfg.withDefaults(), clock: clock, emit: emit, keys: map[K]*dampKey[K, V]{}}
}

// Offer gives the damper the latest health h of key and the value v that
// reports it. It may pass v to emit now, hold it back, or drop it as a
// repeat of what was passed last. Offer may return before the call to emit
// it caused has been made, when another goroutine is inside emit.
func (d *Damper[K, V]) Offer(key K, h upstream.Health, v V) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	k := d.keys[key]
	if k == nil {
		k = &dampKey[K, V]{}
		d.keys[key] = k
	}
	if d.decide(key, k, h, v) {
		d.enqueueLocked(key, v)
		return
	}
	d.mu.Unlock()
}

// decide is the rule above; true: pass v now.
func (d *Damper[K, V]) decide(key K, k *dampKey[K, V], h upstream.Health, v V) bool {
	now := d.clock.Now()
	for k.hold > 0 && now.Sub(k.until) >= d.cfg.Window {
		k.hold /= 2
		k.until = k.until.Add(d.cfg.Window)
		if k.hold < d.cfg.Base {
			k.hold = 0
		}
	}
	if k.has && k.reported == h && !k.pending {
		return false
	}
	if now.Before(k.until) {
		k.pending, k.ph, k.val = true, h, v
		return false
	}
	if k.has && k.reported != h {
		k.flips = append(k.flips, now)
		for len(k.flips) > 0 && now.Sub(k.flips[0]) > d.cfg.Window {
			k.flips = k.flips[1:]
		}
		if len(k.flips) > d.cfg.Flips {
			k.hold = min(max(k.hold*2, d.cfg.Base), d.cfg.Max)
			k.until = now.Add(k.hold)
			k.pending, k.ph, k.val = true, h, v
			k.flips = k.flips[:0]
			if k.timer != nil {
				k.timer()
			}
			k.round++
			round := k.round
			k.timer = d.clock.AfterFunc(k.hold, func() { d.release(key, k, round) })
			return false
		}
	}
	k.has, k.reported, k.pending = true, h, false
	var zero V
	k.val = zero
	if k.timer != nil {
		k.timer() // a suppression that ended before its timer ran
		k.timer = nil
	}
	return true
}

// release passes the latest value if the timer still owns the suppression.
func (d *Damper[K, V]) release(key K, k *dampKey[K, V], round uint64) {
	d.mu.Lock()
	if d.closed || d.keys[key] != k || k.round != round || !k.pending {
		d.mu.Unlock()
		return
	}
	v := k.val
	k.reported, k.pending = k.ph, false
	var zero V
	k.val = zero
	k.timer = nil
	d.enqueueLocked(key, v)
}

// enqueueLocked queues a value for emit and, unless another goroutine is
// delivering already, delivers the queue itself. It returns with d.mu
// released.
func (d *Damper[K, V]) enqueueLocked(key K, v V) {
	d.queue = append(d.queue, dampOut[K, V]{key, v})
	if d.draining {
		d.mu.Unlock()
		return
	}
	d.draining = true
	for len(d.queue) > 0 {
		o := d.queue[0]
		d.queue[0] = dampOut[K, V]{}
		d.queue = d.queue[1:]
		d.mu.Unlock()
		d.emit(o.key, o.val)
		d.mu.Lock()
	}
	d.queue = nil
	d.draining = false
	d.mu.Unlock()
}

// Forget drops everything held for key: its flap history, a value held
// back and the timer that would pass it. Once Forget returns, no call to
// emit for key starts; one already running may finish, as with
// time.Timer.Stop. The next Offer for key starts afresh.
func (d *Damper[K, V]) Forget(key K) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if k := d.keys[key]; k != nil && k.timer != nil {
		k.timer()
	}
	delete(d.keys, key)
	d.queue = slices.DeleteFunc(d.queue, func(o dampOut[K, V]) bool { return o.key == key })
}

// Close forgets every key and makes Offer do nothing.
func (d *Damper[K, V]) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, k := range d.keys {
		if k.timer != nil {
			k.timer()
		}
	}
	clear(d.keys)
	d.queue = nil
}

// Len returns the number of keys the damper holds state for.
func (d *Damper[K, V]) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.keys)
}
