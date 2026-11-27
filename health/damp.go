package health

import (
	"time"

	"github.com/djylb/upstream"
)

// DampConfig controls flap damping for a Checker's targets. The zero value
// leaves damping off.
type DampConfig struct {
	// Enabled turns on damping. Non-positive settings below take their defaults.
	Enabled bool

	// Window is the sliding period over which changes are counted.
	// The default is one minute.
	Window time.Duration

	// Flips is the number of changes allowed in Window before the next
	// change starts a hold. The default is 3.
	Flips int

	// Base is the length of the first hold. The default is 30 seconds.
	Base time.Duration

	// Max is the longest hold. The default is five minutes, and it is
	// always at least Base.
	Max time.Duration
}

func (c DampConfig) withDefaults() DampConfig {
	if !c.Enabled {
		return DampConfig{}
	}
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

// damping is the flap history of one entry. A scheduled release holds the
// latest observation, including the health judged by the rise/fall counters.
type damping struct {
	until  time.Time
	flips  []time.Time
	latest Result
	target Target
	hold   time.Duration
	slot   int // 1 + index in the release schedule, or 0 when not held
}

// holdChange records a change from the effective health and returns whether
// it starts a hold. It runs only when there is no release outstanding.
func (d *damping) holdChange(now time.Time, cfg DampConfig) bool {
	for d.hold > 0 && now.Sub(d.until) >= cfg.Window {
		d.hold /= 2
		d.until = d.until.Add(cfg.Window)
		if d.hold < cfg.Base {
			d.hold = 0
		}
	}

	expired := 0
	for expired < len(d.flips) && now.Sub(d.flips[expired]) > cfg.Window {
		expired++
	}
	if expired != 0 {
		d.flips = append(d.flips[:0], d.flips[expired:]...)
	}
	if len(d.flips) < cfg.Flips {
		d.flips = append(d.flips, now)
		return false
	}

	d.hold = max(d.hold+min(d.hold, cfg.Max-d.hold), cfg.Base)
	d.until = now.Add(d.hold)
	d.flips = d.flips[:0]
	return true
}

// observeLocked feeds the result through thresholds and then damping. The
// effective health stays at the last published verdict during a hold.
func (c *Checker[K, G]) observeLocked(e *entry[K, G], t Target, r Result, o Options) {
	d := e.damp
	if d != nil && d.slot != 0 && !r.FinishedAt.Before(d.until) {
		// A probe can finish before the timer goroutine processes a due
		// release. Publish the held observation before judging this one.
		c.releaseDampingLocked(e)
	}
	if r.Err == nil {
		e.lat.add(r.Latency, c.window)
	}
	judged := e.health
	if d != nil && d.slot != 0 {
		judged = d.latest.Health
	}
	r.Health = e.observe(judged, r.Err == nil, o.Rise, o.Fall)
	if d != nil && d.slot != 0 {
		d.latest, d.target = r, t
		return
	}
	cfg := c.cfg.Damping
	if cfg.Enabled && e.health != upstream.HealthUnknown && r.Health != e.health {
		if d == nil {
			d = new(damping)
			e.damp = d
		}
		if d.holdChange(r.FinishedAt, cfg) {
			d.latest, d.target = r, t
			c.releases.add(d.until, e, &d.slot)
			return
		}
	}
	c.publishLocked(e, t, r, false)
}

func (c *Checker[K, G]) publishLocked(e *entry[K, G], t Target, r Result, released bool) {
	r.Previous = e.health
	e.health, e.last = r.Health, r
	c.enqueueLocked(&delivery[K, G]{e: e, rep: Report[K]{
		Result: r, Stats: e.lat.stats(), Target: t, Key: e.key, Generation: e.generation, DampingReleased: released,
	}})
}

func (c *Checker[K, G]) releaseDampingLocked(e *entry[K, G]) {
	d := e.damp
	c.releases.remove(d.slot)
	if c.checkableLocked(e) {
		c.publishLocked(e, d.target, d.latest, true)
	}
	d.latest, d.target = Result{}, Target{}
}

func (c *Checker[K, G]) clearDampingLocked(e *entry[K, G]) {
	if e.damp != nil {
		c.releases.remove(e.damp.slot)
		e.damp = nil
	}
}
