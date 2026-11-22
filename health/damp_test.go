package health

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/djylb/upstream"
)

// stepClock is a Clock whose timers run synchronously, in time order, from
// Advance, so that decisions that depend on the order of timers and calls
// can be compared exactly.
type stepClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*stepTimer
	seq    int
}

type stepTimer struct {
	at  time.Time
	seq int
	f   func()
}

func newStepClock() *stepClock { return &stepClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &stepTimer{c.now.Add(d), c.seq, f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		i := slices.Index(c.timers, t)
		if i < 0 {
			return false
		}
		c.timers = slices.Delete(c.timers, i, i+1)
		return true
	}
}

func (c *stepClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// Advance moves the clock by d, running each timer due on the way at its
// own time.
func (c *stepClock) Advance(d time.Duration) {
	end := c.Now().Add(d)
	for f := c.takeDue(end); f != nil; f = c.takeDue(end) {
		f()
	}
}

// takeDue dequeues the next timer due by end and returns its callback
// without running it. The clock moves to the timer's time, or to end if
// no timer is due.
func (c *stepClock) takeDue(end time.Time) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var next *stepTimer
	for _, t := range c.timers {
		if !t.at.After(end) && (next == nil || t.at.Before(next.at) || t.at.Equal(next.at) && t.seq < next.seq) {
			next = t
		}
	}
	if next == nil {
		c.now = end
		return nil
	}
	c.timers = slices.DeleteFunc(c.timers, func(t *stepTimer) bool { return t == next })
	if next.at.After(c.now) {
		c.now = next.at
	}
	return next.f
}

type damped struct {
	key string
	h   upstream.Health
}

func newTestDamper(c upstream.Clock, cfg DampConfig, got *[]damped) *Damper[string, upstream.Health] {
	return NewDamper(c, cfg, func(k string, h upstream.Health) { *got = append(*got, damped{k, h}) })
}

func TestDamperFlips(t *testing.T) {
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{}, &got)
	defer d.Close()
	d.Offer("a", up, up)
	d.Offer("a", up, up) // no change
	for range 3 {
		d.Offer("a", down, down)
		d.Offer("a", up, up)
	}
	// The first value, then three changes pass; the fourth suppresses.
	if len(got) != 4 {
		t.Fatalf("reports %v", got)
	}
	d.Offer("a", down, down)
	if len(got) != 4 {
		t.Fatalf("passed while suppressed: %v", got)
	}
	clk.Advance(30*time.Second - 1)
	if len(got) != 4 {
		t.Fatalf("released early: %v", got)
	}
	clk.Advance(1)
	if len(got) != 5 || got[4].h != down {
		t.Fatalf("release: %v", got)
	}
}

func TestDamperReleaseRepeatsReported(t *testing.T) {
	// The latest value of a suppression is passed even when it equals the
	// last one passed.
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{Flips: 1}, &got)
	defer d.Close()
	d.Offer("a", up, up)
	d.Offer("a", down, down)
	d.Offer("a", up, up) // second change within the window: suppressed
	d.Offer("a", down, down)
	d.Offer("a", up, up)
	if len(got) != 2 {
		t.Fatalf("reports %v", got)
	}
	clk.Advance(30 * time.Second)
	if len(got) != 3 || got[2].h != up {
		t.Fatalf("release: %v", got)
	}
}

func TestDamperDelayedRelease(t *testing.T) {
	const base = 10 * time.Second
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{Flips: 1, Base: base}, &got)
	defer d.Close()

	d.Offer("a", up, up)
	d.Offer("a", down, down)
	d.Offer("a", up, up) // starts the first suppression
	release := clk.takeDue(clk.Now().Add(base))
	if release == nil {
		t.Fatal("first suppression did not schedule a release")
	}

	d.Offer("a", up, up)     // passes after the first suppression expires
	d.Offer("a", down, down) // starts the second suppression
	want := []damped{{"a", up}, {"a", down}, {"a", up}}
	if !slices.Equal(got, want) {
		t.Fatalf("before delayed release: got %v, want %v", got, want)
	}
	release()
	if !slices.Equal(got, want) {
		t.Fatalf("old timer released the second suppression: got %v, want %v", got, want)
	}

	clk.Advance(2*base - time.Second)
	if !slices.Equal(got, want) {
		t.Fatalf("before second deadline: got %v, want %v", got, want)
	}
	clk.Advance(time.Second)
	want = append(want, damped{"a", down})
	if !slices.Equal(got, want) {
		t.Fatalf("at second deadline: got %v, want %v", got, want)
	}
}

func TestDamperHoldDoublesAndDecays(t *testing.T) {
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{Flips: 1, Window: time.Minute, Base: 10 * time.Second, Max: 40 * time.Second}, &got)
	defer d.Close()
	flap := func() { // two changes: the second suppresses
		d.Offer("a", upstream.HealthDown, upstream.HealthDown)
		d.Offer("a", upstream.HealthUp, upstream.HealthUp)
		d.Offer("a", upstream.HealthDown, upstream.HealthDown)
	}
	d.Offer("a", upstream.HealthUp, upstream.HealthUp)
	hold := func() time.Duration {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.keys["a"].hold
	}
	flap()
	if hold() != 10*time.Second {
		t.Fatalf("hold %v", hold())
	}
	clk.Advance(10 * time.Second) // released: reported down
	d.Offer("a", upstream.HealthUp, upstream.HealthUp)
	d.Offer("a", upstream.HealthDown, upstream.HealthDown)
	if hold() != 20*time.Second {
		t.Fatalf("hold %v", hold())
	}
	clk.Advance(20 * time.Second)
	d.Offer("a", upstream.HealthUp, upstream.HealthUp)
	d.Offer("a", upstream.HealthDown, upstream.HealthDown)
	d.Offer("a", upstream.HealthUp, upstream.HealthUp)
	if hold() != 40*time.Second {
		t.Fatalf("hold %v", hold())
	}
	clk.Advance(40 * time.Second)
	clk.Advance(3 * time.Minute)
	d.Offer("a", upstream.HealthDown, upstream.HealthDown)
	if hold() != 0 {
		t.Fatalf("hold %v after quiet windows", hold())
	}
}

func TestDamperForgetAndClose(t *testing.T) {
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{Flips: 1}, &got)
	for _, h := range []upstream.Health{upstream.HealthUp, upstream.HealthDown, upstream.HealthUp, upstream.HealthDown} {
		d.Offer("a", h, h)
		d.Offer("b", h, h)
	}
	n := len(got)
	d.Forget("a")
	if d.Len() != 1 {
		t.Fatalf("keys %d", d.Len())
	}
	clk.Advance(time.Hour)
	if len(got) != n+1 || got[n].key != "b" {
		t.Fatalf("forgotten key released: %v", got[n:])
	}
	d.Offer("a", upstream.HealthDown, upstream.HealthDown) // afresh: passes
	if len(got) != n+2 {
		t.Fatalf("afresh: %v", got[n:])
	}
	d.Close()
	d.Offer("c", upstream.HealthUp, upstream.HealthUp)
	if len(got) != n+2 || d.Len() != 0 {
		t.Fatalf("after Close: %v", got[n:])
	}
	if len(clk.timers) != 0 {
		t.Fatalf("%d timers left", len(clk.timers))
	}
}

func TestDamperDelayedReleaseAfterForgetAndClose(t *testing.T) {
	const base = 10 * time.Second
	clk := newStepClock()
	var got []damped
	d := newTestDamper(clk, DampConfig{Flips: 1, Base: base}, &got)
	defer d.Close()

	takeRelease := func() func() {
		t.Helper()
		d.Offer("a", up, up)
		d.Offer("a", down, down)
		d.Offer("a", up, up)
		release := clk.takeDue(clk.Now().Add(base))
		if release == nil {
			t.Fatal("suppression did not schedule a release")
		}
		return release
	}

	forgotten := takeRelease()
	d.Forget("a")
	current := takeRelease()
	want := []damped{{"a", up}, {"a", down}, {"a", up}, {"a", down}}
	forgotten()
	if !slices.Equal(got, want) {
		t.Fatalf("delayed release after Forget: got %v, want %v", got, want)
	}

	d.Close()
	current()
	if !slices.Equal(got, want) {
		t.Fatalf("delayed release after Close: got %v, want %v", got, want)
	}
}

func TestDamperEmitSerialAndReentrant(t *testing.T) {
	var (
		mu      sync.Mutex
		inside  int
		maxIn   int
		perKey  = map[int][]int{}
		damper  *Damper[int, int]
		started sync.WaitGroup
	)
	damper = NewDamper(nil, DampConfig{}, func(k, v int) {
		mu.Lock()
		inside++
		maxIn = max(maxIn, inside)
		perKey[k] = append(perKey[k], v)
		mu.Unlock()
		if k < 100 {
			damper.Offer(k+100, upstream.HealthUp, 0) // from inside emit: queued, no deadlock
		}
		mu.Lock()
		inside--
		mu.Unlock()
	})
	defer damper.Close()
	for g := range 8 {
		started.Go(func() { damper.Offer(g, upstream.HealthUp, 0) })
	}
	started.Wait()
	// Every goroutine's value is passed, once all deliveries have drained.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(perKey)
		mu.Unlock()
		if n == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keys %d", n)
		}
		time.Sleep(time.Millisecond)
	}
	if maxIn != 1 {
		t.Fatalf("%d emits at once", maxIn)
	}
}

// refDamper is the rule a Damper implements, written out on its own as the
// reference for the randomized comparison: the one that already ran in a
// client's health reports before it moved here.
type refDamper struct {
	clk     *stepClock
	window  time.Duration
	flips   int
	base    time.Duration
	maxHold time.Duration
	st      map[string]*refState
	emitted *[]damped
}

type refState struct {
	flips    []time.Time
	until    time.Time
	hold     time.Duration
	pending  *upstream.Health
	reported upstream.Health // 0: none
	timer    func() bool
}

// refStates are the health values the reference tells apart; 0 is "none".
func (r *refDamper) offer(key string, h upstream.Health) {
	d := r.st[key]
	if d == nil {
		d = &refState{}
		r.st[key] = d
	}
	// Health values are 0..2 here; shift by one so that 0 means none.
	s := h + 1
	now := r.clk.Now()
	for d.hold > 0 && now.Sub(d.until) >= r.window {
		d.hold /= 2
		d.until = d.until.Add(r.window)
		if d.hold < r.base {
			d.hold = 0
		}
	}
	if d.reported == s && d.pending == nil {
		return
	}
	if now.Before(d.until) {
		p := s
		d.pending = &p
		return
	}
	if d.reported != 0 && d.reported != s {
		d.flips = append(d.flips, now)
		for len(d.flips) > 0 && now.Sub(d.flips[0]) > r.window {
			d.flips = d.flips[1:]
		}
		if len(d.flips) > r.flips {
			d.hold = min(max(d.hold*2, r.base), r.maxHold)
			d.until = now.Add(d.hold)
			p := s
			d.pending = &p
			d.flips = d.flips[:0]
			d.timer = r.clk.AfterFunc(d.hold, func() { r.release(key) })
			return
		}
	}
	d.reported, d.pending = s, nil
	*r.emitted = append(*r.emitted, damped{key, h})
}

func (r *refDamper) release(key string) {
	d := r.st[key]
	if d == nil || d.pending == nil {
		return
	}
	s := *d.pending
	d.pending, d.reported = nil, s
	*r.emitted = append(*r.emitted, damped{key, s - 1})
}

func (r *refDamper) forget(key string) {
	if d := r.st[key]; d != nil && d.timer != nil {
		d.timer()
	}
	delete(r.st, key)
}

// TestDamperMatchesReference drives a Damper and the reference with the
// same random sequences of offers, forgets and clock steps and compares
// every value they pass.
func TestDamperMatchesReference(t *testing.T) {
	cfg := DampConfig{Window: time.Minute, Flips: 3, Base: 30 * time.Second, Max: 5 * time.Minute}
	steps := []time.Duration{0, 0, time.Second, 5 * time.Second, 20 * time.Second, 30 * time.Second, time.Minute, 3 * time.Minute}
	for seed := range 300 {
		rng := rand.New(rand.NewSource(int64(seed)))
		cl1, cl2 := newStepClock(), newStepClock()
		var got, want []damped
		d := newTestDamper(cl1, cfg, &got)
		ref := &refDamper{clk: cl2, window: cfg.Window, flips: cfg.Flips, base: cfg.Base, maxHold: cfg.Max, st: map[string]*refState{}, emitted: &want}
		keys := []string{"a", "b", "c"}
		for i := range 400 {
			k := keys[rng.Intn(len(keys))]
			switch rng.Intn(10) {
			case 0:
				d.Forget(k)
				ref.forget(k)
			case 1, 2:
				step := steps[rng.Intn(len(steps))]
				cl1.Advance(step)
				cl2.Advance(step)
			default:
				h := []upstream.Health{upstream.HealthUnknown, upstream.HealthUp, upstream.HealthDown}[rng.Intn(3)]
				d.Offer(k, h, h)
				ref.offer(k, h)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("seed %d step %d: passed %v, reference %v", seed, i, got, want)
			}
		}
		d.Close()
	}
}
