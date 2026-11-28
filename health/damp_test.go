package health

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream"
)

// checkDamping records the first verdict and enough transitions to start a hold.
func checkDamping(t *testing.T, h *harness, key string, firstFails bool, n int) {
	t.Helper()
	runDue(h.clk)
	for i := 1; i <= n; i++ {
		forceDampingCheck(t, h, key, firstFails != (i%2 != 0))
	}
}

func forceDampingCheck(t *testing.T, h *harness, key string, fail bool) {
	t.Helper()
	h.sw.set(key, fail)
	if !h.c.CheckNow(key) {
		t.Fatalf("target %q is not checkable", key)
	}
	runDue(h.clk)
}

func dampingOptions() Options {
	return Options{Interval: time.Hour, Rise: 1, Fall: 1}
}

func TestDampingPublishesLatestObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var changes []Report[string]
		h := newHarness(t, Config[string]{
			Options: dampingOptions(),
			Damping: DampConfig{Enabled: true},
			OnChange: func(r Report[string]) {
				changes = append(changes, r)
			},
		})
		generation := h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		forceDampingCheck(t, h, "a", false) // duplicate health
		checkDamping(t, h, "a", false, 6)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down, up, down}) {
			t.Fatalf("transitions during hold: %v", got)
		}
		if len(changes) != 4 || h.rep.len() != 5 {
			t.Fatalf("reports during hold: OnChange=%d OnResult=%d", len(changes), h.rep.len())
		}
		if timers := h.clk.Pending(); timers != 1 {
			t.Fatalf("hold and next check use %d timers, want 1", timers)
		}
		h.clk.Advance(29 * time.Second)
		runDue(h.clk)
		latest := h.clk.Now()
		forceDampingCheck(t, h, "a", true) // latest verdict equals the last published one
		if s, _ := h.c.Status("a"); s.Health != down || s.Checks != 9 || !s.Last.Time.Before(latest) {
			t.Fatalf("status during hold: %+v", s)
		}
		if len(changes) != 4 || h.rep.len() != 5 {
			t.Fatal("released before the deadline")
		}
		h.tick(1)
		if len(changes) != 5 || h.rep.len() != 6 {
			t.Fatalf("release: OnChange=%d OnResult=%d", len(changes), h.rep.len())
		}
		r := changes[4]
		if !r.DampingReleased || r.Changed() || r.Health != down || r.Generation != generation ||
			!r.Time.Equal(latest) || !r.FinishedAt.Equal(latest) || r.Reason != ReasonOther || !errors.Is(r.Err, errDown) {
			t.Fatalf("release did not publish the latest observation: %+v", r)
		}
		if s, _ := h.c.Status("a"); s.Health != down || !s.Last.Time.Equal(latest) {
			t.Fatalf("status after release: %+v", s)
		}
	})
}

func TestDampingSlidingWindow(t *testing.T) {
	cfg := (DampConfig{Enabled: true}).withDefaults()
	now := time.Time{}
	var d damping
	for _, elapsed := range []time.Duration{0, time.Second, 2 * time.Second, cfg.Window + time.Second} {
		if d.holdChange(now.Add(elapsed), cfg) {
			t.Fatalf("early hold after %v", elapsed)
		}
	}
	// The oldest change fell out of the window; a change exactly one
	// window ago still counts.
	if !d.holdChange(now.Add(cfg.Window+time.Second), cfg) {
		t.Fatal("four changes inside the sliding window did not start a hold")
	}
}

func TestDampingHoldDoublesCapsAndDecays(t *testing.T) {
	cfg := (DampConfig{Enabled: true, Flips: 1}).withDefaults()
	now := time.Time{}
	var d damping
	hold := func(want time.Duration) {
		t.Helper()
		if d.holdChange(now, cfg) || !d.holdChange(now, cfg) || d.until.Sub(now) != want {
			t.Fatalf("hold = %v, want %v", d.until.Sub(now), want)
		}
		now = d.until
	}
	for _, want := range []int{30, 60, 120, 240, 300, 300} {
		hold(time.Duration(want) * time.Second)
	}
	now = now.Add(2 * cfg.Window)
	hold(150 * time.Second)
	now = now.Add(10 * cfg.Window)
	hold(cfg.Base)
}

// dequeueDampingRelease holds the scheduler callback after its timer has
// stopped being pending, so the test can run checker operations before it.
func dequeueDampingRelease(t *testing.T, h *harness) func() {
	t.Helper()
	c := h.c
	c.mu.Lock()
	timer, due, seq := c.armTimer, c.armAt, c.armSeq
	c.mu.Unlock()
	if due.Sub(h.clk.Now()) != 30*time.Second || timer == nil || !timer.Stop() {
		t.Fatal("no timer for the damping release")
	}
	h.clk.AdvanceTo(due)
	return func() { c.fire(seq) }
}

func TestDampingRemovalDiscardsDequeuedRelease(t *testing.T) {
	for name, remove := range map[string]func(*harness){
		"remove":        func(h *harness) { h.c.Remove("a") },
		"remove member": func(h *harness) { h.c.RemoveFromGroup("g", "a") },
		"remove group":  func(h *harness) { h.c.RemoveGroup("g") },
		"sync group":    func(h *harness) { h.c.SyncGroup("g", nil) },
		"new address":   func(h *harness) { h.c.AddToGroup("g", "a", h.sw.target("replacement")) },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var removed []uint64
				h := newHarness(t, Config[string]{
					Options:  dampingOptions(),
					Damping:  DampConfig{Enabled: true, Flips: 1},
					OnRemove: func(_ string, gen uint64) { removed = append(removed, gen) },
				})
				old := h.c.AddToGroup("g", "a", h.sw.target("a"))
				checkDamping(t, h, "a", false, 2)
				release := dequeueDampingRelease(t, h)
				remove(h)
				next := h.c.AddToGroup("g", "a", h.sw.target("replacement"))
				runDue(h.clk)
				release()
				runDue(h.clk)
				if !slices.Equal(removed, []uint64{old}) || next == old {
					t.Fatalf("removed %v, replaced generation %d with %d", removed, old, next)
				}
				s, _ := h.c.Status("a")
				if s.Health != up || s.Generation != next || s.Checks != 1 {
					t.Fatalf("replacement status: %+v", s)
				}
				for _, r := range h.rep.list {
					if r.DampingReleased {
						t.Fatalf("stale release after removal: %+v", r)
					}
				}
				h.c.Close()
				<-h.c.Done()
				if h.clk.Pending() != 0 {
					t.Fatal("closed checker kept a timer")
				}
			})
		})
	}
}

func TestDampingReleaseWhileProbeSlotsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: dampingOptions(), Damping: DampConfig{Enabled: true, Flips: 1}, Concurrency: 1})
		h.c.Put("a", h.sw.target("a"))
		checkDamping(t, h, "a", false, 2)
		h.c.Put("slow", Target{
			Options: Options{Timeout: time.Minute},
			Probe: func(ctx context.Context, _ Target) (Observation, error) {
				<-ctx.Done()
				return Observation{}, ctx.Err()
			},
		})
		h.c.Put("waiting", h.sw.target("waiting"))
		runDue(h.clk)
		h.clk.Advance(30 * time.Second)
		runDue(h.clk)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down, up}) || h.sw.count("waiting") != 0 {
			t.Fatalf("target did not release while a probe owned the only slot: %v", got)
		}
	})
}

func TestDampingReconfiguration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := Config[string]{Options: dampingOptions(), Damping: DampConfig{Enabled: true, Flips: 1}}
		h := newHarness(t, cfg)
		cfg.OnResult = h.rep.add
		h.c.Put("a", h.sw.target("a"))
		checkDamping(t, h, "a", false, 2)

		h.c.SetConfig(cfg)
		runDue(h.clk)
		if h.rep.len() != 2 {
			t.Fatal("an unchanged policy ended the hold")
		}

		cfg.Damping.Base = time.Minute
		h.c.SetConfig(cfg)
		runDue(h.clk)
		if h.rep.len() != 3 || !h.rep.list[2].DampingReleased || h.rep.list[2].Health != up {
			t.Fatalf("policy change did not release the held observation: %+v", h.rep.list)
		}

		checkDamping(t, h, "a", false, 2)
		cfg.Disabled = true
		h.c.SetConfig(cfg)
		runDue(h.clk)
		if s, _ := h.c.Status("a"); s.Health != unknown || s.Checking || h.clk.Pending() != 0 {
			t.Fatalf("disabled target: status %+v, timers %d", s, h.clk.Pending())
		}
		if h.rep.len() != 5 || h.rep.list[4].DampingReleased {
			t.Fatal("disabling published a held result or dropped the reset")
		}

		cfg.Disabled = false
		h.c.SetConfig(cfg)
		forceDampingCheck(t, h, "a", false)
		if s, _ := h.c.Status("a"); s.Health != up {
			t.Fatalf("target did not start afresh: %+v", s)
		}
	})
}

func TestDampingRespectsRiseAndFall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := dampingOptions()
		o.Rise, o.Fall = 2, 2
		h := newHarness(t, Config[string]{Options: o, Damping: DampConfig{Enabled: true, Flips: 1}})
		h.c.Put("a", h.sw.target("a"))
		runDue(h.clk)
		for _, fail := range []bool{true, false, true, true, false, false, true} {
			forceDampingCheck(t, h, "a", fail)
		}
		// The held health reached Up after two passes; one subsequent
		// failure cannot change it before release.
		h.clk.Advance(30 * time.Second)
		runDue(h.clk)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down, up}) {
			t.Fatalf("transitions %v", got)
		}
	})
}

func TestDampingAttachReplaysEffectiveHealth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: dampingOptions(), Damping: DampConfig{Enabled: true, Flips: 1}})
		target := func(b *upstream.Backend[string]) Target { return h.sw.target(b.Value()) }
		p := newPool(t, upstream.Target[string]{ID: "a", Value: "a"})
		mustAttach(t, h.c, "first", p, target)
		checkDamping(t, h, "a", false, 2)
		late := newPool(t, upstream.Target[string]{ID: "other", Value: "a"})
		mustAttach(t, h.c, "late", late, target)
		runDue(h.clk)
		if a, b := healthOf(t, p, "a"), healthOf(t, late, "other"); a != down || b != down {
			t.Fatalf("verdicts during hold: first %v, late %v", a, b)
		}
		h.clk.Advance(30 * time.Second)
		runDue(h.clk)
		if a, b := healthOf(t, p, "a"), healthOf(t, late, "other"); a != up || b != up {
			t.Fatalf("released verdicts: first %v, late %v", a, b)
		}
		h.c.Close()
		<-h.c.Done()
		if a, b := healthOf(t, p, "a"), healthOf(t, late, "other"); a != unknown || b != unknown {
			t.Fatalf("checker left its verdicts after close: first %v, late %v", a, b)
		}
	})
}

func TestDampingHoldEndsBeforeLaterCheckIsPublished(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: dampingOptions(), Damping: DampConfig{Enabled: true, Flips: 1}})
		base := h.sw.target("a")
		blocked := make(chan struct{})
		probe := base.Probe
		base.Options.Timeout = time.Minute
		base.Probe = func(ctx context.Context, target Target) (Observation, error) {
			if h.sw.count("a") == 3 {
				select {
				case <-blocked:
				case <-ctx.Done():
					return Observation{}, ctx.Err()
				}
			}
			return probe(ctx, target)
		}
		h.c.Put("a", base)
		checkDamping(t, h, "a", false, 2)
		started := h.clk.Now()
		h.sw.set("a", true)
		if !h.c.CheckNow("a") {
			t.Fatal("could not start a probe during the hold")
		}
		runDue(h.clk)
		fire := dequeueDampingRelease(t, h)
		finished := h.clk.Now()
		close(blocked)
		runDue(h.clk)
		fire()
		runDue(h.clk)
		if got := h.rep.changes(); !slices.Equal(got, []upstream.Health{up, down, up, down}) {
			t.Fatalf("transitions %v", got)
		}
		release, later := h.rep.list[2], h.rep.list[3]
		if !release.DampingReleased || later.DampingReleased || !release.FinishedAt.Equal(started) ||
			!later.Time.Equal(started) || !later.FinishedAt.Equal(finished) {
			t.Fatalf("release %+v, later check %+v", release, later)
		}
	})
}

func TestDampingReleasesInDeadlineOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var released []string
		h := newHarness(t, Config[string]{
			Options: dampingOptions(),
			Damping: DampConfig{Enabled: true, Flips: 1, Base: 10 * time.Second},
			OnChange: func(r Report[string]) {
				if r.DampingReleased {
					released = append(released, r.Key)
				}
			},
		})
		for _, key := range []string{"first", "removed", "last"} {
			h.c.Put(key, h.sw.target(key))
			checkDamping(t, h, key, false, 2)
			h.tick(1)
		}
		h.c.Remove("removed")
		if timers := h.clk.Pending(); timers != 1 {
			t.Fatalf("%d scheduling timers for multiple holds, want 1", timers)
		}
		h.tick(8)
		if !slices.Equal(released, []string{"first"}) {
			t.Fatalf("releases after the middle deadline: %v", released)
		}
		h.tick(1)
		if !slices.Equal(released, []string{"first", "last"}) {
			t.Fatalf("releases after the last deadline: %v", released)
		}
		h.c.Close()
		<-h.c.Done()
		if h.clk.Pending() != 0 {
			t.Fatal("checker left a timer after close")
		}
	})
}
