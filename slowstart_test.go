package upstream

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

func shareOf(t *testing.T, p *Pool[string], id string, n int) float64 {
	t.Helper()
	return float64(pickIDs(t, p, n)[id]) / float64(n)
}

func TestSlowStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, SlowStart: 16 * time.Second, Outlier: OutlierConfig{ConsecutiveFailures: 1, BaseEjection: time.Second}})
		if err := p.Sync(targets("a")); err != nil {
			t.Fatal(err)
		}
		advance := func(d time.Duration) {
			for range int(d / time.Second) {
				clk.Advance(time.Second)
				synctest.Wait()
			}
		}
		advance(20 * time.Second)
		if err := p.Put(Target[string]{ID: "b"}); err != nil {
			t.Fatal(err)
		}
		if s := shareOf(t, p, "b", 1700); s > 0.1 {
			t.Fatalf("new backend took %.2f of picks at once", s)
		}
		advance(8 * time.Second)
		if s := shareOf(t, p, "b", 1700); s < 0.25 || s > 0.4 {
			t.Fatalf("half-way share %.2f, want about 1/3", s)
		}
		advance(9 * time.Second)
		if s := shareOf(t, p, "b", 1700); s < 0.49 || s > 0.51 {
			t.Fatalf("share after the ramp %.2f, want 1/2", s)
		}
		if clk.Pending() != 0 {
			t.Fatal("ramp timer still armed after the ramp")
		}

		// A backend recovering from an ejection ramps up again.
		ejectToHalfOpen(t, p, clk, "b")
		var trial Lease[string]
		for trial.Backend() == nil {
			l := mustPick(t, p)
			if l.Backend().ID() == "b" {
				trial = l
			} else {
				l.Release(Success)
			}
		}
		trial.Release(Success)
		if s := shareOf(t, p, "b", 1700); s > 0.1 {
			t.Fatalf("recovered backend took %.2f of picks at once", s)
		}
		p.Close()
		if clk.Pending() != 0 {
			t.Fatal("Close left the ramp timer")
		}
	})
}

func TestSlowStartToleratesLaterStart(t *testing.T) {
	clk := fakeclock.New()
	p := New[string](Config{Clock: clk, SlowStart: time.Minute})
	defer p.Close()
	if err := p.Sync(targets("a", "b")); err != nil {
		t.Fatal(err)
	}
	b, _ := p.Backend("b")
	// Another rebuild read the clock later than this one will.
	b.core.rampStart.Store(clk.Now().Add(time.Second).UnixNano())
	p.refresh()
	if b.core.rampStart.Load() == 0 {
		t.Fatal("a ramp that started a little later than this rebuild's clock reading ended")
	}
}
