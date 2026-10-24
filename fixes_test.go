package upstream

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

// ejectToHalfOpen ejects backend id with failures and waits for its
// ejection to end.
func ejectToHalfOpen(t *testing.T, p *Pool[string], clk *fakeclock.Clock, id string) {
	t.Helper()
	b, _ := p.Backend(id)
	for b.Status().Outlier.State == OutlierOK {
		l := mustPick(t, p)
		o := Success
		if l.Backend().ID() == id {
			o = Failure
		}
		l.Release(o)
	}
	clk.Advance(time.Hour)
	synctest.Wait()
	if b.State() != StateHalfOpen {
		t.Fatalf("%s is %v", id, b.State())
	}
}

func TestPickFallsBackWhenBalancerInsists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 1}})
		defer p.Close()
		if err := p.Update([]Target[string]{{ID: "heavy", Weight: 50}, {ID: "light"}}); err != nil {
			t.Fatal(err)
		}
		ejectToHalfOpen(t, p, clk, "heavy")
		var trial Lease[string]
		for trial.Backend() == nil {
			l := mustPick(t, p)
			if l.Backend().ID() == "heavy" {
				trial = l
			} else {
				l.Release(Success)
			}
		}
		// The balancer keeps choosing heavy, which has no trial slot.
		for range 200 {
			l, err := p.Pick()
			if err != nil {
				t.Fatalf("Pick with a healthy backend available: %v", err)
			}
			if l.Backend().ID() != "light" {
				t.Fatal("picked heavy past its trial limit")
			}
			l.Release(Success)
		}
		trial.Release(Neutral)
	})
}

func TestTrialsBelongToTheirPeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		d := NewOutlierDetector(OutlierConfig{Clock: clk, ConsecutiveFailures: 1, BaseEjection: time.Second, HalfOpenRequests: 2, HalfOpenSuccesses: 2}, nil)
		defer d.Close()
		fail(t, d, 1)
		clk.Advance(time.Second)
		synctest.Wait()
		stale, _ := d.Admit()
		bad, _ := d.Admit()
		d.Record(bad, Failure) // ejected again; stale is still in flight
		clk.Advance(time.Hour)
		synctest.Wait()
		if d.State() != OutlierHalfOpen {
			t.Fatal("not half-open")
		}
		// The stale trial neither holds a slot nor counts as a success.
		t1, ok1 := d.Admit()
		t2, ok2 := d.Admit()
		if !ok1 || !ok2 {
			t.Fatal("slots of the new period taken by an old trial")
		}
		d.Record(stale, Success)
		d.Record(t1, Success)
		if d.State() != OutlierHalfOpen {
			t.Fatal("recovered with one successful trial of this period")
		}
		d.Record(t2, Success)
		if d.State() != OutlierOK {
			t.Fatalf("state %v after two successful trials", d.State())
		}
	})
}

func TestSubscriberPanicLeaksNothing(t *testing.T) {
	p := New[string](Config{Outlier: OutlierConfig{ConsecutiveFailures: 1}})
	defer p.Close()
	if err := p.Update(targets("a", "b")); err != nil {
		t.Fatal(err)
	}
	var log eventLog
	p.Subscribe(log.add)
	panicked := false
	p.Subscribe(func(ev Event[string]) {
		if ev.Kind == EventStateChanged && !panicked {
			panicked = true
			panic("subscriber")
		}
	})
	log.take()
	l := mustPick(t, p)
	b := l.Backend()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		l.Release(Failure) // ejects, which runs the subscribers
	}()
	if b.InFlight() != 0 {
		t.Fatalf("in flight %d after a panicking release", b.InFlight())
	}
	// The event the second subscriber panicked on still reached the
	// first one; the next flush delivers what was left.
	p.Drain("zz")
	_ = p.Put(Target[string]{ID: "c"})
	got := log.take()
	if len(got) < 2 || got[0] != "state-changed "+b.ID()+" healthy->ejected" || got[len(got)-1] != "added c" {
		t.Fatalf("events after the panic: %v", got)
	}
}

func TestDrainResumeRace(t *testing.T) {
	p := newTestPool(t, Config{}, "a", "b")
	var wg sync.WaitGroup
	chans := make(chan *Drain, 4000)
	for range 4 {
		wg.Go(func() {
			for range 500 {
				ch, _ := p.Drain("a")
				chans <- ch
				p.Resume("a")
				if l, err := p.Pick(); err == nil {
					l.Release(Success)
				}
			}
		})
	}
	wg.Wait()
	close(chans)
	timeout := time.After(10 * time.Second)
	for ch := range chans {
		select {
		case <-ch.Done():
		case <-timeout:
			t.Fatal("a drain channel never closed")
		}
	}
}

func TestActiveHealthUnknown(t *testing.T) {
	p := newTestPool(t, Config{}, "a")
	a, _ := p.Backend("a")
	p.ReportCheck(a, HealthDown, CheckResult{Err: errors.New("x")})
	if _, err := p.Pick(); !errors.Is(err, ErrUnavailable) {
		t.Fatal("down backend picked")
	}
	p.ReportCheck(a, HealthUnknown, CheckResult{})
	if a.Status().Health != HealthUnknown || a.State() != StateHealthy {
		t.Fatalf("status %+v", a.Status())
	}
	l := mustPick(t, p)
	l.Release(Success)
	p.ReportCheck(a, HealthUp, CheckResult{})
	if a.Status().Health != HealthUp {
		t.Fatal("not up")
	}
	for h := HealthUnknown; h <= HealthDown+1; h++ {
		if h.String() == "" {
			t.Fatal("unnamed health")
		}
	}
}

func TestWindowNeverMovesBack(t *testing.T) {
	var w window
	size := 10 * time.Second
	w.add(65*time.Second, size, true)
	w.add(55*time.Second, size, true) // a late writer with an older time
	total, failed := w.add(66*time.Second, size, false)
	if total < 3 || failed < 2 {
		t.Fatalf("window lost counts: %v requests, %v failures", total, failed)
	}
}

func shareOf(t *testing.T, p *Pool[string], id string, n int) float64 {
	t.Helper()
	return float64(pickIDs(t, p, n)[id]) / float64(n)
}

func TestSlowStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, SlowStart: 16 * time.Second, Outlier: OutlierConfig{ConsecutiveFailures: 1, BaseEjection: time.Second}})
		if err := p.Update(targets("a")); err != nil {
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
	if err := p.Update(targets("a", "b")); err != nil {
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
