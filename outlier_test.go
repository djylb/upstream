package upstream

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

// fail admits and fails n requests.
func fail(t *testing.T, d *OutlierDetector, n int) {
	t.Helper()
	for range n {
		tk, ok := d.Admit()
		if !ok {
			t.Fatal("not admitted")
		}
		d.Record(tk, Failure)
	}
}

func TestOutlierConsecutiveFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		var changes atomic.Int32
		d := NewOutlierDetector(OutlierConfig{ConsecutiveFailures: 3}, clk, func() { changes.Add(1) })
		defer d.Close()

		// Requests admitted before the ejection finish after it.
		var late []Ticket
		for range 5 {
			tk, _ := d.Admit()
			late = append(late, tk)
		}
		fail(t, d, 2)
		ok, _ := d.Admit()
		d.Record(ok, Success) // resets the run
		fail(t, d, 2)
		d.Record(late[0], Neutral) // neutral neither resets nor counts
		if d.State() != OutlierOK {
			t.Fatalf("ejected after 2 consecutive failures")
		}
		fail(t, d, 1)
		if d.State() != OutlierEjected || changes.Load() != 1 {
			t.Fatalf("state %v after 3 failures, %d changes", d.State(), changes.Load())
		}
		for _, tk := range late[1:] {
			d.Record(tk, Failure)
		}
		if _, ok := d.Admit(); ok {
			t.Fatal("ejected backend admitted")
		}
		st := d.Status()
		if changes.Load() != 1 || st.Ejections != 1 || !errors.Is(st.Cause, ErrConsecutiveFailures) {
			t.Fatalf("re-reported: %d changes, status %+v", changes.Load(), st)
		}
		if want := clk.Now().Add(30 * time.Second); !st.EjectedUntil.Equal(want) {
			t.Fatalf("ejected until %v, want %v", st.EjectedUntil, want)
		}
	})
}

func TestOutlierBackoffAndHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		var changes atomic.Int32
		d := NewOutlierDetector(OutlierConfig{
			ConsecutiveFailures: 1,
			BaseEjection:        time.Second,
			MaxEjection:         5 * time.Second,
			HalfOpenRequests:    2,
			HalfOpenSuccesses:   2,
		}, clk, func() { changes.Add(1) })
		defer d.Close()

		advance := func(dur time.Duration) {
			clk.Advance(dur)
			synctest.Wait()
		}
		var waits []time.Duration
		fail(t, d, 1)
		for range 5 {
			start := clk.Now()
			for d.State() == OutlierEjected {
				advance(100 * time.Millisecond)
			}
			waits = append(waits, clk.Now().Sub(start))
			if d.State() != OutlierHalfOpen {
				t.Fatalf("state %v after ejection", d.State())
			}
			// Two trials at a time; the third is refused.
			t1, ok1 := d.Admit()
			t2, ok2 := d.Admit()
			if _, ok3 := d.Admit(); !ok1 || !ok2 || ok3 {
				t.Fatalf("half-open admits %v %v %v, want true true false", ok1, ok2, ok3)
			}
			d.Record(t1, Failure)
			if d.State() != OutlierEjected || !errors.Is(d.Status().Cause, ErrTrialFailed) {
				t.Fatalf("failed trial left state %v", d.State())
			}
			d.Record(t2, Failure) // from the previous half-open period: ignored
		}
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
		if !slices.Equal(waits, want) {
			t.Fatalf("ejection times %v, want %v", waits, want)
		}

		// Recovery needs two successful trials and resets the backoff.
		advance(5 * time.Second)
		t1, _ := d.Admit()
		t2, _ := d.Admit()
		d.Record(t1, Success)
		if d.State() != OutlierHalfOpen {
			t.Fatal("recovered after one of two trials")
		}
		d.Record(t2, Success)
		if d.State() != OutlierOK || d.Status().Cause != nil {
			t.Fatalf("state %v after two trials", d.State())
		}
		fail(t, d, 1)
		if until := d.Status().EjectedUntil; !until.Equal(clk.Now().Add(time.Second)) {
			t.Fatalf("backoff not reset: until %v", until)
		}
		before := changes.Load()
		d.Reset()
		if d.State() != OutlierOK || changes.Load() != before+1 {
			t.Fatal("Reset did not clear the ejection")
		}
		d.Reset() // nothing to clear: no change
		if changes.Load() != before+1 {
			t.Fatal("Reset of a healthy detector reported a change")
		}
		if clk.Pending() != 0 {
			t.Fatalf("%d timers left after Reset", clk.Pending())
		}
	})
}

func TestOutlierFailureRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		d := NewOutlierDetector(OutlierConfig{FailurePercent: 50, MinRequests: 10, Window: 10 * time.Second}, clk, nil)
		defer d.Close()
		record := func(o Outcome, n int) {
			for range n {
				tk, ok := d.Admit()
				if !ok {
					t.Fatal("not admitted")
				}
				d.Record(tk, o)
			}
		}
		// 4 failures out of 9 requests: below the volume threshold.
		record(Success, 5)
		record(Failure, 4)
		if d.State() != OutlierOK {
			t.Fatal("ejected below MinRequests")
		}
		// An old window fades out: after two windows the failures are gone.
		clk.Advance(25 * time.Second)
		record(Success, 6)
		record(Failure, 4)
		if d.State() != OutlierOK {
			t.Fatal("ejected at 40% failures")
		}
		record(Failure, 1) // 5 of 11 is still below 50%
		if d.State() != OutlierOK {
			t.Fatal("ejected at 45% failures")
		}
		record(Failure, 1) // 6 of 12
		if d.State() != OutlierEjected || !errors.Is(d.Status().Cause, ErrFailureRate) {
			t.Fatalf("state %v at 50%% failures", d.State())
		}
	})
}

func TestOutlierDisabledAndClosed(t *testing.T) {
	d := NewOutlierDetector(OutlierConfig{}, nil, nil)
	fail(t, d, 100)
	if d.State() != OutlierOK {
		t.Fatal("disabled detector ejected")
	}
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		d := NewOutlierDetector(OutlierConfig{ConsecutiveFailures: 1}, clk, nil)
		fail(t, d, 1)
		d.Close()
		d.Close()
		if clk.Pending() != 0 {
			t.Fatal("Close left the timer")
		}
		clk.Advance(time.Hour)
		synctest.Wait()
		if d.State() != OutlierEjected {
			t.Fatal("closed detector changed state")
		}
		d.Reset()
		if d.State() != OutlierEjected {
			t.Fatal("closed detector reset")
		}
	})
	if (OutlierConfig{FailurePercent: 1}).Enabled() != true || (OutlierConfig{}).Enabled() {
		t.Fatal("Enabled")
	}
}

func TestBackoffBounds(t *testing.T) {
	p := newOutlierParams(OutlierConfig{BaseEjection: 3 * time.Second, MaxEjection: time.Second}, nil)
	if p.max != 3*time.Second || p.backoff(0) != 3*time.Second || p.backoff(5) != 3*time.Second {
		t.Fatalf("max below base not raised: %v", p.max)
	}
	p = newOutlierParams(OutlierConfig{BaseEjection: time.Nanosecond, MaxEjection: time.Duration(1<<63 - 1)}, nil)
	for _, level := range []uint{0, 1, 30, 61, 62, 63, 1000} {
		if b := p.backoff(level); b <= 0 || b > p.max {
			t.Fatalf("backoff(%d) = %v", level, b)
		}
	}
}

func TestPoolPassiveHealth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 2, BaseEjection: time.Second}})
		defer p.Close()
		if err := p.Sync(targets("a", "b")); err != nil {
			t.Fatal(err)
		}
		var log eventLog
		p.Subscribe(log.add)
		log.take()

		a, _ := p.Backend("a")
		for a.Status().Outlier.State == OutlierOK {
			l, err := p.Pick()
			if err != nil {
				t.Fatal(err)
			}
			o := Success
			if l.Backend().ID() == "a" {
				o = Failure
			}
			l.Release(o)
		}
		if got := pickIDs(t, p, 10); got["b"] != 10 {
			t.Fatalf("ejected backend picked: %v", got)
		}
		clk.Advance(time.Second)
		synctest.Wait()
		if a.State() != StateHalfOpen {
			t.Fatalf("state %v after the ejection", a.State())
		}
		// Only one trial at a time: picks of a beyond it go to b.
		var trial Lease[string]
		for trial.Backend() == nil {
			l := mustPick(t, p)
			if l.Backend().ID() == "a" {
				trial = l
			} else {
				l.Release(Success)
			}
		}
		if got := pickIDs(t, p, 10); got["b"] != 10 {
			t.Fatalf("second trial admitted: %v", got)
		}
		trial.Release(Success)
		want := []string{
			"state-changed a healthy->ejected",
			"state-changed a ejected->half-open",
			"state-changed a half-open->healthy",
		}
		if got := log.take(); !slices.Equal(got, want) {
			t.Fatalf("events %v, want %v", got, want)
		}
		if !p.ResetOutlier("a") || p.ResetOutlier("zz") {
			t.Fatal("ResetOutlier")
		}
		// Removing the backend stops its timer.
		for a.Status().Outlier.State == OutlierOK {
			l := mustPick(t, p)
			o := Success
			if l.Backend().ID() == "a" {
				o = Failure
			}
			l.Release(o)
		}
		if clk.Pending() != 1 {
			t.Fatalf("%d timers for one ejection", clk.Pending())
		}
		p.Remove("a")
		if clk.Pending() != 0 {
			t.Fatalf("%d timers after removing the ejected backend", clk.Pending())
		}
	})
}

func TestPoolCloseStopsTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		p := New[string](Config{Clock: clk, Outlier: OutlierConfig{ConsecutiveFailures: 1}})
		if err := p.Sync(targets("a")); err != nil {
			t.Fatal(err)
		}
		l := mustPick(t, p)
		l.Release(Failure)
		if clk.Pending() != 1 {
			t.Fatal("no ejection timer")
		}
		p.Close()
		if clk.Pending() != 0 {
			t.Fatal("Close left the timer")
		}
	})
}
