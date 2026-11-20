package upstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream/internal/fakeclock"
)

// record admits n requests and records o for each.
func record(t *testing.T, d *OutlierDetector, o Outcome, n int) {
	t.Helper()
	for range n {
		tk, ok := d.Admit()
		if !ok {
			t.Fatal("not admitted")
		}
		d.Record(tk, o)
	}
}

// Degraded counts in the failure-rate window only: it neither extends nor
// resets the consecutive run, and never ejects below MinRequests.
func TestDegradedCountsInRateOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		d := NewOutlierDetector(OutlierConfig{
			ConsecutiveFailures: 3,
			FailurePercent:      50,
			MinRequests:         10,
		}, clk, nil)
		defer d.Close()

		// A burst far beyond ConsecutiveFailures, below MinRequests.
		record(t, d, Degraded, 9)
		if st := d.Status(); st.State != OutlierOK || st.ConsecutiveFailures != 0 {
			t.Fatalf("degraded burst below MinRequests: %+v", st)
		}
		// Degraded does not end a run of failures either.
		clk.Advance(30 * time.Second) // forget the window
		record(t, d, Failure, 2)
		record(t, d, Degraded, 3) // 5 requests, below MinRequests
		if st := d.Status(); st.State != OutlierOK || st.ConsecutiveFailures != 2 {
			t.Fatalf("status %+v, want a run of 2", st)
		}
		record(t, d, Failure, 1) // the run reaches 3
		if st := d.Status(); st.State != OutlierEjected || !errors.Is(st.Cause, ErrConsecutiveFailures) {
			t.Fatalf("status %+v, want consecutive ejection", st)
		}
	})
}

func TestDegradedReachesMinRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		d := NewOutlierDetector(OutlierConfig{
			ConsecutiveFailures: 1,
			FailurePercent:      50,
			MinRequests:         10,
			BaseEjection:        time.Second,
		}, clk, nil)
		defer d.Close()
		record(t, d, Success, 5)
		record(t, d, Degraded, 4)
		if d.State() != OutlierOK {
			t.Fatal("ejected below MinRequests")
		}
		record(t, d, Degraded, 1) // 5 of 10
		if st := d.Status(); st.State != OutlierEjected || !errors.Is(st.Cause, ErrFailureRate) {
			t.Fatalf("status %+v, want rate ejection", st)
		}
		// A half-open trial answered Degraded fails.
		clk.Advance(time.Second)
		synctest.Wait()
		if d.State() != OutlierHalfOpen {
			t.Fatalf("state %v", d.State())
		}
		record(t, d, Degraded, 1)
		if st := d.Status(); st.State != OutlierEjected || !errors.Is(st.Cause, ErrTrialFailed) {
			t.Fatalf("status %+v, want failed trial", st)
		}
	})
}

// Degraded without a rate rule records nothing against the backend.
func TestDegradedWithoutRateRule(t *testing.T) {
	d := NewOutlierDetector(OutlierConfig{ConsecutiveFailures: 1}, nil, nil)
	defer d.Close()
	record(t, d, Degraded, 100)
	if st := d.Status(); st.State != OutlierOK || st.ConsecutiveFailures != 0 {
		t.Fatalf("status %+v", st)
	}
}

func TestDegradedPoolCountsAndNoRetry(t *testing.T) {
	p := New[string](Config{Retry: RetryConfig{Attempts: 3}})
	defer p.Close()
	if err := p.Sync(targets("a", "b")); err != nil {
		t.Fatal(err)
	}
	tries := 0
	bad := WithOutcome(errors.New("503"), Degraded)
	if err := p.Do(context.Background(), func(*Backend[string]) error { tries++; return bad }); !errors.Is(err, bad) || tries != 1 {
		t.Fatalf("Do = %v after %d tries", err, tries)
	}
	var n uint64
	for _, s := range p.Status() {
		n += s.Degraded + s.Failures
		if s.Failures != 0 {
			t.Fatalf("counted as failure: %+v", s)
		}
	}
	if n != 1 {
		t.Fatalf("%d degraded outcomes counted, want 1", n)
	}
	var o Outcome
	if err := o.UnmarshalText([]byte("degraded")); err != nil || o != Degraded {
		t.Fatalf("UnmarshalText = %v, %v", o, err)
	}
}

// Concurrent Degraded and Failure outcomes: under -race; the run only ever
// counts failures, so a run of Degraded below MinRequests never ejects.
func TestDegradedConcurrent(t *testing.T) {
	d := NewOutlierDetector(OutlierConfig{ConsecutiveFailures: 2, FailurePercent: 1, MinRequests: 1 << 20}, nil, nil)
	defer d.Close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if tk, ok := d.Admit(); ok {
					d.Record(tk, Degraded)
				}
			}
		})
	}
	wg.Go(func() {
		for range 1000 {
			if tk, ok := d.Admit(); ok {
				d.Record(tk, Success)
			}
		}
	})
	wg.Wait()
	if st := d.Status(); st.State != OutlierOK || st.ConsecutiveFailures != 0 || st.Ejections != 0 {
		t.Fatalf("status %+v", st)
	}
}
