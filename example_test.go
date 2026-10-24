package upstream_test

import (
	"errors"
	"fmt"
	"net"

	"github.com/djylb/upstream"
)

func Example() {
	pool := upstream.New[string](upstream.Config{
		Balancer: upstream.PowerOfTwo(),
		Outlier:  upstream.OutlierConfig{ConsecutiveFailures: 5},
	})
	defer pool.Close()
	err := pool.Update([]upstream.Target[string]{
		{ID: "a", Value: "10.0.0.1:8080", Weight: 2},
		{ID: "b", Value: "10.0.0.2:8080"},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	lease, err := pool.Pick()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("in flight:", lease.Backend().InFlight())
	// Connect to lease.Value() here, then report how it went.
	lease.Release(upstream.Success)
	fmt.Println("in flight:", pool.Status()[0].InFlight+pool.Status()[1].InFlight)
	// Output:
	// in flight: 1
	// in flight: 0
}

func ExamplePool_Do() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	_ = pool.Update([]upstream.Target[string]{{ID: "local", Value: "127.0.0.1:1"}})

	// Do releases the lease with the outcome of the function's error.
	err := pool.Do(func(b *upstream.Backend[string]) error {
		return errors.New("connection refused")
	})
	fmt.Println(err)
	fmt.Println("failures:", pool.Status()[0].Failures)
	// Output:
	// connection refused
	// failures: 1
}

func ExamplePool_PickKey() {
	pool := upstream.New[string](upstream.Config{Balancer: upstream.ConsistentHash(upstream.HashConfig{})})
	defer pool.Close()
	_ = pool.Update([]upstream.Target[string]{{ID: "a"}, {ID: "b"}, {ID: "c"}})

	first, _ := pool.PickKey("198.51.100.7")
	again, _ := pool.PickKey("198.51.100.7")
	fmt.Println("sticky:", first.Backend() == again.Backend())
	first.Release(upstream.Success)
	again.Release(upstream.Success)
	// Output: sticky: true
}

func ExamplePool_Remove() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	_ = pool.Update([]upstream.Target[string]{{ID: "old"}, {ID: "new"}})

	lease, _ := pool.PickKey("")
	for lease.Backend().ID() != "old" {
		lease.Release(upstream.Neutral)
		lease, _ = pool.PickKey("")
	}
	drained, _ := pool.Remove("old")
	select {
	case <-drained.Done():
	default:
		fmt.Println("waiting for the last lease")
	}
	lease.Release(upstream.Success)
	<-drained.Done()
	fmt.Println("drained")
	// Output:
	// waiting for the last lease
	// drained
}

func ExamplePool_Subscribe() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	_ = pool.Update([]upstream.Target[string]{{ID: "a"}})

	cancel := pool.Subscribe(func(ev upstream.Event[string]) {
		fmt.Println(ev.Kind, ev.Backend.ID(), ev.From, "->", ev.To, ev.Err)
	})
	defer cancel()
	a, _ := pool.Backend("a")
	refused := errors.New("refused")
	// Repeated failures report one transition.
	pool.ReportCheck(a, upstream.HealthDown, upstream.CheckResult{Err: refused})
	pool.ReportCheck(a, upstream.HealthDown, upstream.CheckResult{Err: refused})
	pool.ReportCheck(a, upstream.HealthUp, upstream.CheckResult{})
	// Output:
	// added a healthy -> healthy <nil>
	// state-changed a healthy -> unhealthy refused
	// state-changed a unhealthy -> healthy <nil>
}

func ExampleOutlierDetector() {
	// Passive health checking for a single connection target, without a
	// pool.
	d := upstream.NewOutlierDetector(upstream.OutlierConfig{ConsecutiveFailures: 2}, nil)
	defer d.Close()
	dial := func() error {
		t, ok := d.Admit()
		if !ok {
			return errors.New("backend ejected")
		}
		_, err := net.Dial("tcp", "invalid address")
		d.Record(t, upstream.OutcomeOf(err))
		return err
	}
	for range 3 {
		_ = dial()
	}
	fmt.Println(d.State(), d.Status().Ejections)
	// Output: ejected 1
}

func ExampleConsistentHash() {
	ring := upstream.ConsistentHash(upstream.HashConfig{}).Build([]upstream.Candidate{
		{ID: "cache-1"}, {ID: "cache-2"}, {ID: "cache-3"},
	})
	key := uint64(12345)
	primary := ring.Pick(key, 0)
	backup := ring.Pick(key, 1) // where the key goes if the primary leaves
	fmt.Println(primary != backup)
	// Output: true
}
