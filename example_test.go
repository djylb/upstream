package upstream_test

import (
	"context"
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
	err := pool.Sync([]upstream.Target[string]{
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
	pool := upstream.New[string](upstream.Config{
		Retry: upstream.RetryConfig{Attempts: 3, Budget: 0.2},
	})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{
		{ID: "primary", Value: "10.0.0.1:8080"},
		{ID: "backup", Value: "10.0.0.2:8080", Priority: 1},
	})

	// Do releases each lease with the outcome of the function's error and
	// retries a failure on a backend it has not tried.
	err := pool.Do(context.Background(), func(b *upstream.Backend[string]) error {
		if b.ID() == "primary" {
			return errors.New("connection refused")
		}
		fmt.Println("served by", b.ID())
		return nil
	})
	fmt.Println(err)
	fmt.Println("primary failures:", pool.Status()[0].Failures)
	// Output:
	// served by backup
	// <nil>
	// primary failures: 1
}

func ExampleRequest() {
	pool := upstream.New[string](upstream.Config{Retry: upstream.RetryConfig{Attempts: 2}})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{{ID: "a"}, {ID: "b"}})

	// A Request leases distinct backends for one request: each Next
	// leases a backend not tried before, as the retry settings allow.
	req := pool.Request("client-key")
	first, _ := req.Next()
	first.Release(upstream.Failure)
	second, _ := req.Next()
	fmt.Println(first.Backend() == nil, second.Backend() != nil)
	second.Release(upstream.Success)
	_, err := req.Next()
	fmt.Println(err)
	// Output:
	// true true
	// upstream: retry limit reached
}

func ExampleRequest_Do() {
	pool := upstream.New[string](upstream.Config{Retry: upstream.RetryConfig{Attempts: 2}})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{{ID: "a"}, {ID: "b"}})

	// Do runs the retry loop for leases that outlive it, such as
	// connections: Take hands the lease of the attempt that worked on.
	var kept upstream.Lease[string]
	req := pool.Request("")
	err := req.Do(context.Background(), func(l *upstream.Lease[string]) (retry bool, err error) {
		if req.Tried() == 1 {
			return true, errors.New("refused") // released as a Failure, then retried
		}
		kept = l.Take()
		return false, nil
	})
	fmt.Println(err, req.Tried(), kept.Backend() != nil)
	kept.Release(upstream.Success)
	// Output:
	// <nil> 2 true
}

func ExampleLease_Report() {
	pool := upstream.New[string](upstream.Config{Balancer: upstream.LeastConn()})
	defer pool.Close()
	if err := pool.Sync([]upstream.Target[string]{{ID: "relay"}}); err != nil {
		fmt.Println(err)
		return
	}

	lease, err := pool.Pick()
	if err != nil {
		fmt.Println(err)
		return
	}
	// The connection is up: report the outcome now and keep the in-flight
	// slot until the connection closes.
	lease.Report(upstream.Success)
	s := pool.Status()[0]
	fmt.Println(s.InFlight, s.Successes)
	lease.Release(upstream.Neutral) // the reported outcome stands
	s = pool.Status()[0]
	fmt.Println(s.InFlight, s.Successes, s.Neutrals)
	// Output:
	// 1 1
	// 0 1 0
}

func ExampleParseBalancer() {
	// Balancers and fallbacks by name, as a configuration file gives them.
	b, err := upstream.ParseBalancer("least-conn")
	if err != nil {
		fmt.Println(err)
		return
	}
	f, _ := upstream.ParseFallback("all")
	pool := upstream.New[string](upstream.Config{Balancer: b, Fallback: f})
	defer pool.Close()
	fmt.Println(b, f)
	// Output: least-conn all
}

func ExamplePool_PickKey() {
	pool := upstream.New[string](upstream.Config{Balancer: upstream.ConsistentHash(upstream.HashConfig{})})
	defer pool.Close()
	if err := pool.Sync([]upstream.Target[string]{{ID: "a"}, {ID: "b"}, {ID: "c"}}); err != nil {
		fmt.Println(err)
		return
	}

	const key = "session"
	first, err := pool.PickKey(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer first.Release(upstream.Success)
	again, err := pool.PickKey(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer again.Release(upstream.Success)
	fmt.Println("sticky:", first.Backend() == again.Backend())
	// Output: sticky: true
}

func ExamplePool_Remove() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	if err := pool.Sync([]upstream.Target[string]{{ID: "old"}, {ID: "new"}}); err != nil {
		fmt.Println(err)
		return
	}

	lease, err := pool.Acquire("old")
	if err != nil {
		fmt.Println(err)
		return
	}
	drained, ok := pool.Remove("old")
	if !ok {
		lease.Release(upstream.Neutral)
		fmt.Println("backend already removed")
		return
	}
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
	_ = pool.Sync([]upstream.Target[string]{{ID: "a"}})

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
	d := upstream.NewOutlierDetector(upstream.OutlierConfig{ConsecutiveFailures: 2}, nil, nil)
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

func ExampleTarget_priority() {
	// Two primaries share the load; the backup takes traffic when both are
	// down, and, with Overflow, when both are at their MaxInFlight.
	pool := upstream.New[string](upstream.Config{Overflow: true})
	defer pool.Close()
	if err := pool.Sync([]upstream.Target[string]{
		{ID: "primary-1", MaxInFlight: 1},
		{ID: "primary-2", MaxInFlight: 1},
		{ID: "backup", Priority: 1},
	}); err != nil {
		fmt.Println(err)
		return
	}

	var leases []upstream.Lease[string]
	defer func() {
		for i := range leases {
			leases[i].Release(upstream.Neutral)
		}
	}()
	for range 3 {
		l, err := pool.Pick()
		if err != nil {
			fmt.Println(err)
			return
		}
		leases = append(leases, l)
	}
	fmt.Println("third pick:", leases[2].Backend().ID())
	// Output:
	// third pick: backup
}

func ExamplePool_MarkOffline() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{{ID: "edge-1", Value: "edge-1"}})
	defer pool.Subscribe(func(ev upstream.Event[string]) {
		if ev.Kind == upstream.EventStateChanged {
			fmt.Println(ev.Backend.ID(), ev.From, "->", ev.To, ev.Err)
		}
	})()

	// The session the backend is reached through dropped, and came back.
	b, _ := pool.Backend("edge-1")
	pool.MarkOffline(b, errors.New("session closed"))
	_, err := pool.Pick()
	fmt.Println(err)
	pool.MarkOnline(b)
	// Output:
	// edge-1 healthy -> offline session closed
	// upstream: no backend available
	// edge-1 offline -> healthy <nil>
}

func ExampleRendezvous() {
	b := upstream.Rendezvous(upstream.RendezvousConfig{Key: [16]byte{1, 2, 3}})
	members := []upstream.Candidate{{ID: "a", Weight: 1}, {ID: "b", Weight: 2}, {ID: "c", Weight: 1}}
	before := b.Build(members)
	after := b.Build(members[:2]) // c leaves

	// Only the keys that were on c move.
	moved := 0
	for key := range uint64(1000) {
		i, j := before.Pick(key, 0), after.Pick(key, 0)
		if i != 2 && i != j {
			moved++
		}
	}
	fmt.Println(moved)
	// Output: 0
}
