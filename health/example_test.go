package health_test

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/health"
)

// A client that forwards tunnels to local targets checks each target and
// reports reachability transitions upstream. Targets are keyed by address
// and grouped by tunnel, so an address used by two tunnels is checked once.
func Example() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	changes := make(chan health.Report[string], 4)
	checker := health.New[string, int](context.Background(), health.Config[string]{
		Options:  health.Options{Interval: 10 * time.Second, Timeout: 3 * time.Second},
		OnChange: func(r health.Report[string]) { changes <- r },
	})
	defer checker.Close()

	// The server pushes tunnel 7 with its targets.
	addr := ln.Addr().String()
	checker.SyncGroup(7, map[string]health.Target{
		addr:           {Address: addr},                           // TCP connect check
		"192.0.2.1:53": {Network: "udp", Address: "192.0.2.1:53"}, // not checked
	})

	r := <-changes
	fmt.Println(r.Key == addr, r.Health, r.Reason)
	s, _ := checker.Status("192.0.2.1:53")
	fmt.Println(s.Checking, s.Health)

	// Deleting the tunnel stops all its checks at once.
	fmt.Println(checker.RemoveGroup(7), checker.Len())
	// Output:
	// true up none
	// false unknown
	// 2 0
}

func ExampleWatch() {
	pool := upstream.New[string](upstream.Config{Balancer: upstream.LeastConn()})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{{ID: "db-1", Value: "192.0.2.11:5432"}, {ID: "db-2", Value: "192.0.2.12:5432"}})

	// Balance locally across the members that pass a TCP check of their
	// value; the watcher closes with the pool.
	watcher, err := health.Watch(context.Background(), pool, nil,
		health.Config[string]{Options: health.Options{Interval: 5 * time.Second, Rise: 2, Fall: 2}})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer watcher.Close()
}

// A tunnel client keeps one pool per tunnel to balance new connections
// over the tunnel's healthy targets, and one checker for all of them, so
// that a target that several tunnels use is checked once.
func ExampleAttach() {
	checker := health.New[string, int](context.Background(), health.Config[string]{
		Options: health.Options{Interval: 10 * time.Second, Timeout: 3 * time.Second},
		OnChange: func(r health.Report[string]) {
			// Report reachability upstream: r.Key, r.Previous -> r.Health,
			// r.Reason, r.Stats.Average.
		},
	})
	defer checker.Close()

	tunnels := map[int]*upstream.Pool[string]{}
	push := func(tunnel int, addrs []string) { // a configuration push
		pool := tunnels[tunnel]
		if pool == nil {
			pool = upstream.New[string](upstream.Config{Retry: upstream.RetryConfig{Attempts: 3}})
			tunnels[tunnel] = pool
			// TCP check of each address; nil needs a pool of strings.
			if _, err := health.Attach(checker, tunnel, pool, nil); err != nil {
				panic(err)
			}
		}
		ts := make([]upstream.Target[string], len(addrs))
		for i, a := range addrs {
			ts[i] = upstream.Target[string]{ID: a, Value: a}
		}
		_ = pool.Sync(ts)
	}
	push(7, []string{"192.0.2.5:22", "192.0.2.6:22"})
	push(8, []string{"192.0.2.6:22"})
	fmt.Println(checker.Len())

	// Deleting a tunnel: closing its pool ends its checks.
	tunnels[7].Close()
	delete(tunnels, 7)
	for _, p := range tunnels {
		p.Close()
	}
	// Output: 2
}
