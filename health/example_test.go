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
	_ = pool.Update([]upstream.Target[string]{{ID: "db-1", Value: "10.0.0.1:5432"}, {ID: "db-2", Value: "10.0.0.2:5432"}})

	// Balance locally across the targets that pass a TCP check.
	checker := health.Watch(context.Background(), pool,
		func(b *upstream.Backend[string]) health.Target { return health.Target{Address: b.Value()} },
		health.Config[string]{Options: health.Options{Interval: 5 * time.Second, Rise: 2, Fall: 2}})
	defer checker.Close()
}
