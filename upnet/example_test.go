package upnet_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/upnet"
)

// A TCP relay picks the backend with the fewest open connections for each
// client and fails over to another one when a dial fails.
func ExampleDialer() {
	pool := upstream.New[string](upstream.Config{
		Balancer: upstream.LeastConn(),
		Outlier:  upstream.OutlierConfig{ConsecutiveFailures: 3},
		Retry:    upstream.RetryConfig{Attempts: 3},
	})
	defer pool.Close()
	_ = pool.Sync([]upstream.Target[string]{{ID: "a", Value: "192.0.2.1:9000"}, {ID: "b", Value: "192.0.2.2:9000"}})

	dialer := &upnet.Dialer[string]{
		Pool:        pool,
		DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
	}
	relay := func(client net.Conn) {
		defer func() { _ = client.Close() }()
		backend, err := dialer.Dial(context.Background())
		if err != nil {
			return // every backend failed or none is available
		}
		defer func() { _ = backend.Close() }() // releases the backend
		go func() {
			_, _ = io.Copy(backend, client)
			_ = backend.CloseWrite()
		}()
		_, _ = io.Copy(client, backend)
	}
	_ = relay
	fmt.Println(pool.Available())
	// Output: 2
}
