package health

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// TCPOptions configure the TCP check.
type TCPOptions struct {
	// Dial replaces the default dialer, for example to use a custom
	// resolver, bind a source address or reach targets through a tunnel.
	// With a custom Dial, Latency covers the whole call, name resolution
	// included. (*net.Dialer).DialContext has this type.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	// Reset closes each check connection with a reset instead of the
	// normal close. A normal close lets the target see a clean end of
	// stream and leaves a TIME_WAIT entry on the checking host for a short
	// while; a reset leaves no TIME_WAIT entry, which matters when many
	// targets are checked often, but the target may log an aborted
	// connection.
	Reset bool
}

// TCP returns the TCP check: it connects to the target's address and
// closes the connection as soon as it is established, without writing or
// reading anything. The check passes if the connection is established.
// Host names are resolved by each check; when a name has several
// addresses, they are tried as net.Dialer does, and the Observation names
// the one that answered.
func TCP(o TCPOptions) Probe {
	return func(ctx context.Context, t Target) (Observation, error) {
		network := t.Network
		if network == "" {
			network = "tcp"
		}
		var (
			conn net.Conn
			err  error
			obs  Observation
		)
		begin := time.Now()
		switch {
		case o.Dial != nil:
			conn, err = o.Dial(ctx, network, t.Address)
			obs.Latency = time.Since(begin)
			if err == nil && conn == nil {
				err = errNoConn
			}
		case !controlHooks:
			conn, err = new(net.Dialer).DialContext(ctx, network, t.Address)
			obs.Latency = time.Since(begin)
		default:
			// Record when each connection attempt starts so that the
			// latency excludes name resolution and failed attempts.
			var mu sync.Mutex
			starts := make(map[string]time.Time, 2)
			d := net.Dialer{ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
				mu.Lock()
				starts[address] = time.Now()
				mu.Unlock()
				return nil
			}}
			conn, err = d.DialContext(ctx, network, t.Address)
			end := time.Now()
			start := begin
			if err == nil {
				mu.Lock()
				if s, ok := starts[addrString(conn.RemoteAddr())]; ok {
					start = s
				}
				mu.Unlock()
			}
			obs.Latency = end.Sub(start)
		}
		if err != nil {
			return obs, err
		}
		obs.Addr = addrString(conn.RemoteAddr())
		if tc, ok := conn.(*net.TCPConn); ok && o.Reset {
			_ = tc.SetLinger(0)
		}
		_ = conn.Close() // the target was reachable; a close error changes nothing
		return obs, nil
	}
}

// errNoConn is the error of a TCP check whose Dial returned neither a
// connection nor an error.
var errNoConn = errors.New("health: dial returned no connection")

// addrString formats a, which may be nil for a connection of a custom Dial.
func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

// controlHooks reports whether dialers call their Control hooks; the
// simulated networks of js and wasip1 refuse sockets that have one.
const controlHooks = runtime.GOOS != "js" && runtime.GOOS != "wasip1"

func isTCP(network string) bool {
	switch network {
	case "", "tcp", "tcp4", "tcp6":
		return true
	}
	return false
}
