// Package upnet connects to the backends of an upstream.Pool: a Dialer
// picks a backend, dials it, retries on another backend when the dial
// fails, and holds the backend's lease for as long as the connection is
// open, so that in-flight counts, least-connections balancing and drains
// follow real connections. It is separate so that the root package does
// not depend on net.
package upnet

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/djylb/upstream"
)

// ErrNoAddress means a Dialer has no Address function and its pool's
// values are not strings.
var ErrNoAddress = errors.New("upnet: Address is nil and the backend values are not strings")

// A Dialer opens connections to the backends of Pool. A failed dial counts
// as a Failure of its backend and, as the pool's upstream.RetryConfig
// allows, is retried on another backend; an established connection
// reports Success at once, so that a half-open backend's trial ends with
// the dial rather than with the connection, unless DeferReport is set. A
// panic in Address or DialContext releases the backend's lease and goes
// on. The fields must not change while dials are in progress.
type Dialer[T any] struct {
	// Pool holds the backends.
	Pool *upstream.Pool[T]

	// Address returns the address to dial for backend b. Nil uses the
	// backend's value, for pools of strings.
	Address func(b *upstream.Backend[T]) string

	// DialContext opens a connection, as the method of net.Dialer does,
	// for example to bind a source address or set keep-alives. Nil means
	// a zero net.Dialer.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)

	// Network is the network to dial, as for net.Dial. Empty means "tcp".
	Network string

	// DeferReport leaves the outcome of an established connection to the
	// caller, for connections that must pass a handshake, such as TLS,
	// before they count: call Conn.Report with its result. A Conn closed
	// without a report counts as Neutral.
	DeferReport bool
}

// Dial connects to a backend picked without a key. It returns the last
// dial error if every attempt failed, and otherwise ctx's error or the
// pool's error when no backend could be picked.
func (d *Dialer[T]) Dial(ctx context.Context) (*Conn[T], error) {
	return d.DialKey(ctx, "")
}

// DialKey connects to a backend picked for key, as with Pool.PickKey.
func (d *Dialer[T]) DialKey(ctx context.Context, key string) (*Conn[T], error) {
	address := d.Address
	if address == nil {
		var zero T
		if _, ok := any(zero).(string); !ok {
			return nil, ErrNoAddress
		}
		address = func(b *upstream.Backend[T]) string {
			s, _ := any(b.Value()).(string)
			return s
		}
	}
	dial := d.DialContext
	if dial == nil {
		dial = new(net.Dialer).DialContext
	}
	network := d.Network
	if network == "" {
		network = "tcp"
	}
	a := d.Pool.Attempts(key)
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return nil, last
			}
			return nil, err
		}
		l, err := a.Next()
		if err != nil {
			if last != nil {
				return nil, last
			}
			return nil, err
		}
		conn, err := dialLease(ctx, &l, dial, network, address)
		if err == nil {
			if !d.DeferReport {
				l.Report(upstream.Success)
			}
			return &Conn[T]{Conn: conn, b: l.Backend(), lease: l}, nil
		}
		o := upstream.OutcomeOf(err)
		l.Release(o)
		if o != upstream.Failure {
			return nil, err
		}
		last = err
	}
}

// dialLease dials the backend of l, releasing l with Failure if Address
// or the dial function panics.
func dialLease[T any](ctx context.Context, l *upstream.Lease[T], dial func(context.Context, string, string) (net.Conn, error), network string, address func(*upstream.Backend[T]) string) (net.Conn, error) {
	done := false
	defer func() {
		if !done {
			l.Release(upstream.Failure)
		}
	}()
	conn, err := dial(ctx, network, address(l.Backend()))
	done = true
	if err == nil && conn == nil {
		err = errNoConn
	}
	return conn, err
}

var errNoConn = errors.New("upnet: dial returned no connection")

// A Conn is a connection to a backend. Closing it releases the backend's
// lease. It forwards CloseRead, CloseWrite, ReadFrom and WriteTo to the
// underlying connection where it has them, so that relays keep half-close
// and the zero-copy paths of io.Copy.
type Conn[T any] struct {
	net.Conn
	b     *upstream.Backend[T]
	mu    sync.Mutex
	lease upstream.Lease[T] // guarded by mu
}

// Backend returns the backend the connection goes to.
func (c *Conn[T]) Backend() *upstream.Backend[T] { return c.b }

// Report records how the connection went for passive health checking,
// such as the result of a TLS handshake. Only the first outcome of the
// connection counts: without Dialer.DeferReport, the dial already reported
// Success, and Report does nothing.
func (c *Conn[T]) Report(o upstream.Outcome) {
	c.mu.Lock()
	c.lease.Report(o)
	c.mu.Unlock()
}

// NetConn returns the underlying connection.
func (c *Conn[T]) NetConn() net.Conn { return c.Conn }

// Close closes the connection and releases the backend's lease; later
// calls return the underlying connection's error and release nothing.
func (c *Conn[T]) Close() error {
	err := c.Conn.Close()
	c.mu.Lock()
	c.lease.Release(upstream.Neutral) // unless reported, the outcome is neutral
	c.mu.Unlock()
	return err
}

// CloseWrite shuts down the writing side, if the underlying connection
// can.
func (c *Conn[T]) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// CloseRead shuts down the reading side, if the underlying connection can.
func (c *Conn[T]) CloseRead() error {
	if cr, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return cr.CloseRead()
	}
	return errors.ErrUnsupported
}

// ReadFrom implements io.ReaderFrom.
func (c *Conn[T]) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(writerOnly{c.Conn}, r)
}

// WriteTo implements io.WriterTo.
func (c *Conn[T]) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := c.Conn.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, readerOnly{c.Conn})
}

// writerOnly and readerOnly hide the other methods of a connection from
// io.Copy, which would otherwise call back into Conn.
type writerOnly struct{ io.Writer }

type readerOnly struct{ io.Reader }
