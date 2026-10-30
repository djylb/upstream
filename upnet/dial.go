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

// A Dialer opens connections to the backends of Pool. A failed dial counts
// as a Failure of its backend and, as the pool's upstream.RetryConfig
// allows, is retried on another backend; an established connection
// reports Success at once, so that a half-open backend's trial ends with
// the dial rather than with the connection, unless DeferReport is set.
// The time the dial took, by the pool's Clock, is observed as the
// backend's latency (see upstream.Lease.Observe). A panic in Address or DialContext releases the
// backend's lease and goes on. The fields must not change while dials are
// in progress.
type Dialer[T any] struct {
	// Pool holds the backends.
	Pool *upstream.Pool[T]

	// Address returns the address to dial for backend b. Nil uses the
	// address of the backend's value, as upstream.AddressOf finds it; for
	// a pool whose values have no address, dials fail with
	// upstream.ErrNoAddress, and a backend whose value has none, such as a
	// nil Addresser, fails its attempt with it.
	Address func(b *upstream.Backend[T]) string

	// DialContext opens a connection, as the method of net.Dialer does,
	// for example to bind a source address or set keep-alives. Nil means
	// a zero net.Dialer. A connection it returns together with an error is
	// closed.
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
	address := func(b *upstream.Backend[T]) (string, bool) { return d.Address(b), true }
	if d.Address == nil {
		if !upstream.Addressable[T]() {
			return nil, upstream.ErrNoAddress
		}
		address = func(b *upstream.Backend[T]) (string, bool) { return upstream.AddressOf(b.Value()) }
	}
	clock := d.Pool.Clock()
	dial := d.DialContext
	if dial == nil {
		dial = new(net.Dialer).DialContext
	}
	network := d.Network
	if network == "" {
		network = "tcp"
	}
	var c *Conn[T]
	r := d.Pool.Request(key)
	err := r.Do(ctx, func(l *upstream.Lease[T]) (bool, error) {
		addr, ok := address(l.Backend())
		if !ok {
			return true, upstream.ErrNoAddress
		}
		start := clock.Now()
		conn, err := dial(ctx, network, addr)
		switch {
		case err != nil && conn != nil:
			_ = conn.Close()
		case err == nil && conn == nil:
			err = errNoConn
		}
		if err != nil {
			return upstream.OutcomeOf(err) == upstream.Failure, err
		}
		l.Observe(clock.Now().Sub(start))
		if !d.DeferReport {
			l.Report(upstream.Success)
		}
		c = &Conn[T]{Conn: conn, b: l.Backend(), lease: l.Take()}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

var errNoConn = errors.New("upnet: dial returned no connection")

// A Conn is a connection to a backend. Closing it releases the backend's
// lease. It forwards CloseRead, CloseWrite, ReadFrom and WriteTo to the
// underlying connection where it has them, so that relays keep half-close
// and the zero-copy paths of io.Copy.
//
// Report and Close never call the pool under the mutex: they claim the
// lease under it and use it after. A Close that comes while a Report is
// in progress, as from a pool subscriber that Report runs, leaves the
// release to the Report.
type Conn[T any] struct {
	net.Conn
	b         *upstream.Backend[T]
	mu        sync.Mutex
	lease     upstream.Lease[T] // guarded by mu, except by the reporter
	reporting bool              // a Report is using lease; guarded by mu
	reported  bool              // guarded by mu
	closed    bool              // Close has claimed lease; guarded by mu
}

// Backend returns the backend the connection goes to.
func (c *Conn[T]) Backend() *upstream.Backend[T] { return c.b }

// Report records how the connection went for passive health checking,
// such as the result of a TLS handshake. Only the first outcome of the
// connection counts: without Dialer.DeferReport, the dial already reported
// Success, and Report does nothing. Report does nothing after Close.
func (c *Conn[T]) Report(o upstream.Outcome) {
	c.mu.Lock()
	if c.closed || c.reported {
		c.mu.Unlock()
		return
	}
	c.reported = true
	c.reporting = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.reporting = false
		closed := c.closed
		c.mu.Unlock()
		if closed {
			c.lease.Release(upstream.Neutral) // the outcome was reported
		}
	}()
	c.lease.Report(o)
}

// NetConn returns the underlying connection.
func (c *Conn[T]) NetConn() net.Conn { return c.Conn }

// Close closes the connection and releases the backend's lease; later
// calls return the underlying connection's error and release nothing.
func (c *Conn[T]) Close() error {
	err := c.Conn.Close()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return err
	}
	c.closed = true
	if c.reporting {
		c.mu.Unlock() // the Report releases the lease
		return err
	}
	l := c.lease.Take()
	c.mu.Unlock()
	l.Release(upstream.Neutral) // unless reported, the outcome is neutral
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
