package upnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
)

var errRefused = errors.New("refused")

// pipeDialer dials in-memory pipes; dials of "bad" fail. served receives
// what the far end of each pipe reads before it closes.
type pipeDialer struct {
	wg     sync.WaitGroup
	served chan string
}

func (p *pipeDialer) dial(_ context.Context, _, address string) (net.Conn, error) {
	if address == "bad" {
		return nil, errRefused
	}
	client, server := net.Pipe()
	p.wg.Go(func() {
		defer func() { _ = server.Close() }()
		b, _ := io.ReadAll(server)
		p.served <- address + ":" + string(b)
	})
	return client, nil
}

func newPool(t *testing.T, cfg upstream.Config, ts ...upstream.Target[string]) *upstream.Pool[string] {
	t.Helper()
	p := upstream.New[string](cfg)
	t.Cleanup(p.Close)
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDialRetriesAndHoldsLease(t *testing.T) {
	pd := &pipeDialer{served: make(chan string, 4)}
	defer pd.wg.Wait()
	p := newPool(t, upstream.Config{Retry: upstream.RetryConfig{Attempts: 2}},
		upstream.Target[string]{ID: "a", Value: "bad"}, upstream.Target[string]{ID: "b", Value: "good", Priority: 1})
	d := &Dialer[string]{Pool: p, DialContext: pd.dial}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if conn.Backend().ID() != "b" {
		t.Fatalf("connected to %s", conn.Backend().ID())
	}
	st := p.Status()
	if st[0].Failures != 1 || st[1].InFlight != 1 || st[1].Successes != 1 {
		t.Fatalf("status %+v", st)
	}
	if _, err := conn.ReadFrom(strings.NewReader("hello")); err != nil { // a pipe has no ReadFrom
		t.Fatal(err)
	}
	if err := conn.CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseWrite on a pipe: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if got := <-pd.served; got != "good:hello" {
		t.Fatalf("served %q", got)
	}
	if st := p.Status()[1]; st.InFlight != 0 || st.Successes != 1 || st.Neutrals != 0 {
		t.Fatalf("after Close: %+v", st)
	}
	if conn.Backend().ID() != "b" {
		t.Fatal("closed connection forgot its backend")
	}

	// Every backend failing returns the last dial error.
	q := newPool(t, upstream.Config{Retry: upstream.RetryConfig{Attempts: 5}},
		upstream.Target[string]{ID: "a", Value: "bad"}, upstream.Target[string]{ID: "b", Value: "bad"})
	if _, err := (&Dialer[string]{Pool: q, DialContext: pd.dial}).DialKey(context.Background(), "k"); !errors.Is(err, errRefused) {
		t.Fatalf("Dial = %v", err)
	}
	if _, err := (&Dialer[string]{Pool: newPool(t, upstream.Config{}), DialContext: pd.dial}).Dial(context.Background()); !errors.Is(err, upstream.ErrNoBackends) {
		t.Fatalf("Dial on an empty pool = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Dial(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial with a done context = %v", err)
	}
}

func TestDialNeedsAddress(t *testing.T) {
	p := upstream.New[int](upstream.Config{})
	defer p.Close()
	_ = p.Sync([]upstream.Target[int]{{ID: "a", Value: 1}})
	if _, err := (&Dialer[int]{Pool: p}).Dial(context.Background()); !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Dial = %v", err)
	}
	pd := &pipeDialer{served: make(chan string, 1)}
	defer pd.wg.Wait()
	d := &Dialer[int]{Pool: p, DialContext: pd.dial, Address: func(*upstream.Backend[int]) string { return "one" }}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if got := <-pd.served; got != "one:" {
		t.Fatalf("served %q", got)
	}
	none := &Dialer[int]{Pool: p, Address: d.Address, DialContext: func(context.Context, string, string) (net.Conn, error) { return nil, nil }}
	if _, err := none.Dial(context.Background()); !errors.Is(err, errNoConn) {
		t.Fatalf("Dial without a connection = %v", err)
	}
}

func TestDialTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(c, buf); err != nil {
			done <- err
			return
		}
		_, err = c.Write(buf)
		done <- err
	}()
	p := newPool(t, upstream.Config{Balancer: upstream.LeastConn()}, upstream.Target[string]{ID: "l", Value: ln.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := (&Dialer[string]{Pool: p}).Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, ok := conn.NetConn().(*net.TCPConn); !ok {
		t.Fatalf("underlying %T", conn.NetConn())
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if _, err := io.CopyN(&got, conn, 4); err != nil || got.String() != "ping" {
		t.Fatalf("echo %q, %v", got.String(), err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDialAddressPanicReleases(t *testing.T) {
	p := newPool(t, upstream.Config{}, upstream.Target[string]{ID: "a", Value: "a"})
	d := &Dialer[string]{Pool: p, Address: func(*upstream.Backend[string]) string { panic("boom") }}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic did not go on")
			}
		}()
		_, _ = d.Dial(context.Background())
	}()
	if s := p.Status()[0]; s.InFlight != 0 || s.Failures != 1 {
		t.Fatalf("status after a panicking Address: %+v", s)
	}
}

func TestDeferReport(t *testing.T) {
	pd := &pipeDialer{served: make(chan string, 2)}
	defer pd.wg.Wait()
	p := newPool(t, upstream.Config{}, upstream.Target[string]{ID: "a", Value: "a"})
	d := &Dialer[string]{Pool: p, DialContext: pd.dial, DeferReport: true}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s := p.Status()[0]; s.Successes != 0 || s.InFlight != 1 {
		t.Fatalf("reported before the handshake: %+v", s)
	}
	conn.Report(upstream.Failure) // say, the TLS handshake failed
	conn.Report(upstream.Success) // ignored
	_ = conn.Close()
	<-pd.served
	if s := p.Status()[0]; s.Failures != 1 || s.Successes != 0 || s.Neutrals != 0 || s.InFlight != 0 {
		t.Fatalf("status %+v", s)
	}
	// Without a report, closing counts as neutral; without DeferReport,
	// Report does nothing.
	conn, _ = d.Dial(context.Background())
	_ = conn.Close()
	<-pd.served
	if s := p.Status()[0]; s.Neutrals != 1 {
		t.Fatalf("status %+v", s)
	}
}

func TestBackendCloseRace(t *testing.T) {
	p := newPool(t, upstream.Config{}, upstream.Target[string]{ID: "a", Value: "a"})
	d := &Dialer[string]{Pool: p, DialContext: func(context.Context, string, string) (net.Conn, error) {
		c, s := net.Pipe()
		_ = s.Close()
		return c, nil
	}}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { _ = conn.Backend() })
	wg.Go(func() { _ = conn.Close() })
	wg.Wait()
}

func TestDialPanicReleasesLease(t *testing.T) {
	p := newPool(t, upstream.Config{}, upstream.Target[string]{ID: "a", Value: "a"})
	d := &Dialer[string]{Pool: p, DialContext: func(context.Context, string, string) (net.Conn, error) { panic("boom") }}
	func() {
		defer func() { _ = recover() }()
		_, _ = d.Dial(context.Background())
	}()
	if n := p.Status()[0].InFlight; n != 0 {
		t.Fatalf("in flight %d after a panicking dial", n)
	}
}

// TestReportFromSubscriberClose checks that a subscriber that closes the
// connection whose Report ejects its backend, as a handler of
// upstream.StateEjected may, does not deadlock, and that the lease is
// released once.
func TestReportFromSubscriberClose(t *testing.T) {
	pd := &pipeDialer{served: make(chan string, 1)}
	defer pd.wg.Wait()
	p := newPool(t, upstream.Config{Outlier: upstream.OutlierConfig{ConsecutiveFailures: 1}}, upstream.Target[string]{ID: "a", Value: "a"})
	d := &Dialer[string]{Pool: p, DialContext: pd.dial, DeferReport: true}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Subscribe(func(ev upstream.Event[string]) {
		if ev.To == upstream.StateEjected {
			_ = conn.Close()
			conn.Report(upstream.Success) // after Close: ignored
		}
	})()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.Report(upstream.Failure)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Report deadlocked")
	}
	<-pd.served
	if s := p.Status()[0]; s.InFlight != 0 || s.Failures != 1 || s.Neutrals != 0 || s.State != upstream.StateEjected {
		t.Fatalf("status %+v", s)
	}
	_ = conn.Close()
}

// TestDialClosesConnWithError checks that a connection DialContext
// returns together with an error is closed, and that the dial is retried.
func TestDialClosesConnWithError(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	pd := &pipeDialer{served: make(chan string, 1)}
	defer pd.wg.Wait()
	p := newPool(t, upstream.Config{Retry: upstream.RetryConfig{Attempts: 2}},
		upstream.Target[string]{ID: "a", Value: "half"}, upstream.Target[string]{ID: "b", Value: "b", Priority: 1})
	d := &Dialer[string]{Pool: p, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "half" {
			return client, errRefused
		}
		return pd.dial(ctx, network, address)
	}}
	conn, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if conn.Backend().ID() != "b" {
		t.Fatalf("dialed %s", conn.Backend().ID())
	}
	if _, err := client.Write([]byte{0}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("connection left open: %v", err)
	}
	if conn.Backend().Latency() <= 0 {
		t.Fatal("dial latency not observed")
	}
	_ = conn.Close()
	<-pd.served
}

type zoned struct{ addr, zone string }

func (z zoned) Address() string { return z.addr }

// TestDialAddresser checks that values with an Address method need no
// Address function.
func TestDialAddresser(t *testing.T) {
	pd := &pipeDialer{served: make(chan string, 1)}
	defer pd.wg.Wait()
	p := upstream.New[zoned](upstream.Config{})
	defer p.Close()
	_ = p.Put(upstream.Target[zoned]{ID: "a", Value: zoned{addr: "10.0.0.1:80", zone: "z"}})
	conn, err := (&Dialer[zoned]{Pool: p, DialContext: pd.dial}).Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if got := <-pd.served; got != "10.0.0.1:80:" {
		t.Fatalf("served %q", got)
	}
}

// TestDialLatencyUsesPoolClock checks that the dial latency comes from
// the pool's Clock.
func TestDialLatencyUsesPoolClock(t *testing.T) {
	pd := &pipeDialer{served: make(chan string, 1)}
	defer pd.wg.Wait()
	clk := fakeclock.New()
	p := newPool(t, upstream.Config{Clock: clk}, upstream.Target[string]{ID: "a", Value: "a"})
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		clk.Advance(7 * time.Millisecond)
		return pd.dial(ctx, network, address)
	}
	conn, err := (&Dialer[string]{Pool: p, DialContext: dial}).Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	<-pd.served
	if got := conn.Backend().Latency(); got != 7*time.Millisecond {
		t.Fatalf("latency %v, want 7ms", got)
	}
}

// TestDialNilAddresser checks that a backend whose value is a nil
// Addresser fails with ErrNoAddress instead of dialing "".
func TestDialNilAddresser(t *testing.T) {
	p := upstream.New[upstream.Addresser](upstream.Config{})
	defer p.Close()
	_ = p.Put(upstream.Target[upstream.Addresser]{ID: "a"})
	dial := func(context.Context, string, string) (net.Conn, error) {
		t.Error("dialed a backend without an address")
		return nil, errRefused
	}
	if _, err := (&Dialer[upstream.Addresser]{Pool: p, DialContext: dial}).Dial(context.Background()); !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Dial = %v, want ErrNoAddress", err)
	}
	if s := p.Status()[0]; s.InFlight != 0 {
		t.Fatalf("lease leaked: %+v", s)
	}
}
