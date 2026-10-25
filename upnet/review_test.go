package upnet

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/djylb/upstream"
)

func TestReviewBackendCloseRace(t *testing.T) {
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

func TestReviewDialPanicLeaksLease(t *testing.T) {
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
