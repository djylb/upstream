package uphttp

import (
	"net/http"
	"testing"

	"github.com/djylb/upstream"
)

func TestReviewTransportPanicLeaksLease(t *testing.T) {
	p := newRTPool(t, upstream.Config{}, "a")
	rt := &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(*http.Request) (*http.Response, error) { panic("boom") })
	}}
	req, _ := http.NewRequest(http.MethodGet, "http://x/", http.NoBody)
	func() {
		defer func() { _ = recover() }()
		_, _ = rt.RoundTrip(req)
	}()
	if n := p.Status()[0].InFlight; n != 0 {
		t.Fatalf("in flight %d after a panicking transport", n)
	}
}
