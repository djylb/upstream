package uphttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/upstream/health"
)

// The default client clones http.DefaultTransport, whose Proxy is
// ProxyFromEnvironment: with HTTP_PROXY set, checks go to the proxy and an
// unreachable backend is reported healthy.
func TestDefaultClientIgnoresEnvProxy(t *testing.T) {
	var hits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	probe := Probe(Check{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 192.0.2.1 is TEST-NET-1: nothing answers there.
	_, err := probe(ctx, health.Target{Address: "192.0.2.1:9"})
	if err == nil || hits.Load() > 0 {
		t.Fatalf("check of an unreachable backend went through the proxy: err=%v proxyHits=%d", err, hits.Load())
	}
}
