package uphttp_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/health"
	"github.com/djylb/upstream/uphttp"
)

func ExampleProbe() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && r.Host == "api.internal" {
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// Use it as the Probe of a health.Target; the check runs only for
	// targets that name it.
	target := health.Target{
		Address: strings.TrimPrefix(srv.URL, "http://"),
		Probe: uphttp.Probe(uphttp.Check{
			Path:       "/healthz",
			Host:       "api.internal",
			Status:     uphttp.StatusRange{Min: 200, Max: 299},
			ExpectBody: "ok",
		}),
	}
	_, err := target.Probe(context.Background(), target)
	fmt.Println(err)

	target.Probe = uphttp.Probe(uphttp.Check{Path: "/other"})
	_, err = target.Probe(context.Background(), target)
	fmt.Println(err, health.Classify(err))
	// Output:
	// <nil>
	// uphttp: unexpected status 503 response
}

func ExampleTransportCache() {
	pool := upstream.New[string](upstream.Config{})
	defer pool.Close()
	cache := uphttp.NewTransportCache(func(id string) *http.Transport {
		return &http.Transport{MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute}
	}, uphttp.TransportCacheConfig{IdleTTL: 10 * time.Minute, MaxEntries: 1024})
	defer cache.CloseIdleConnections()

	// Drop a backend's connections when it leaves the pool.
	defer pool.Subscribe(func(ev upstream.Event[string]) {
		if ev.Kind == upstream.EventRemoved {
			cache.Remove(ev.Backend.ID())
		}
	})()

	_ = pool.Update([]upstream.Target[string]{{ID: "a", Value: "10.0.0.1:80"}})
	lease, _ := pool.Pick()
	t := cache.Get(lease.Backend().ID())
	fmt.Println(t == cache.Get("a"))
	lease.Release(upstream.Success)
	// Output: true
}

func ExampleParseStatusRange() {
	r, _ := uphttp.ParseStatusRange("2xx")
	fmt.Println(r, r.Contains(204), r.Contains(301))
	// Output: 200-299 true false
}
