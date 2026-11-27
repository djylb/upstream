package uphttp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
		Address: srv.Listener.Addr().String(),
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
	cache := uphttp.NewTransportCache(func(string) *http.Transport {
		return &http.Transport{MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute}
	}, uphttp.TransportCacheConfig{IdleTTL: 10 * time.Minute, MaxEntries: 1024})
	defer cache.CloseIdleConnections()

	// Drop a backend's connections when it leaves or changes.
	defer pool.Subscribe(func(ev upstream.Event[string]) {
		if ev.Kind == upstream.EventRemoved || ev.Kind == upstream.EventUpdated {
			cache.Remove(ev.Backend.ID())
		}
	})()

	if err := pool.Sync([]upstream.Target[string]{{ID: "a"}}); err != nil {
		fmt.Println(err)
		return
	}
	lease, err := pool.Pick()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer lease.Release(upstream.Success)
	t := cache.Get(lease.Backend().ID())
	fmt.Println(t == cache.Get("a"))
	// Output: true
}

func ExampleParseStatusRange() {
	r, _ := uphttp.ParseStatusRange("2xx")
	fmt.Println(r, r.Contains(204), r.Contains(301))
	// Output: 200-299 true false
}

func ExampleRoundTripper() {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello from " + r.Host))
	}))
	defer backend.Close()

	pool := upstream.New[string](upstream.Config{
		Balancer: upstream.ConsistentHash(upstream.HashConfig{LoadFactor: 1.25}),
		Outlier:  upstream.OutlierConfig{ConsecutiveFailures: 5},
		Retry:    upstream.RetryConfig{Attempts: 2, Budget: 0.2},
	})
	defer pool.Close()
	if err := pool.Sync([]upstream.Target[string]{{ID: "web-1", Value: backend.Listener.Addr().String()}}); err != nil {
		fmt.Println(err)
		return
	}

	cache := uphttp.NewTransportCache(func(string) *http.Transport {
		return &http.Transport{IdleConnTimeout: time.Minute}
	}, uphttp.TransportCacheConfig{IdleTTL: 10 * time.Minute})
	defer cache.CloseIdleConnections()

	client := &http.Client{Transport: &uphttp.RoundTripper[string]{
		Pool:      pool,
		Transport: func(b *upstream.Backend[string]) http.RoundTripper { return cache.Get(b.ID()) },
		Key: func(r *http.Request) string { // sticky sessions
			if c, err := r.Cookie("session"); err == nil {
				return c.Value
			}
			return ""
		},
	}}
	resp, err := client.Get("http://shop.internal/")
	if err != nil {
		fmt.Println(err)
		return
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
	// Output: hello from shop.internal
}
