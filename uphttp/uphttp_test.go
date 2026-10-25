package uphttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/upstream/health"
	"github.com/djylb/upstream/internal/fakeclock"
)

func hostOf(t *testing.T, s *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func TestProbe(t *testing.T) {
	var mu sync.Mutex
	var last *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		last = r.Clone(context.Background())
		mu.Unlock()
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte("status: ready\n"))
		case "/fail":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(strings.Repeat("x", 1<<20)))
		case "/moved":
			http.Redirect(w, r, "/fail", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	addr := hostOf(t, srv)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		check   Check
		wantErr error
		status  int
	}{
		{name: "ok", check: Check{Path: "/ok"}},
		{name: "path without slash", check: Check{Path: "ok"}},
		{name: "default path", check: Check{}, status: 404},
		{name: "body", check: Check{Path: "/ok", ExpectBody: "ready"}},
		{name: "body mismatch", check: Check{Path: "/ok", ExpectBody: "nope"}, wantErr: health.ErrBadResponse},
		{name: "body beyond limit", check: Check{Path: "/ok", ExpectBody: "ready", MaxBody: 4}, wantErr: ErrBodyMismatch},
		{name: "status", check: Check{Path: "/fail"}, status: 503},
		{name: "status range", check: Check{Path: "/fail", Status: StatusRange{Min: 500, Max: 599}}},
		{name: "redirect not followed", check: Check{Path: "/moved"}},
		{name: "redirect outside range", check: Check{Path: "/moved", Status: StatusRange{Min: 200, Max: 299}}, status: 302},
		{name: "bad method", check: Check{Method: "BAD METHOD"}, wantErr: errAny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Probe(tt.check)(ctx, health.Target{Address: addr})
			var se *StatusError
			switch {
			case tt.status != 0:
				if !errors.As(err, &se) || se.Code != tt.status || health.Classify(err) != health.ReasonResponse {
					t.Fatalf("err = %v, want status %d", err, tt.status)
				}
			case errors.Is(tt.wantErr, errAny):
				if err == nil {
					t.Fatal("want an error")
				}
			case !errors.Is(err, tt.wantErr):
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}

	hdr := http.Header{"X-Check": {"1"}}
	c := Check{Method: http.MethodHead, Path: "/ok?full=1", Host: "svc.internal", Header: hdr}
	probe := Probe(c)
	hdr.Set("X-Check", "changed later")
	obs, err := probe(ctx, health.Target{Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	if obs.Addr != addr {
		t.Fatalf("answered by %q, want %q", obs.Addr, addr)
	}
	mu.Lock()
	defer mu.Unlock()
	if last.Method != http.MethodHead || last.Host != "svc.internal" || last.URL.RawQuery != "full=1" || last.Header.Get("X-Check") != "1" {
		t.Fatalf("request %s %s host %q header %v", last.Method, last.URL, last.Host, last.Header)
	}
}

var errAny = errors.New("any error")

func TestProbeTLS(t *testing.T) {
	if runtime.GOOS == "js" {
		t.Skip("slow on the simulated network")
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	target := health.Target{Address: hostOf(t, srv)}
	if _, err := Probe(Check{Scheme: "https", Client: srv.Client()})(ctx, target); err != nil {
		t.Fatalf("TLS check: %v", err)
	}
	// The default client does not trust the test certificate.
	if _, err := Probe(Check{Scheme: "https"})(ctx, target); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestParseStatusRange(t *testing.T) {
	good := map[string]StatusRange{
		"200":       {200, 200},
		" 2xx ":     {200, 299},
		"5XX":       {500, 599},
		"200-399":   {200, 399},
		"204 - 204": {204, 204},
		"100-999":   {100, 999},
	}
	for s, want := range good {
		got, err := ParseStatusRange(s)
		if err != nil || got != want {
			t.Errorf("ParseStatusRange(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	for _, s := range []string{"", "2", "20x", "0xx", "x2x", "099", "1000", "300-200", "200-", "-200", "2a0", "200-3xx", "+20"} {
		if _, err := ParseStatusRange(s); !errors.Is(err, ErrStatusRange) {
			t.Errorf("ParseStatusRange(%q) err = %v", s, err)
		}
	}
	if s := (StatusRange{200, 299}).String(); s != "200-299" {
		t.Errorf("String = %q", s)
	}
	if s := (StatusRange{204, 204}).String(); s != "204" {
		t.Errorf("String = %q", s)
	}
	if (&StatusError{Code: 500}).Error() != "uphttp: unexpected status 500" {
		t.Error("StatusError text")
	}
}

func FuzzParseStatusRange(f *testing.F) {
	for _, s := range []string{"200", "2xx", "200-399", " 5XX", "1-2", "999-100"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		r, err := ParseStatusRange(s)
		if err != nil {
			if !errors.Is(err, ErrStatusRange) {
				t.Fatalf("error %v does not wrap ErrStatusRange", err)
			}
			return
		}
		if r.Min < 100 || r.Max > 999 || r.Min > r.Max {
			t.Fatalf("ParseStatusRange(%q) = %v", s, r)
		}
		back, err := ParseStatusRange(r.String())
		if err != nil || back != r {
			t.Fatalf("round trip of %v: %v, %v", r, back, err)
		}
	})
}

func TestTransportCache(t *testing.T) {
	clk := fakeclock.New()
	created := map[string]int{}
	var mu sync.Mutex
	c := NewTransportCache(func(k string) *http.Transport {
		mu.Lock()
		created[k]++
		mu.Unlock()
		return &http.Transport{}
	}, TransportCacheConfig{Clock: clk, IdleTTL: time.Minute, MaxEntries: 2})

	a := c.Get("a")
	if c.Get("a") != a || created["a"] != 1 {
		t.Fatal("transport not reused")
	}
	clk.Advance(50 * time.Second)
	c.Get("b")
	clk.Advance(50 * time.Second) // a idle 100s, b 50s
	if c.Get("b") == nil || c.Len() != 1 {
		t.Fatalf("idle transport not expired: Len %d", c.Len())
	}
	if c.Get("a") == a || created["a"] != 2 {
		t.Fatal("expired transport reused")
	}

	// Capacity evicts the least recently used.
	c.Get("b")
	c.Get("c")
	if c.Len() != 2 {
		t.Fatalf("Len %d over capacity", c.Len())
	}
	c.Get("b")
	if created["b"] != 1 {
		t.Fatal("recently used transport evicted")
	}
	c.Get("a")
	if created["a"] != 3 {
		t.Fatalf("least recently used transport not evicted: %v", created)
	}

	if !c.Remove("a") || c.Remove("a") {
		t.Fatal("Remove")
	}
	c.CloseIdleConnections()
	if c.Len() != 0 {
		t.Fatal("CloseIdleConnections kept entries")
	}
	c.Get("z")
	if c.Len() != 1 {
		t.Fatal("cache unusable after CloseIdleConnections")
	}

	// Defaults: no expiry, no bound, system clock.
	d := NewTransportCache(func(string) *http.Transport { return &http.Transport{} }, TransportCacheConfig{})
	for _, k := range []string{"1", "2", "3", "4"} {
		d.Get(k)
	}
	if d.Len() != 4 {
		t.Fatalf("Len %d", d.Len())
	}
}

func TestTransportCacheRace(t *testing.T) {
	var mu sync.Mutex
	made := 0
	c := NewTransportCache(func(string) *http.Transport {
		mu.Lock()
		made++
		mu.Unlock()
		return &http.Transport{}
	}, TransportCacheConfig{MaxEntries: 3, IdleTTL: time.Millisecond})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 500 {
				k := string('a' + rune((i+w)%5))
				if c.Get(k) == nil {
					t.Error("nil transport")
					return
				}
				if i%50 == 0 {
					c.Remove(k)
				}
			}
		})
	}
	wg.Wait()
	if c.Len() > 3 {
		t.Fatalf("Len %d over capacity", c.Len())
	}
}
