package uphttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
)

// fakeBackends answers requests in memory: each backend address maps to a
// status code, and "down" fails to connect.
type fakeBackends struct {
	mu     sync.Mutex
	status map[string]int
	seen   []string // address and body of every request
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (f *fakeBackends) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, r.URL.Host+" "+string(body))
	if r.URL.Host == "down" {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	code := f.status[r.URL.Host]
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("from " + r.URL.Host)), Request: r}, nil
}

func (f *fakeBackends) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seen
	f.seen = nil
	return s
}

func newRTPool(t *testing.T, cfg upstream.Config, values ...string) *upstream.Pool[string] {
	t.Helper()
	p := upstream.New[string](cfg)
	t.Cleanup(p.Close)
	ts := make([]upstream.Target[string], len(values))
	for i, v := range values {
		ts[i] = upstream.Target[string]{ID: v, Value: v}
	}
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRoundTripperRetries(t *testing.T) {
	fb := &fakeBackends{status: map[string]int{"busy": 503, "ok": 200}}
	p := newRTPool(t, upstream.Config{Balancer: upstream.Failover(), Retry: upstream.RetryConfig{Attempts: 3}}, "busy", "ok")
	client := &http.Client{Transport: &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper { return fb }}}

	resp, err := client.Get("http://service/path")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := fb.requests(); len(got) != 2 || got[0] != "busy " || got[1] != "ok " {
		t.Fatalf("requests %q", got)
	}
	st := p.Status()
	if st[0].Failures != 1 || st[0].InFlight != 0 || st[1].InFlight != 1 || st[1].Successes != 1 {
		t.Fatalf("status while the body is open %+v", st)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "from ok" || p.Status()[1].InFlight != 0 {
		t.Fatalf("body %q, in flight %d after EOF", b, p.Status()[1].InFlight)
	}
	_ = resp.Body.Close()

	// A POST that reached the backend is not sent again.
	resp, err = client.Post("http://service/", "text/plain", strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || len(fb.requests()) != 1 {
		t.Fatalf("POST: status %d", resp.StatusCode)
	}

	// A POST that never reached a backend is, with its body again.
	q := newRTPool(t, upstream.Config{Balancer: upstream.Failover(), Retry: upstream.RetryConfig{Attempts: 3}}, "down", "ok")
	client.Transport = &RoundTripper[string]{Pool: q, Transport: func(*upstream.Backend[string]) http.RoundTripper { return fb }}
	resp, err = client.Post("http://service/", "text/plain", strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := fb.requests(); resp.StatusCode != 200 || len(got) != 2 || got[1] != "ok data" {
		t.Fatalf("POST after a refused dial: %d, %q", resp.StatusCode, got)
	}

	// A body that cannot be read again is not retried.
	req, _ := http.NewRequest(http.MethodPut, "http://service/", io.NopCloser(strings.NewReader("once")))
	if _, err := client.Do(req); err == nil || len(fb.requests()) != 1 {
		t.Fatalf("PUT without GetBody: %v", err)
	}

	// Without retries the failed response comes back.
	r := newRTPool(t, upstream.Config{}, "busy")
	client.Transport = &RoundTripper[string]{Pool: r, Transport: func(*upstream.Backend[string]) http.RoundTripper { return fb }}
	resp, err = client.Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || r.Status()[0].Failures != 1 || r.Status()[0].InFlight != 0 {
		t.Fatalf("status %d, %+v", resp.StatusCode, r.Status()[0])
	}
}

func TestRoundTripperRealServers(t *testing.T) {
	var hits [2]int
	var mu sync.Mutex
	srvs := make([]*httptest.Server, 2)
	for i := range srvs {
		srvs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			hits[i]++
			mu.Unlock()
			_, _ = io.WriteString(w, r.Host)
		}))
		t.Cleanup(srvs[i].Close)
	}
	p := upstream.New[*url.URL](upstream.Config{Balancer: upstream.ConsistentHash(upstream.HashConfig{})})
	defer p.Close()
	for i, s := range srvs {
		u, _ := url.Parse(s.URL)
		if err := p.Put(upstream.Target[*url.URL]{ID: string(rune('a' + i)), Value: u}); err != nil {
			t.Fatal(err)
		}
	}
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: &RoundTripper[*url.URL]{
		Pool:      p,
		Transport: func(*upstream.Backend[*url.URL]) http.RoundTripper { return tr },
		Key:       func(r *http.Request) string { return r.Header.Get("Session") },
	}}
	for range 10 {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://public.example/", http.NoBody)
		req.Header.Set("Session", "s-1")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(b) != "public.example" {
			t.Fatalf("Host %q", b)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if hits[0] != 10 && hits[1] != 10 {
		t.Fatalf("one session spread over backends: %v", hits)
	}
}

func TestRoundTripperErrors(t *testing.T) {
	p := upstream.New[int](upstream.Config{})
	defer p.Close()
	_ = p.Sync([]upstream.Target[int]{{ID: "a", Value: 1}})
	req, _ := http.NewRequest(http.MethodGet, "http://x/", http.NoBody)
	if _, err := (&RoundTripper[int]{Pool: p}).RoundTrip(req); !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("RoundTrip = %v", err)
	}
	empty := newRTPool(t, upstream.Config{})
	if _, err := (&RoundTripper[string]{Pool: empty}).RoundTrip(req); !errors.Is(err, upstream.ErrNoBackends) {
		t.Fatalf("RoundTrip on an empty pool = %v", err)
	}
	// A canceled request is neutral and not retried.
	q := newRTPool(t, upstream.Config{Retry: upstream.RetryConfig{Attempts: 3}}, "a", "b")
	calls := 0
	rt := &RoundTripper[string]{Pool: q, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, context.Canceled })
	}}
	if _, err := rt.RoundTrip(req); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("RoundTrip = %v after %d calls", err, calls)
	}
	for _, s := range q.Status() {
		if s.Failures != 0 || s.InFlight != 0 {
			t.Fatalf("status %+v", s)
		}
	}
}

// rwBody is the body of an upgraded connection.
type rwBody struct {
	io.Reader
	written strings.Builder
	closed  bool
}

func (b *rwBody) Write(p []byte) (int, error) { return b.written.Write(p) }

func (b *rwBody) Close() error { b.closed = true; return nil }

func TestRoundTripperUpgrade(t *testing.T) {
	p := newRTPool(t, upstream.Config{}, "ws")
	body := &rwBody{Reader: strings.NewReader("")}
	rt := &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusSwitchingProtocols, Body: body}, nil
		})
	}}
	req, _ := http.NewRequest(http.MethodGet, "http://x/", http.NoBody)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	rw, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatal("the upgraded body is not writable")
	}
	// The backend's direction ends first; the client may still write.
	if n, err := rw.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read %d, %v", n, err)
	}
	if p.Status()[0].InFlight != 1 {
		t.Fatal("the end of the backend's stream released the upgraded connection")
	}
	if _, err := rw.Write([]byte("frame")); err != nil || body.written.String() != "frame" {
		t.Fatal("write did not reach the connection")
	}
	if p.Status()[0].InFlight != 1 {
		t.Fatal("upgraded connection holds no lease")
	}
	_ = rw.Close()
	_ = rw.Close()
	if !body.closed || p.Status()[0].InFlight != 0 {
		t.Fatal("close did not release the lease")
	}
}

func TestRoundTripperPanicsRelease(t *testing.T) {
	ok := rtFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("x")), Request: r}, nil
	})
	for name, rt := range map[string]*RoundTripper[string]{
		"Rewrite":   {Rewrite: func(*http.Request, *upstream.Backend[string]) { panic("boom") }},
		"Transport": {Transport: func(*upstream.Backend[string]) http.RoundTripper { panic("boom") }},
		"Outcome": {
			Transport: func(*upstream.Backend[string]) http.RoundTripper { return ok },
			Outcome:   func(*http.Response, error) upstream.Outcome { panic("boom") },
		},
	} {
		p := newRTPool(t, upstream.Config{}, "a")
		rt.Pool = p
		req, _ := http.NewRequest(http.MethodGet, "http://x/", http.NoBody)
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: the panic did not go on", name)
				}
			}()
			_, _ = rt.RoundTrip(req)
		}()
		if s := p.Status()[0]; s.InFlight != 0 || s.Failures != 1 {
			t.Errorf("%s: status %+v", name, s)
		}
	}
}

func TestTransportPanicReleasesLease(t *testing.T) {
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

// TestRoundTripperRetryAndOutcomeApart checks that Retry decides retries
// apart from the Outcome that counts for health: a 503 is retried without
// counting against its backend.
func TestRoundTripperRetryAndOutcomeApart(t *testing.T) {
	fb := &fakeBackends{status: map[string]int{"busy": 503, "ok": 200}}
	p := newRTPool(t, upstream.Config{Balancer: upstream.Failover(), Retry: upstream.RetryConfig{Attempts: 2}}, "busy", "ok")
	rt := &RoundTripper[string]{
		Pool:      p,
		Transport: func(*upstream.Backend[string]) http.RoundTripper { return fb },
		Outcome: func(resp *http.Response, err error) upstream.Outcome {
			if err == nil && resp.StatusCode == 503 {
				return upstream.Neutral
			}
			return upstream.OutcomeOf(err)
		},
		Retry: func(resp *http.Response, err error) bool { return err != nil || resp.StatusCode == 503 },
	}
	resp, err := (&http.Client{Transport: rt}).Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if s := p.Status()[0]; s.Neutrals != 1 || s.Failures != 0 {
		t.Fatalf("busy: %+v", s)
	}
	if s := p.Status()[1]; s.Latency <= 0 {
		t.Fatal("latency not observed")
	}

	// Without a retry left, the last response is returned.
	p2 := newRTPool(t, upstream.Config{}, "busy")
	rt.Pool = p2
	resp, err = (&http.Client{Transport: rt}).Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || p2.Status()[0].InFlight != 0 {
		t.Fatalf("status %d, %+v", resp.StatusCode, p2.Status()[0])
	}
}

// TestRoundTripperAttemptTimeout checks that a backend that does not
// answer costs one attempt, not the request's whole deadline.
func TestRoundTripperAttemptTimeout(t *testing.T) {
	p := newRTPool(t, upstream.Config{Balancer: upstream.Failover(), Retry: upstream.RetryConfig{Attempts: 2}}, "hole", "ok")
	tr := rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "hole" {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("attempt context has no deadline")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	})
	rt := &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper { return tr }, AttemptTimeout: 10 * time.Millisecond}
	resp, err := (&http.Client{Transport: rt}).Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
	if s := p.Status()[0]; s.Failures != 1 || s.InFlight != 0 {
		t.Fatalf("hole: %+v", s)
	}
	rt.Pool = newRTPool(t, upstream.Config{}, "hole")
	if _, err := (&http.Client{Transport: rt}).Get("http://service/"); !errors.Is(err, ErrAttemptTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v", err)
	}
}

// TestRewriteURLJoinsPath checks the default Rewrite for *url.URL values.
func TestRewriteURLJoinsPath(t *testing.T) {
	for _, tc := range []struct{ target, in, want string }{
		{"http://b:1", "http://svc/x?q=1", "http://b:1/x?q=1"},
		{"https://b:1/api", "http://svc/x", "https://b:1/api/x"},
		{"http://b:1/api/", "http://svc/x?q=1", "http://b:1/api/x?q=1"},
		{"http://b:1/api?k=v", "http://svc/x?q=1", "http://b:1/api/x?k=v&q=1"},
		{"http://b:1/a%2Fb", "http://svc/x", "http://b:1/a%2Fb/x"},
	} {
		target, _ := url.Parse(tc.target)
		u, _ := url.Parse(tc.in)
		rewriteURL(u, target)
		if u.String() != tc.want {
			t.Errorf("%s + %s = %s, want %s", tc.target, tc.in, u, tc.want)
		}
	}
}

// TestRoundTripperLatencyUsesPoolClock checks that the latency to the
// response headers comes from the pool's Clock.
func TestRoundTripperLatencyUsesPoolClock(t *testing.T) {
	clk := fakeclock.New()
	p := newRTPool(t, upstream.Config{Clock: clk}, "a")
	rt := &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(r *http.Request) (*http.Response, error) {
			clk.Advance(9 * time.Millisecond)
			return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
		})
	}}
	resp, err := (&http.Client{Transport: rt}).Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := p.Status()[0].Latency; got != 9*time.Millisecond {
		t.Fatalf("latency %v, want 9ms", got)
	}
}

// TestRoundTripperNilAddress checks that a backend without an address
// fails with ErrNoAddress instead of sending the request unrewritten.
func TestRoundTripperNilAddress(t *testing.T) {
	send := func(*upstream.Backend[*url.URL]) http.RoundTripper {
		return rtFunc(func(*http.Request) (*http.Response, error) {
			t.Error("sent a request to a backend without an address")
			return nil, errors.New("sent")
		})
	}
	p := upstream.New[*url.URL](upstream.Config{})
	defer p.Close()
	_ = p.Put(upstream.Target[*url.URL]{ID: "a"})
	_, err := (&http.Client{Transport: &RoundTripper[*url.URL]{Pool: p, Transport: send}}).Get("http://service/")
	if !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Get = %v, want ErrNoAddress", err)
	}
	q := upstream.New[upstream.Addresser](upstream.Config{})
	defer q.Close()
	_ = q.Put(upstream.Target[upstream.Addresser]{ID: "a"})
	sendA := func(*upstream.Backend[upstream.Addresser]) http.RoundTripper { return send(nil) }
	_, err = (&http.Client{Transport: &RoundTripper[upstream.Addresser]{Pool: q, Transport: sendA}}).Get("http://service/")
	if !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Get = %v, want ErrNoAddress", err)
	}
	if p.Status()[0].InFlight != 0 || q.Status()[0].InFlight != 0 {
		t.Fatal("lease leaked")
	}
}

// slowBody is a response body that advances clk by step on each Read and
// fails if its request's context has ended.
type slowBody struct {
	ctx   context.Context
	clk   *fakeclock.Clock
	step  time.Duration
	reads int
}

func (b *slowBody) Read(p []byte) (int, error) {
	if b.reads == 0 {
		return 0, io.EOF
	}
	b.reads--
	b.clk.Advance(b.step)
	if d, ok := b.ctx.Deadline(); ok && !b.clk.Now().Before(d) {
		<-b.ctx.Done() // the fake timer fires in its own goroutine
	}
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	p[0] = 'x'
	return 1, nil
}

func (b *slowBody) Close() error { return nil }

// TestRoundTripperHeaderTimeout checks that late headers fail the
// attempt with ErrHeaderTimeout, that it is retried, that the attempt's
// context has no deadline, and that the timer does not outlive the attempt.
func TestRoundTripperHeaderTimeout(t *testing.T) {
	clk := fakeclock.New()
	p := newRTPool(t, upstream.Config{Clock: clk, Balancer: upstream.Failover(), Retry: upstream.RetryConfig{Attempts: 2}}, "hole", "ok")
	tr := rtFunc(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); ok {
			t.Error("attempt context has a deadline")
		}
		if r.URL.Host == "hole" {
			clk.Advance(10 * time.Millisecond)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	})
	rt := &RoundTripper[string]{Pool: p, Transport: func(*upstream.Backend[string]) http.RoundTripper { return tr }, HeaderTimeout: 10 * time.Millisecond}
	resp, err := (&http.Client{Transport: rt}).Get("http://service/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "ok" || clk.Pending() != 0 {
		t.Fatalf("body %q, %d timers pending", body, clk.Pending())
	}
	if s := p.Status()[0]; s.Failures != 1 || s.InFlight != 0 {
		t.Fatalf("hole: %+v", s)
	}
	rt.Pool = newRTPool(t, upstream.Config{Clock: clk}, "hole")
	if _, err := (&http.Client{Transport: rt}).Get("http://service/"); !errors.Is(err, ErrHeaderTimeout) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrAttemptTimeout) {
		t.Fatalf("error %v", err)
	}
}

// TestRoundTripperHeaderTimeoutLoses checks the race: a response that the
// transport returns after the timer fired is closed and the attempt fails.
func TestRoundTripperHeaderTimeoutLoses(t *testing.T) {
	clk := fakeclock.New()
	p := newRTPool(t, upstream.Config{Clock: clk}, "a")
	closed := false
	rt := &RoundTripper[string]{Pool: p, HeaderTimeout: time.Millisecond, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(r *http.Request) (*http.Response, error) {
			clk.Advance(time.Millisecond)
			<-r.Context().Done() // fired, but answer anyway
			return &http.Response{StatusCode: 200, Body: closeFunc(func() { closed = true }), Request: r}, nil
		})
	}}
	if _, err := (&http.Client{Transport: rt}).Get("http://service/"); !errors.Is(err, ErrHeaderTimeout) || !closed {
		t.Fatalf("error %v, closed %v", err, closed)
	}
	if s := p.Status()[0]; s.InFlight != 0 {
		t.Fatalf("%+v", s)
	}
}

type closeFunc func()

func (closeFunc) Read([]byte) (int, error) { return 0, io.EOF }
func (f closeFunc) Close() error           { f(); return nil }

// TestRoundTripperHeaderTimeoutStreams checks that HeaderTimeout does not
// cut a body that streams for longer than it, and that combined with
// AttemptTimeout the attempt deadline still ends the body.
func TestRoundTripperHeaderTimeoutStreams(t *testing.T) {
	for _, tc := range []struct {
		attempt time.Duration
		wantErr error
	}{{0, nil}, {35 * time.Millisecond, context.DeadlineExceeded}} {
		clk := fakeclock.New()
		p := newRTPool(t, upstream.Config{Clock: clk}, "a")
		rt := &RoundTripper[string]{Pool: p, HeaderTimeout: 10 * time.Millisecond, AttemptTimeout: tc.attempt, Transport: func(*upstream.Backend[string]) http.RoundTripper {
			return rtFunc(func(r *http.Request) (*http.Response, error) {
				clk.Advance(5 * time.Millisecond)
				return &http.Response{StatusCode: 200, Body: &slowBody{ctx: r.Context(), clk: clk, step: 10 * time.Millisecond, reads: 5}, Request: r}, nil
			})
		}}
		resp, err := (&http.Client{Transport: rt}).Get("http://service/")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if tc.wantErr == nil && (err != nil || string(body) != "xxxxx") {
			t.Fatalf("body %q, error %v", body, err)
		}
		if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
			t.Fatalf("attempt %v: error %v", tc.attempt, err)
		}
		if clk.Pending() != 0 || p.Status()[0].InFlight != 0 {
			t.Fatalf("%d timers pending, %+v", clk.Pending(), p.Status()[0])
		}
	}
	// Combined, late headers: the header timeout wins over the longer
	// attempt timeout.
	clk := fakeclock.New()
	p := newRTPool(t, upstream.Config{Clock: clk}, "a")
	rt := &RoundTripper[string]{Pool: p, HeaderTimeout: 10 * time.Millisecond, AttemptTimeout: time.Second, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(r *http.Request) (*http.Response, error) {
			clk.Advance(10 * time.Millisecond)
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
	}}
	if _, err := (&http.Client{Transport: rt}).Get("http://service/"); !errors.Is(err, ErrHeaderTimeout) || clk.Pending() != 0 {
		t.Fatalf("error %v, %d pending", err, clk.Pending())
	}
}

func TestRoundTripperGetBodyErrorStopsTimer(t *testing.T) {
	clk := fakeclock.New()
	p := newRTPool(t, upstream.Config{Clock: clk, Retry: upstream.RetryConfig{Attempts: 3}}, "a", "b")
	rt := &RoundTripper[string]{Pool: p, AttemptTimeout: time.Hour, Transport: func(*upstream.Backend[string]) http.RoundTripper {
		return rtFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 503, Body: http.NoBody, Request: r}, nil
		})
	}}
	req, _ := http.NewRequest("PUT", "http://service/", strings.NewReader("x"))
	req.Header.Set("Idempotency-Key", "1")
	req.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("no body") }
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("RoundTrip succeeded although GetBody failed")
	}
	if clk.Pending() != 0 {
		t.Fatalf("leaked %d timers", clk.Pending())
	}
}
