package uphttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/djylb/upstream"
)

// ErrAttemptTimeout is the error of an attempt that RoundTripper's
// AttemptTimeout cut short. It wraps context.DeadlineExceeded, so the
// attempt counts as a Failure of its backend.
var ErrAttemptTimeout = fmt.Errorf("uphttp: attempt timed out: %w", context.DeadlineExceeded)

// ErrHeaderTimeout is the error of an attempt whose response headers did
// not arrive within RoundTripper's HeaderTimeout. It wraps
// context.DeadlineExceeded, so the attempt counts as a Failure of its
// backend.
var ErrHeaderTimeout = fmt.Errorf("uphttp: timed out waiting for response headers: %w", context.DeadlineExceeded)

// A RoundTripper sends each request to a backend picked from Pool, so that
// an http.Client or a reverse proxy balances requests over the pool. It
// retries a request that failed on another backend, as Retry and the
// pool's upstream.RetryConfig allow, when the request can be sent again:
// its body is empty or has GetBody, and either its method is idempotent
// (GET, HEAD, OPTIONS, TRACE, or any method with an Idempotency-Key
// header) or the connection to the failed backend was never established.
// When no retry is allowed, the last response or error is returned.
//
// The lease on the backend lasts until the response body is read to the
// end or closed, or, for an upgraded connection (status 101), until it is
// closed, so in-flight counts and drains cover streaming responses and
// upgraded connections; the outcome is reported, and the time to the
// response headers, by the pool's Clock, observed as the backend's
// latency (see upstream.Lease.Observe), as soon as the headers arrive. A panic in
// Rewrite, Transport, the round trip, Outcome or Retry releases the
// backend's lease and goes on. The fields must not change while requests
// are in flight.
type RoundTripper[T any] struct {
	// Pool holds the backends.
	Pool *upstream.Pool[T]

	// Rewrite points the outgoing request, a clone of the incoming one, at
	// backend b, usually by setting r.URL.Scheme and r.URL.Host. Nil
	// handles two kinds of pools. For *url.URL values it sets the scheme
	// and host and joins the URL's path and query before the request's,
	// as a reverse proxy to that URL does. For values with an address, as
	// upstream.AddressOf finds it, it sets the host to the address; the
	// scheme stays, or becomes "http" if empty. Other pools need a
	// Rewrite, or RoundTrip fails with upstream.ErrNoAddress; a backend
	// whose value has none, such as a nil *url.URL or a nil Addresser,
	// fails its attempt with it, as a Failure.
	//
	// The request's Host field is kept, so the backend sees the Host
	// header the client sent; set r.Host to send the backend's own. With
	// the scheme "https", the transport takes the TLS server name from the
	// URL's host, the backend address, and verifies the backend's
	// certificate against it, unless its TLSClientConfig sets ServerName.
	Rewrite func(r *http.Request, b *upstream.Backend[T])

	// Transport returns the round tripper for backend b, such as one from
	// a TransportCache. Nil means http.DefaultTransport.
	Transport func(b *upstream.Backend[T]) http.RoundTripper

	// Key returns the request's affinity key for the pool's balancer, such
	// as a session cookie or the client address; an empty key carries no
	// affinity. Nil means no key.
	Key func(r *http.Request) string

	// Outcome judges a response or error for passive health checking.
	// Nil means upstream.OutcomeOf for errors, Failure for status codes
	// from 500 and Success otherwise.
	Outcome func(resp *http.Response, err error) upstream.Outcome

	// Retry reports whether a response or error should be retried on
	// another backend, if the request can be sent again. It decides apart
	// from Outcome, so that, for example, a 503 from an overloaded backend
	// is retried without counting against its health. Nil retries what
	// the Outcome judges a Failure.
	Retry func(resp *http.Response, err error) bool

	// AttemptTimeout, if positive, bounds each attempt, from sending the
	// request until its response body ends or is closed. The attempt's
	// context comes from the pool's Clock.WithTimeout, so the transport,
	// its dialer and the TLS handshake see the deadline. An attempt that
	// times out before the response headers fails with ErrAttemptTimeout
	// and may be retried, so that a backend that does not answer cannot use
	// up the whole request's deadline; a body still being read when it
	// expires fails with context.DeadlineExceeded. Leave it zero for
	// streaming responses and use HeaderTimeout instead.
	AttemptTimeout time.Duration

	// HeaderTimeout, if positive, bounds only the wait for an attempt's
	// response headers, from sending the request until the transport
	// returns the response; reading the body is not bounded, so it suits
	// streaming responses. It sets no deadline on the attempt's context:
	// a timer of the pool's Clock cancels the context if the headers have
	// not arrived, and stops once they have. Headers count only if they
	// arrive before the timer fires; once it has fired, a response the
	// transport still returns is closed and the attempt fails with
	// ErrHeaderTimeout, which may be retried like ErrAttemptTimeout. With
	// both set, whichever expires first ends the attempt.
	HeaderTimeout time.Duration
}

// RoundTrip implements http.RoundTripper.
func (rt *RoundTripper[T]) RoundTrip(req *http.Request) (*http.Response, error) {
	rewrite := func(r *http.Request, b *upstream.Backend[T]) error { rt.Rewrite(r, b); return nil }
	if rt.Rewrite == nil {
		if rewrite = defaultRewrite[T](); rewrite == nil {
			closeBody(req)
			return nil, upstream.ErrNoAddress
		}
	}
	key := ""
	if rt.Key != nil {
		key = rt.Key(req)
	}
	replayable := req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
	var last attempt[T] // the last attempt's response, kept with its lease
	r := rt.Pool.Request(key)
	err := r.Do(req.Context(), func(l *upstream.Lease[T]) (bool, error) {
		last.discard()
		a := rt.try(req, l, rewrite, r.Tried() > 1)
		retry := replayable && (idempotent(req) || notSent(a.err)) && rt.retry(a)
		if a.err != nil {
			l.Release(a.o)
			return retry, a.err
		}
		a.lease = l.Take()
		last = a
		return retry, nil
	})
	if err != nil && last.resp != nil {
		// The last response asked for a retry that could not be made.
		return last.finish(), nil
	}
	if err != nil {
		if r.Tried() == 0 {
			closeBody(req)
		}
		return nil, err
	}
	return last.finish(), nil
}

// An attempt is one round trip to a backend.
type attempt[T any] struct {
	resp   *http.Response
	err    error
	cancel context.CancelFunc // ends the attempt's context, or nil
	lease  upstream.Lease[T]  // set for a response
	o      upstream.Outcome
}

// discard drops the response of an attempt that is retried.
func (a *attempt[T]) discard() {
	if a.resp != nil && a.resp.Body != nil {
		discard(a.resp.Body)
	}
	a.lease.Release(a.o)
	if a.cancel != nil {
		a.cancel()
	}
	*a = attempt[T]{}
}

// try sends one attempt of req to the backend of l. If Rewrite, Transport,
// the round trip or Outcome panics, it closes the response if there is
// one and lets the panic go on; the caller releases l.
func (rt *RoundTripper[T]) try(req *http.Request, l *upstream.Lease[T], rewrite func(*http.Request, *upstream.Backend[T]) error, again bool) (a attempt[T]) {
	ctx := req.Context()
	clock := rt.Pool.Clock()
	if rt.AttemptTimeout > 0 {
		ctx, a.cancel = clock.WithTimeout(ctx, rt.AttemptTimeout)
	}
	var hdr *headerTimer
	if rt.HeaderTimeout > 0 {
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		hdr = &headerTimer{cancel: cancel}
		outer := a.cancel
		a.cancel = func() {
			cancel(context.Canceled)
			if outer != nil {
				outer()
			}
		}
		hdr.stop = clock.AfterFunc(rt.HeaderTimeout, hdr.fire)
	}
	done := false
	defer func() {
		if hdr != nil {
			hdr.headers()
		}
		if !done {
			if a.resp != nil && a.resp.Body != nil {
				_ = a.resp.Body.Close()
			}
			if a.cancel != nil {
				a.cancel()
			}
		}
	}()
	out := req.Clone(ctx)
	if again && req.GetBody != nil {
		var err error
		if out.Body, err = req.GetBody(); err != nil {
			a.err, a.o = err, upstream.Neutral
			return a
		}
	}
	b := l.Backend()
	if err := rewrite(out, b); err != nil {
		a.err, a.o = err, upstream.Failure
		return a
	}
	start := clock.Now()
	a.resp, a.err = rt.transport(b).RoundTrip(out)
	if hdr != nil && !hdr.headers() && req.Context().Err() == nil && errors.Is(context.Cause(ctx), ErrHeaderTimeout) {
		if a.resp != nil && a.resp.Body != nil {
			_ = a.resp.Body.Close()
		}
		a.resp, a.err = nil, ErrHeaderTimeout
	}
	if a.err != nil && !errors.Is(a.err, ErrHeaderTimeout) && a.cancel != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && req.Context().Err() == nil {
		a.err = ErrAttemptTimeout
	}
	if a.err == nil {
		l.Observe(clock.Now().Sub(start))
	}
	a.o = rt.outcome(a.resp, a.err)
	if a.err != nil && a.cancel != nil {
		a.cancel()
		a.cancel = nil
	}
	done = true
	return a
}

// A headerTimer cancels an attempt whose response headers are late. Its
// state decides the race between the timer and the headers: whichever of
// fire and headers runs first wins.
type headerTimer struct {
	cancel context.CancelCauseFunc
	stop   func() bool
	state  atomic.Bool // set by the first of fire and headers
}

func (h *headerTimer) fire() {
	if h.state.CompareAndSwap(false, true) {
		h.cancel(ErrHeaderTimeout)
	}
}

// headers marks the response headers as arrived and stops the timer. It
// reports whether they won the race; later calls report false.
func (h *headerTimer) headers() bool {
	if h.state.CompareAndSwap(false, true) {
		h.stop()
		return true
	}
	return false
}

func (rt *RoundTripper[T]) transport(b *upstream.Backend[T]) http.RoundTripper {
	if rt.Transport != nil {
		return rt.Transport(b)
	}
	return http.DefaultTransport
}

func (rt *RoundTripper[T]) outcome(resp *http.Response, err error) upstream.Outcome {
	if rt.Outcome != nil {
		return rt.Outcome(resp, err)
	}
	if err != nil {
		return upstream.OutcomeOf(err)
	}
	if resp.StatusCode >= 500 {
		return upstream.Failure
	}
	return upstream.Success
}

func (rt *RoundTripper[T]) retry(a attempt[T]) bool {
	if rt.Retry != nil {
		return rt.Retry(a.resp, a.err)
	}
	return a.o == upstream.Failure
}

// defaultRewrite returns the rewrite for a nil Rewrite, or nil if the
// pool's values have no address. The rewrite fails with
// upstream.ErrNoAddress for a value without one, such as a nil *url.URL
// or a nil Addresser.
func defaultRewrite[T any]() func(*http.Request, *upstream.Backend[T]) error {
	var zero T
	if _, ok := any(zero).(*url.URL); ok {
		return func(r *http.Request, b *upstream.Backend[T]) error {
			u, _ := any(b.Value()).(*url.URL)
			if u == nil {
				return upstream.ErrNoAddress
			}
			rewriteURL(r.URL, u)
			return nil
		}
	}
	if !upstream.Addressable[T]() {
		return nil
	}
	return func(r *http.Request, b *upstream.Backend[T]) error {
		addr, ok := upstream.AddressOf(b.Value())
		if !ok {
			return upstream.ErrNoAddress
		}
		r.URL.Host = addr
		if r.URL.Scheme == "" {
			r.URL.Scheme = "http"
		}
		return nil
	}
}

// rewriteURL points u at target: its scheme and host, its path joined
// before u's with one slash between them, and its query before u's.
func rewriteURL(u, target *url.URL) {
	u.Scheme, u.Host = target.Scheme, target.Host
	u.Path, u.RawPath = joinURLPath(target, u)
	switch {
	case target.RawQuery == "":
	case u.RawQuery == "":
		u.RawQuery = target.RawQuery
	default:
		u.RawQuery = target.RawQuery + "&" + u.RawQuery
	}
}

func joinURLPath(a, b *url.URL) (path, rawpath string) {
	if a.RawPath == "" && b.RawPath == "" {
		return singleJoiningSlash(a.Path, b.Path), ""
	}
	apath, bpath := a.EscapedPath(), b.EscapedPath()
	aslash, bslash := strings.HasSuffix(apath, "/"), strings.HasPrefix(bpath, "/")
	switch {
	case aslash && bslash:
		return a.Path + b.Path[1:], apath + bpath[1:]
	case !aslash && !bslash:
		return a.Path + "/" + b.Path, apath + "/" + bpath
	}
	return a.Path + b.Path, apath + bpath
}

func singleJoiningSlash(a, b string) string {
	aslash, bslash := strings.HasSuffix(a, "/"), strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// idempotent reports whether req may be sent twice, by the rules
// net/http applies to its own retries.
func idempotent(req *http.Request) bool {
	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	_, a := req.Header["Idempotency-Key"]
	_, b := req.Header["X-Idempotency-Key"]
	return a || b
}

// notSent reports whether err means that no connection to the backend was
// established, so the request never reached it.
func notSent(err error) bool {
	if errors.Is(err, upstream.ErrNoAddress) {
		return true
	}
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

func closeBody(req *http.Request) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
}

// discard reads a little of a response that is not used, so that its
// connection can be reused, and closes it.
func discard(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4<<10))
	_ = body.Close()
}

// finish hands the response of the last attempt to the caller: it
// reports the outcome and keeps the lease until the body ends.
func (a *attempt[T]) finish() *http.Response {
	resp := a.resp
	if resp.Body == nil {
		a.lease.Release(a.o)
		if a.cancel != nil {
			a.cancel()
		}
		return resp
	}
	a.lease.Report(a.o)
	body := &leaseBody[T]{ReadCloser: resp.Body, l: a.lease, cancel: a.cancel}
	if w, ok := resp.Body.(io.Writer); ok { // an upgraded connection
		resp.Body = &leaseConnBody[T]{leaseBody: body, w: w}
	} else {
		resp.Body = body
	}
	return resp
}

// leaseBody releases its lease when the body is read to the end or
// closed, whichever comes first.
type leaseBody[T any] struct {
	io.ReadCloser
	l      upstream.Lease[T]
	cancel context.CancelFunc // the attempt's context, or nil
	done   atomic.Bool
}

func (b *leaseBody[T]) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		b.release()
	}
	return n, err
}

func (b *leaseBody[T]) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

func (b *leaseBody[T]) release() {
	if b.done.CompareAndSwap(false, true) {
		b.l.Release(upstream.Neutral) // the outcome was reported
		if b.cancel != nil {
			b.cancel()
		}
	}
}

// leaseConnBody is the body of a 101 response, an upgraded connection
// that is also writable. It releases its lease on Close only: the end of
// the backend's stream says nothing about the client's direction.
type leaseConnBody[T any] struct {
	*leaseBody[T]
	w io.Writer
}

func (b *leaseConnBody[T]) Read(p []byte) (int, error) { return b.ReadCloser.Read(p) }

func (b *leaseConnBody[T]) Write(p []byte) (int, error) { return b.w.Write(p) }
