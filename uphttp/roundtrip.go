package uphttp

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/djylb/upstream"
)

// ErrNoRewrite means a RoundTripper has no Rewrite function and its pool's
// values are neither strings nor *url.URL values.
var ErrNoRewrite = errors.New("uphttp: Rewrite is nil and the backend values are neither strings nor *url.URL")

// A RoundTripper sends each request to a backend picked from Pool, so that
// an http.Client or a reverse proxy balances requests over the pool. It
// retries a request that failed on another backend, as the pool's
// upstream.RetryConfig allows, when the request can be sent again: its
// body is empty or has GetBody, and either its method is idempotent (GET,
// HEAD, OPTIONS, TRACE, or any method with an Idempotency-Key header) or
// the connection to the failed backend was never established.
//
// The lease on the backend lasts until the response body is read to the
// end or closed, or, for an upgraded connection (status 101), until it is
// closed, so in-flight counts and drains cover streaming responses and
// upgraded connections; the outcome is reported as soon as the response
// headers arrive. A panic in Rewrite, Transport, the round trip or Outcome
// releases the backend's lease and goes on. The fields must not change
// while requests are in flight.
type RoundTripper[T any] struct {
	// Pool holds the backends.
	Pool *upstream.Pool[T]

	// Rewrite points the outgoing request, a clone of the incoming one, at
	// backend b, usually by setting r.URL.Scheme and r.URL.Host. Nil sets
	// the host for pools of strings (host and port; the scheme stays, or
	// becomes "http" if empty) and the scheme and host for pools of
	// *url.URL values; other pools need a Rewrite, or RoundTrip fails with
	// ErrNoRewrite.
	//
	// The request's Host field is kept, so the backend sees the Host
	// header the client sent; set r.Host to send the backend's own. With
	// the scheme "https", the transport takes the TLS server name from the
	// URL's host, the backend address, and verifies the backend's
	// certificate against it, unless its TLSClientConfig sets ServerName.
	Rewrite func(r *http.Request, b *upstream.Backend[T])

	// Transport returns the round tripper for backend b, such as one from
	// a TransportCache keyed by b.ID(). Nil means http.DefaultTransport.
	Transport func(b *upstream.Backend[T]) http.RoundTripper

	// Key returns the request's affinity key for the pool's balancer, such
	// as a session cookie or the client address; an empty key carries no
	// affinity. Nil means no key.
	Key func(r *http.Request) string

	// Outcome judges a response or error for passive health checking and
	// retries. Nil means upstream.OutcomeOf for errors, Failure for status
	// codes from 500 and Success otherwise.
	Outcome func(resp *http.Response, err error) upstream.Outcome
}

// RoundTrip implements http.RoundTripper.
func (rt *RoundTripper[T]) RoundTrip(req *http.Request) (*http.Response, error) {
	rewrite := rt.Rewrite
	if rewrite == nil {
		if rewrite = defaultRewrite[T](); rewrite == nil {
			closeBody(req)
			return nil, ErrNoRewrite
		}
	}
	key := ""
	if rt.Key != nil {
		key = rt.Key(req)
	}
	a := rt.Pool.Attempts(key)
	l, err := a.Next()
	if err != nil {
		closeBody(req)
		return nil, err
	}
	replayable := req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
	for {
		resp, o, err := rt.try(req, &l, rewrite, a.Count() > 1)
		retry := o == upstream.Failure && replayable && req.Context().Err() == nil && (idempotent(req) || notSent(err))
		if !retry {
			return finish(l, o, resp, err)
		}
		next, nerr := a.Next()
		if nerr != nil {
			return finish(l, o, resp, err)
		}
		if resp != nil {
			discard(resp.Body)
		}
		l.Release(o)
		l = next
	}
}

// try sends one attempt of req to the backend of l. If Rewrite, Transport,
// the round trip or Outcome panics, it releases l with Failure, closes the
// response if there is one, and lets the panic go on.
func (rt *RoundTripper[T]) try(req *http.Request, l *upstream.Lease[T], rewrite func(*http.Request, *upstream.Backend[T]), again bool) (resp *http.Response, o upstream.Outcome, err error) {
	done := false
	defer func() {
		if !done {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			l.Release(upstream.Failure)
		}
	}()
	out := req.Clone(req.Context())
	if again && req.GetBody != nil {
		if out.Body, err = req.GetBody(); err != nil {
			done = true
			return nil, upstream.Neutral, err
		}
	}
	b := l.Backend()
	rewrite(out, b)
	resp, err = rt.transport(b).RoundTrip(out)
	o = rt.outcome(resp, err)
	done = true
	return resp, o, err
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

func defaultRewrite[T any]() func(*http.Request, *upstream.Backend[T]) {
	var zero T
	switch any(zero).(type) {
	case string:
		return func(r *http.Request, b *upstream.Backend[T]) {
			r.URL.Host, _ = any(b.Value()).(string)
			if r.URL.Scheme == "" {
				r.URL.Scheme = "http"
			}
		}
	case *url.URL:
		return func(r *http.Request, b *upstream.Backend[T]) {
			if u, _ := any(b.Value()).(*url.URL); u != nil {
				r.URL.Scheme, r.URL.Host = u.Scheme, u.Host
			}
		}
	}
	return nil
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

// finish hands the response of the last attempt to the caller: an error
// releases the lease at once; a response reports its outcome and keeps
// the lease until its body ends.
func finish[T any](l upstream.Lease[T], o upstream.Outcome, resp *http.Response, err error) (*http.Response, error) {
	if err != nil || resp.Body == nil {
		l.Release(o)
		return resp, err
	}
	l.Report(o)
	body := &leaseBody[T]{ReadCloser: resp.Body, l: l}
	if w, ok := resp.Body.(io.Writer); ok { // an upgraded connection
		resp.Body = &leaseConnBody[T]{leaseBody: body, w: w}
	} else {
		resp.Body = body
	}
	return resp, nil
}

// leaseBody releases its lease when the body is read to the end or
// closed, whichever comes first.
type leaseBody[T any] struct {
	io.ReadCloser
	l    upstream.Lease[T]
	done atomic.Bool
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
