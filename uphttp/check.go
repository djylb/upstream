// Package uphttp holds the HTTP-specific parts of upstream: an HTTP health
// check, a cache of per-backend transports and an http.RoundTripper that
// balances requests over a pool. It is separate so that the root package
// does not depend on net/http.
package uphttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"

	"github.com/djylb/upstream/health"
)

// ErrBodyMismatch means a response body did not contain Check.ExpectBody.
// It wraps health.ErrBadResponse.
var ErrBodyMismatch = fmt.Errorf("uphttp: response body does not match: %w", health.ErrBadResponse)

// A StatusError reports a response status outside Check.Status. It wraps
// health.ErrBadResponse.
type StatusError struct {
	// Code is the response status code.
	Code int
}

// Error implements error.
func (e *StatusError) Error() string {
	return "uphttp: unexpected status " + strconv.Itoa(e.Code)
}

// Unwrap returns health.ErrBadResponse.
func (e *StatusError) Unwrap() error { return health.ErrBadResponse }

// Check describes an HTTP health check. The zero value sends GET / over
// plain HTTP and expects a status from 200 to 399.
type Check struct {
	// Client sends the requests. Nil means a client that connects to the
	// target directly, ignoring proxy settings in the environment, does not
	// follow redirects and does not keep connections alive, so no idle
	// connection outlives a check; supply a client to reuse connections
	// across checks or to go through a proxy. For "https" the default
	// client verifies the certificate against ServerName with the system
	// roots; give a client of your own for other roots, client
	// certificates or its own dialer, and set the server name in its
	// transport.
	Client *http.Client

	// Header is added to every request.
	Header http.Header

	// Method is the request method. The default is GET.
	Method string

	// Scheme is "http" (the default) or "https".
	Scheme string

	// Path is the request path, with an optional query. The default is
	// "/".
	Path string

	// Host overrides the Host header, for backends that serve virtual
	// hosts. The connection still goes to the target's address.
	Host string

	// ServerName is the TLS server name that the default client sends
	// and verifies the certificate against for "https". The default is
	// Host without its port, and, without Host, the host of the target's
	// address. It has no effect with a Client of your own.
	ServerName string

	// ExpectBody, if not empty, must occur in the first MaxBody bytes of
	// the response body.
	ExpectBody string

	// Status is the range of passing status codes. The zero value means
	// 200 to 399.
	Status StatusRange

	// MaxBody bounds how much of the body is read, both to match
	// ExpectBody and to drain the response for connection reuse. The
	// default is 64 KiB.
	MaxBody int64
}

// Probe returns a health.Probe that sends c to the target's address, a
// host and port. The Observation names the address of the connection that
// carried the request. The Check is copied; changing it later has no
// effect.
func Probe(c Check) health.Probe {
	if c.Client == nil {
		c.Client = defaultClient(serverName(c))
	}
	if c.Method == "" {
		c.Method = http.MethodGet
	}
	if c.Scheme == "" {
		c.Scheme = "http"
	}
	if c.Path == "" || c.Path[0] != '/' {
		c.Path = "/" + c.Path
	}
	if c.Status == (StatusRange{}) {
		c.Status = StatusRange{Min: 200, Max: 399}
	}
	if c.MaxBody <= 0 {
		c.MaxBody = 64 << 10
	}
	c.Header = c.Header.Clone()
	expect := []byte(c.ExpectBody)
	return func(ctx context.Context, t health.Target) (health.Observation, error) {
		var mu sync.Mutex
		var obs health.Observation
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
			if a := info.Conn.RemoteAddr(); a != nil {
				mu.Lock()
				obs.Addr = a.String()
				mu.Unlock()
			}
		}})
		err := c.do(ctx, t.Address, expect)
		mu.Lock()
		defer mu.Unlock()
		return obs, err
	}
}

// serverName returns the TLS server name of c for the default client, or
// "" for the target's host.
func serverName(c Check) string {
	if c.ServerName != "" || c.Host == "" {
		return c.ServerName
	}
	if h, _, err := net.SplitHostPort(c.Host); err == nil {
		return h
	}
	return c.Host
}

// defaultClient returns the client of a Check without one; a non-empty
// serverName replaces the target's host for TLS.
func defaultClient(serverName string) *http.Client {
	// A fresh transport rather than a clone of the default one, which
	// would send checks to the proxy named by the environment. Timeouts
	// come from the check's context.
	t := &http.Transport{
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}
	if serverName != "" {
		t.TLSClientConfig = &tls.Config{ServerName: serverName}
	}
	return &http.Client{
		Transport: t,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *Check) do(ctx context.Context, address string, expect []byte) error {
	req, err := http.NewRequestWithContext(ctx, c.Method, c.Scheme+"://"+address+c.Path, http.NoBody)
	if err != nil {
		return err
	}
	// A copy per request: a client's transport may change the header,
	// and checks run concurrently.
	req.Header = c.Header.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	if c.Host != "" {
		req.Host = c.Host
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, c.MaxBody)
	if !c.Status.Contains(resp.StatusCode) {
		_, _ = io.Copy(io.Discard, body)
		return &StatusError{Code: resp.StatusCode}
	}
	if len(expect) == 0 {
		_, _ = io.Copy(io.Discard, body)
		return nil
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if !bytes.Contains(data, expect) {
		return ErrBodyMismatch
	}
	return nil
}
