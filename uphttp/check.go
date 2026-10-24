// Package uphttp holds the HTTP-specific parts of upstream: an HTTP health
// check and a cache of per-backend transports. It is separate so that the
// root package does not depend on net/http.
package uphttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	// across checks or to go through a proxy. To check over
	// TLS, set Scheme to "https" and give a client whose transport has the
	// TLS configuration (server name, roots, client certificates) and, if
	// needed, its own dialer.
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
		c.Client = defaultClient()
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
			mu.Lock()
			obs.Addr = info.Conn.RemoteAddr().String()
			mu.Unlock()
		}})
		err := c.do(ctx, t.Address, expect)
		mu.Lock()
		defer mu.Unlock()
		return obs, err
	}
}

func defaultClient() *http.Client {
	// A fresh transport rather than a clone of the default one, which
	// would send checks to the proxy named by the environment. Timeouts
	// come from the check's context.
	t := &http.Transport{
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
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
	for k, v := range c.Header {
		req.Header[k] = v
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
