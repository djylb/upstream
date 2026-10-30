package uphttp

import (
	"container/list"
	"net/http"
	"sync"
	"time"

	"github.com/djylb/upstream"
)

// TransportCacheConfig configures a TransportCache.
type TransportCacheConfig struct {
	// Clock is the time source. Nil means upstream.SystemClock.
	Clock upstream.Clock

	// IdleTTL drops a transport that has not been returned by Get for this
	// long. Zero keeps transports until they are evicted or removed.
	IdleTTL time.Duration

	// MaxEntries bounds the number of cached transports; adding one more
	// evicts the least recently used. Zero means no bound.
	MaxEntries int
}

// A TransportCache keeps one *http.Transport per key, typically per
// backend ID, so that each backend gets its own connection pool and its
// own settings (TLS server name, dialer, proxy protocol and so on) while
// connections are reused across requests.
//
// Dropping a transport, whether by IdleTTL, MaxEntries, Remove or
// CloseIdleConnections, closes its idle connections; requests still using
// it finish normally, and the next Get for its key creates a new one. The
// connections of those requests then return to the dropped transport's
// idle pool, where only its own IdleConnTimeout closes them: give the
// transports newTransport creates an IdleConnTimeout, or they stay open
// until the server closes them.
// Expired entries are dropped during Get, in time proportional to the
// number dropped; the cache starts no goroutines.
//
// Keyed by backend ID, a transport carries settings derived from the
// backend's value, such as its TLS server name, so it must go when that
// value changes: subscribe to the pool and call Remove on
// upstream.EventRemoved and on upstream.EventUpdated, which a pool emits
// only when a member's weight or value really changed.
type TransportCache[K comparable] struct {
	newTransport func(K) *http.Transport
	clock        upstream.Clock
	ttl          time.Duration
	maxEntries   int

	mu      sync.Mutex
	entries map[K]*list.Element // of *cacheEntry[K]; guarded by mu
	lru     list.List           // most recently used first; guarded by mu
}

type cacheEntry[K comparable] struct {
	used      time.Time
	transport *http.Transport
	key       K
}

// NewTransportCache returns a cache that creates transports with
// newTransport, which must not return nil. newTransport is called without
// any lock held and may be called twice for one key when two Gets race;
// the loser's transport is discarded unused.
func NewTransportCache[K comparable](newTransport func(K) *http.Transport, cfg TransportCacheConfig) *TransportCache[K] {
	c := &TransportCache[K]{
		newTransport: newTransport,
		clock:        cfg.Clock,
		ttl:          max(cfg.IdleTTL, 0),
		maxEntries:   max(cfg.MaxEntries, 0),
		entries:      make(map[K]*list.Element),
	}
	if c.clock == nil {
		c.clock = upstream.SystemClock()
	}
	return c
}

// Get returns the transport for key, creating it if needed.
func (c *TransportCache[K]) Get(key K) *http.Transport {
	now := c.clock.Now()
	c.mu.Lock()
	dropped := c.expireLocked(now)
	if t := c.useLocked(key, now); t != nil {
		c.mu.Unlock()
		closeIdle(dropped)
		return t
	}
	c.mu.Unlock()
	closeIdle(dropped)

	t := c.newTransport(key)

	c.mu.Lock()
	if cur := c.useLocked(key, now); cur != nil {
		c.mu.Unlock()
		t.CloseIdleConnections()
		return cur
	}
	c.entries[key] = c.lru.PushFront(&cacheEntry[K]{used: now, transport: t, key: key})
	for c.maxEntries > 0 && c.lru.Len() > c.maxEntries {
		dropped = append(dropped, c.dropLocked(c.lru.Back()))
	}
	c.mu.Unlock()
	closeIdle(dropped)
	return t
}

func (c *TransportCache[K]) useLocked(key K, now time.Time) *http.Transport {
	el := c.entries[key]
	if el == nil {
		return nil
	}
	e := el.Value.(*cacheEntry[K])
	if now.After(e.used) {
		e.used = now
	}
	c.lru.MoveToFront(el)
	return e.transport
}

func (c *TransportCache[K]) expireLocked(now time.Time) []*http.Transport {
	if c.ttl == 0 {
		return nil
	}
	var dropped []*http.Transport
	for el := c.lru.Back(); el != nil; el = c.lru.Back() {
		if now.Sub(el.Value.(*cacheEntry[K]).used) < c.ttl {
			break
		}
		dropped = append(dropped, c.dropLocked(el))
	}
	return dropped
}

func (c *TransportCache[K]) dropLocked(el *list.Element) *http.Transport {
	e := c.lru.Remove(el).(*cacheEntry[K])
	delete(c.entries, e.key)
	return e.transport
}

// Remove drops the transport for key and closes its idle connections. It
// reports false if there was none.
func (c *TransportCache[K]) Remove(key K) bool {
	c.mu.Lock()
	el := c.entries[key]
	var t *http.Transport
	if el != nil {
		t = c.dropLocked(el)
	}
	c.mu.Unlock()
	if t == nil {
		return false
	}
	t.CloseIdleConnections()
	return true
}

// Len returns the number of cached transports, including expired ones not
// yet dropped.
func (c *TransportCache[K]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// CloseIdleConnections drops every transport and closes its idle
// connections. The cache stays usable. Call it when shutting down.
func (c *TransportCache[K]) CloseIdleConnections() {
	c.mu.Lock()
	dropped := make([]*http.Transport, 0, c.lru.Len())
	for el := c.lru.Back(); el != nil; el = c.lru.Back() {
		dropped = append(dropped, c.dropLocked(el))
	}
	c.mu.Unlock()
	closeIdle(dropped)
}

func closeIdle(ts []*http.Transport) {
	for _, t := range ts {
		t.CloseIdleConnections()
	}
}
