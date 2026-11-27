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
// when a member's value, weight, priority, in-flight cap or cost changes.
type TransportCache[K comparable] struct {
	newTransport func(K) *http.Transport
	clock        upstream.Clock
	ttl          time.Duration
	maxEntries   int

	mu       sync.Mutex
	building map[K]*transportBuild
	entries  map[K]*list.Element // of *cacheEntry[K]; guarded by mu
	lru      list.List           // most recently used first; guarded by mu
}

// transportBuild counts factories sharing one cache generation.
type transportBuild struct{ builders int }

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
		building:     make(map[K]*transportBuild),
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
	token := c.building[key]
	if token == nil {
		token = new(transportBuild)
		c.building[key] = token
	}
	token.builders++
	c.mu.Unlock()
	defer func() {
		if v := recover(); v != nil {
			c.mu.Lock()
			token.builders--
			if token.builders == 0 && c.building[key] == token {
				delete(c.building, key)
			}
			c.mu.Unlock()
			panic(v)
		}
	}()
	closeIdle(dropped)
	t := c.newTransport(key)
	now = c.clock.Now()
	c.mu.Lock()
	dropped = c.expireLocked(now)
	if cur := c.useLocked(key, now); cur != nil {
		c.mu.Unlock()
		closeIdle(dropped)
		t.CloseIdleConnections()
		return cur
	}
	if c.building[key] != token {
		c.mu.Unlock()
		closeIdle(dropped)
		return t
	}
	delete(c.building, key)
	now = c.frontTime(now)
	c.entries[key] = c.lru.PushFront(&cacheEntry[K]{used: now, transport: t, key: key})
	for c.maxEntries > 0 && c.lru.Len() > c.maxEntries {
		dropped = append(dropped, c.dropLocked(c.lru.Back()))
	}
	c.mu.Unlock()
	closeIdle(dropped)
	return t
}

// entryOf returns the cache entry that el holds.
func entryOf[K comparable](el *list.Element) *cacheEntry[K] {
	return el.Value.(*cacheEntry[K]) //nolint:forcetypeassert // the list holds only entries, pushed by Get
}

func (c *TransportCache[K]) useLocked(key K, now time.Time) *http.Transport {
	el := c.entries[key]
	if el == nil {
		return nil
	}
	e := entryOf[K](el)
	e.used = c.frontTime(now)
	c.lru.MoveToFront(el)
	return e.transport
}

func (c *TransportCache[K]) frontTime(now time.Time) time.Time {
	if el := c.lru.Front(); el != nil && entryOf[K](el).used.After(now) {
		return entryOf[K](el).used
	}
	return now
}

func (c *TransportCache[K]) expireLocked(now time.Time) []*http.Transport {
	if c.ttl == 0 {
		return nil
	}
	var dropped []*http.Transport
	for el := c.lru.Back(); el != nil; el = c.lru.Back() {
		if now.Sub(entryOf[K](el).used) < c.ttl {
			break
		}
		dropped = append(dropped, c.dropLocked(el))
	}
	return dropped
}

func (c *TransportCache[K]) dropLocked(el *list.Element) *http.Transport {
	c.lru.Remove(el)
	e := entryOf[K](el)
	delete(c.entries, e.key)
	return e.transport
}

// Remove drops the transport for key and closes its idle connections. It
// also invalidates pending builds. It reports whether a cached entry existed.
func (c *TransportCache[K]) Remove(key K) bool {
	c.mu.Lock()
	delete(c.building, key)
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
	clear(c.building)
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
