// Package upstream is a backend pool for proxies, tunnels and load
// balancers: it picks a backend for each connection or request, tracks
// backend health actively and passively, and reports the state of every
// backend.
//
// The pieces compose but also stand alone:
//
//   - A [Balancer] builds a [Picker] from [Candidate] values and needs no
//     pool: [RoundRobin], [WeightedRoundRobin], [Random], [PowerOfTwo],
//     [LeastConn], [Failover] and [ConsistentHash] (a hash ring with
//     optional bounded loads), or any implementation of the interface.
//   - An [OutlierDetector] is passive health checking for one backend:
//     consecutive failures or a failure rate eject it, with exponential
//     backoff, and half-open trials let it back in.
//   - Package health runs active health checks on any set of targets and
//     drives pools with them; package uphttp adds an HTTP check, a
//     per-backend transport cache and an http.RoundTripper over a pool;
//     package upnet dials the backends of a pool.
//   - A [Pool] combines them: it holds the members, routes picks through
//     the balancer to eligible backends, counts requests in flight through
//     a [Lease], feeds outcomes to an outlier detector per backend, takes
//     active health reports with [Pool.ReportCheck], drains and removes
//     backends gracefully, and announces every change as an [Event].
//
// # Picking
//
// Pick, PickKey and PickHash return a Lease, which must be released
// exactly once with the request's [Outcome]; [Lease.Report] records the
// outcome early for leases that live long, such as connections. [Pool.Do]
// picks, runs a function and releases, retrying failures on other
// backends as [RetryConfig] allows, within a retry budget; [Attempts] does
// the same for leases that outlive a function. Picks are lock-free and do
// not allocate: they read an immutable routing view that every change
// rebuilds and publishes atomically, so updates never disturb picks in
// flight.
//
// # Health
//
// A backend is eligible when it is not draining, not reported unhealthy by
// active checks and not ejected by passive checks. A half-open backend
// takes up to OutlierConfig.HalfOpenRequests trial requests at a time.
// When no backend is eligible, Pick fails with [ErrUnavailable], or, with
// [FallbackAll], picks among all members that are not draining.
//
// State changes are edge-triggered: a backend that keeps failing is
// reported down once.
//
// # Concurrency
//
// All types are safe for concurrent use. No lock is held while calling a
// Balancer, a Clock, a probe or a subscriber, so callbacks may call back
// into the pool. The pool starts no goroutines; the only timers it owns are
// those ending ejections and those stepping slow starts, which Close stops.
// No Close in this module waits; types that own goroutines have a Done
// channel for callers that need to wait for them.
package upstream
