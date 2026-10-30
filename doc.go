// Package upstream is a backend pool for proxies, tunnels and load
// balancers: it picks a backend for each connection or request, tracks
// backend health actively and passively, and reports the state of every
// backend.
//
// The pieces compose but also stand alone:
//
//   - A [Balancer] builds a [Picker] from [Candidate] values and needs no
//     pool: [RoundRobin], [WeightedRoundRobin], [Random], [PowerOfTwo],
//     [LeastConn] and [ConsistentHash] (a hash ring with
//     optional bounded loads), or any implementation of the interface.
//     Candidates carry in-flight counts, a moving average latency and the
//     pool's backend, so custom balancers can route by load, latency or
//     the backend's value; a [Rebuilder] updates its Picker incrementally.
//   - An [OutlierDetector] is passive health checking for one backend:
//     consecutive failures or a failure rate eject it, with exponential
//     backoff, and half-open trials let it back in.
//   - Package health runs active health checks on any set of targets and
//     drives pools with them; package uphttp adds an HTTP check, a
//     per-backend transport cache and an http.RoundTripper over a pool;
//     package upnet dials the backends of a pool. All three find a
//     backend's address the same way, with [AddressOf]: a string value,
//     or a value that implements [Addresser].
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
// outcome early for leases that live long, such as connections, and
// [Lease.Observe] feeds the backend's latency. [Pool.Do] picks, runs a
// function and releases, retrying failures on other backends as
// [RetryConfig] allows, within a retry budget; a [Request] does the same
// for leases that outlive a function. Picks are lock-free and do not
// allocate: they read an immutable routing view that changes rebuild and
// publish atomically, so updates never disturb picks in flight. Updates
// that change no routing rebuild nothing, and rebuilds that overlap are
// coalesced. [Pool.SetConfig] changes the balancer, health, retry and
// fallback settings of a running pool.
//
// Target.Priority puts members in tiers: picks go to the most preferred
// tier with a member that can take them, balanced within it, which makes
// active-backup pools. Target.MaxInFlight caps a member's leases, and
// Config.Overflow lets picks go on to the next tier when a tier is full.
//
// # Health
//
// A backend is eligible when it is not draining, not marked offline with
// [Pool.MarkOffline], not reported unhealthy by active checks and not
// ejected by passive checks; each of these vetoes picks on its own. A
// half-open backend takes up to OutlierConfig.HalfOpenRequests trial
// requests at a time. When no backend is eligible, Pick fails with
// [ErrUnavailable], or, with [FallbackAll], picks among all members that
// are neither draining nor offline; panic
// mode also covers too few eligible members (Config.PanicThreshold) and
// eligible members that all refuse a pick. OutlierConfig.MaxEjectionPercent
// keeps passive health checking from ejecting too much of a pool.
//
// State changes are edge-triggered: a backend that keeps failing is
// reported down once.
//
// # Concurrency
//
// All types are safe for concurrent use. No lock is held while calling a
// Balancer, a Clock, a probe or a subscriber, so callbacks may call back
// into the pool. The pool rebuilds its view on the goroutine that changed
// it, at most twice per change; a rebuild left over after that runs on a
// short-lived goroutine of the pool, which builds nothing once Close has
// begun. A Balancer that panics during a build does not reach any caller:
// the pool keeps its last view, emits [EventBalancerPanic] and builds again
// on its next change. The only timers the pool owns are those ending ejections and
// those stepping slow starts, which Close stops.
// No Close in this module waits; types that own goroutines have a Done
// channel for callers that need to wait for them.
package upstream
