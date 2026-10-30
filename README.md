# upstream

[![Go Reference](https://pkg.go.dev/badge/github.com/djylb/upstream.svg)](https://pkg.go.dev/github.com/djylb/upstream)

A backend pool for proxies, tunnels and load balancers. It picks a backend for
every connection or request, tracks backend health actively (probes) and
passively (real traffic), drains and removes backends gracefully, and reports
the state of every backend.

- Lock-free, allocation-free picks; membership and health changes publish a new
  immutable view atomically, so updates never disturb picks in flight.
- Balancers: round-robin, smooth weighted round-robin, weighted random,
  power-of-two-choices, least connections and consistent hashing with
  optional bounded loads, or your own.
- Priority tiers for active-backup setups, with per-backend caps on requests
  in flight and optional overflow to the next tier.
- Passive health: ejection on consecutive failures or a failure rate, with
  exponential, capped backoff and half-open trials.
- Active health: TCP connect checks by default, HTTP checks or any function on
  request; rise/fall thresholds, jitter, bounded concurrency, latency
  statistics and failure classification.
- Retries on backends not tried yet, bounded by attempts and a retry budget;
  a dialer and an `http.RoundTripper` over a pool.
- Runtime reconfiguration: balancer, passive health, retries, slow start and
  fallback change on a running pool without losing state.
- Slow start for backends that join or recover, explicit all-down behaviour,
  edge-triggered events.
- Standard library only. Each piece works on its own: balancers without the
  pool, the outlier detector for a single target, the health checker for any
  set of addresses.

```
go get github.com/djylb/upstream
```

| Package    | Contents                                                                         |
|------------|----------------------------------------------------------------------------------|
| `upstream` | `Pool`, balancers, retries, `OutlierDetector`, `Clock`                           |
| `health`   | `Checker` for active health checks, TCP check, `Watch` and `Attach` to drive pools |
| `uphttp`   | HTTP health check, per-backend `http.Transport` cache, `RoundTripper` over a pool |
| `upnet`    | `Dialer` that connects to a pool's backends                                      |

The root package does not import `net` or `net/http`; `upnet` does not
import `net/http`.

## Picking backends

```go
pool := upstream.New[string](upstream.Config{
	Balancer:  upstream.PowerOfTwo(),
	Outlier:   upstream.OutlierConfig{ConsecutiveFailures: 5},
	Retry:     upstream.RetryConfig{Attempts: 3, Budget: 0.2},
	SlowStart: 30 * time.Second,
})
defer pool.Close()

err := pool.Sync([]upstream.Target[string]{
	{ID: "a", Value: "10.0.0.1:8080", Weight: 2},
	{ID: "b", Value: "10.0.0.2:8080"},
})

lease, err := pool.Pick() // or PickKey(clientIP) for affinity
if err != nil {
	return err // ErrNoBackends, ErrUnavailable, ErrSaturated or ErrClosed
}
conn, err := net.Dial("tcp", lease.Value())
lease.Report(upstream.OutcomeOf(err)) // the dial decides the outcome
if err != nil {
	lease.Release(upstream.Failure)
	return err
}
defer lease.Release(upstream.Neutral) // the slot follows the connection
defer conn.Close()
```

`upnet.Dialer` does all of this, with retries.

A `Lease` holds one in-flight slot of its backend until it is released, with
`Success`, `Failure` or `Neutral` (for example when the client went away).
Release every lease exactly once. Releasing the same lease variable twice is a
no-op. When the outcome is known long before the lease ends, as for a
connection that stays open, `Report` records it at once and `Release` later
only returns the slot; a half-open backend's trial then ends with the report.

`Lease.Observe` records a latency sample, such as the time to the first byte,
for the backend's moving average; `upnet` and `uphttp` observe dial and
response-header times themselves.

`Sync` replaces the membership in one step; `Put` adds or replaces one
backend. A backend keeps its counters and health across updates as long as its
ID stays the same. Leases keep the version of the backend they were issued
with. An update that changes neither a member's weight nor its value
(compared with `==`, or with `Config.Equal`) keeps the backend, emits no event
and rebuilds nothing. An empty key carries no affinity: `PickKey("")` is
`Pick`.

### Retries

```go
err := pool.Do(ctx, func(b *upstream.Backend[string]) error {
	conn, err = net.Dial("tcp", b.Value())
	return err
})
```

`Do` and `DoKey` pick a backend, run the function and release the lease with
its outcome, even if the function panics. A `Failure` is retried on a backend
not tried yet, up to `RetryConfig.Attempts` backends, while the context is not
done; the last error is returned. `WithOutcome(err, upstream.Neutral)` marks an
error that is not the backend's fault, which is neither counted against it nor
retried.

With `RetryConfig.Budget`, retries within any `Window` number at most
`MinRetries` plus `Budget` times the requests started, so retries cannot
multiply the load when many requests fail at once.

`Pool.Request(key)` is the retry loop for leases that outlive a function: each
`Next` leases a backend not returned before, or fails with `ErrRetryLimit`,
`ErrRetryBudget` or `ErrUnavailable`, and `Request.Do` runs the loop with a
function that may hand its lease on with `Lease.Take`:

```go
var conn net.Conn
var kept upstream.Lease[string]
req := pool.Request(clientIP)
err := req.Do(ctx, func(l *upstream.Lease[string]) (retry bool, err error) {
	if conn, err = net.Dial("tcp", l.Value()); err != nil {
		return true, err // released as a Failure, then retried
	}
	kept = l.Take() // released when the connection closes
	return false, nil
})
```

### Dialing and HTTP

`upnet.Dialer` dials a pool's backends with retries and returns a `Conn` that
holds the backend's lease until it is closed, so least-connections balancing
and drains follow real connections. The dial reports the outcome; with
`DeferReport`, the caller reports it with `Conn.Report` after a handshake such
as TLS, so handshake failures count against the backend. The `Conn` forwards
`CloseWrite`, `CloseRead`, `ReadFrom` and `WriteTo` so that relays keep
half-close and the zero-copy paths of `io.Copy`.

```go
dialer := &upnet.Dialer[string]{Pool: pool}
conn, err := dialer.DialKey(ctx, clientIP)
```

`uphttp.RoundTripper` balances an `http.Client` or a reverse proxy over a pool:
it points each request at a picked backend, takes an affinity key from the
request, judges responses (5xx is a failure by default) and retries on another
backend when the request can be sent again: its body can be replayed and its
method is idempotent, or the connection was never established. The lease lasts
until the response body is read to the end or closed; for an upgraded
connection (101), until it is closed, since the client may still be writing.
The outgoing request keeps the incoming `Host`; with `https`, the TLS server
name and certificate check use the backend's address unless the transport's
`TLSClientConfig.ServerName` says otherwise. For `*url.URL` values the URL's
path and query are joined before the request's, as a reverse proxy to that URL
does. `Outcome` judges a response for health and `Retry` decides whether to
retry it, so that, say, a 503 is retried without counting against the
backend. Two timeouts keep a backend that does not answer from using up the
whole request's deadline; an attempt they cut before its headers may be
retried. `AttemptTimeout` gives each attempt a real context deadline that
lasts until its body ends (dialer, TLS and servers that honour deadlines see
it) and fails with `ErrAttemptTimeout`; `HeaderTimeout` bounds only the wait
for the response headers, sets no deadline and leaves the body unbounded,
failing with `ErrHeaderTimeout`. For streaming responses use `HeaderTimeout`
alone; for bounded responses use `AttemptTimeout`, or both to fail fast on
headers within a longer overall bound.

A panic in a function given to `Do`, `Request.Do`, `upnet.Dialer` or
`uphttp.RoundTripper` releases the backend's lease and goes on.

A backend's address is one concept for every package: `upstream.AddressOf`
takes a string value as it is and calls `Address()` on values that implement
`upstream.Addresser`, such as a struct with a zone or labels. `health.Watch`,
`health.Attach`, `upnet.Dialer` and `uphttp.RoundTripper` use it unless given a
function of their own, and fail with `upstream.ErrNoAddress` for pools whose
values have no address.

```go
client := &http.Client{Transport: &uphttp.RoundTripper[string]{
	Pool:      pool,
	Transport: func(b *upstream.Backend[string]) http.RoundTripper { return cache.Get(b.ID()) },
	Key:       func(r *http.Request) string { return sessionCookie(r) },
}}
```

### Balancers

| Balancer               | Behaviour                                                                 |
|------------------------|---------------------------------------------------------------------------|
| `RoundRobin()`         | Cycles in order, ignores weights.                                         |
| `WeightedRoundRobin()` | Smooth weighted round-robin (the default): exact proportions, picks spread through the cycle. |
| `Random()`             | Weighted random, constant time.                                           |
| `PowerOfTwo()`         | Two random candidates, the one with fewer requests in flight per weight.  |
| `LeastConn()`          | Fewest requests in flight per weight, random tie-break; scans all.        |
| `ConsistentHash(cfg)`  | Hash ring keyed by `PickKey`/`PickHash`; optional bounded loads.          |

Consistent hashing places backends by ID, so every process with the same IDs
maps a key to the same backend, and adding or removing one of n backends moves
only about 1/n of the keys. With `LoadFactor` set, a backend already carrying
that multiple of its fair share is passed over for the next one on the ring.

A `Balancer` builds a `Picker` from `Candidate` values and can be used without
a pool. Implement the two interfaces for custom strategies. A candidate carries
the backend's in-flight count and moving average latency, both read live at
pick time, and `Meta`, which a pool sets to the `*Backend[T]`: a zone-aware or
least-latency balancer reads what it needs from there. A pool builds one picker
per tier and calls `Build` for a tier only when its candidates change, never
two at once, and coalesces changes that arrive during a build into one more. A balancer whose build is costly also
implements `Rebuilder` to update the previous picker instead;
`ConsistentHash` does, so a member that goes down or comes back on a large ring
costs a linear merge, not a new sort.

### Tiers

`Target.Priority` puts backends in tiers; lower values are preferred and the
default, 0, puts every backend in one tier. A pick goes to the most preferred
tier that has an eligible backend and is balanced within that tier by the
pool's balancer. It moves to the next tier when no backend of the tier can
take it: all are down, ejected, offline, draining or half-open without a free
trial slot, or a retry has tried them all. Distinct priorities make an
active-backup pool.

`Target.MaxInFlight` caps a backend's leases in flight; a backend at its cap
refuses picks, in panic mode too, and the pick goes to the other backends of
its tier. When every backend of a tier that the pick may use is full, the
pick fails with `ErrSaturated`, so that a backup takes traffic only when the
tier above is down, not when it is busy; `Config.Overflow` lets such picks go
on to the next tier instead, and they fail with `ErrSaturated` only if they
found a tier full and no tier could take them.

`ErrUnavailable` means no backend can take the pick for reasons of health,
drains or offline marks; `ErrSaturated` means backends refused it for capacity
and calls for backpressure: it clears as leases are released.
`errors.Is(ErrSaturated, ErrUnavailable)` holds, so code that only asks
whether a pick failed for lack of backends needs one check.

```go
pool := upstream.New[string](upstream.Config{Overflow: true})
err := pool.Sync([]upstream.Target[string]{
	{ID: "a", Value: "10.0.0.1:8080", MaxInFlight: 100},
	{ID: "b", Value: "10.0.0.2:8080", MaxInFlight: 100},
	{ID: "backup", Value: "10.0.0.9:8080", Priority: 1},
})
```

## Health and eligibility

A backend takes new picks when it is

- not draining or removed,
- not marked offline by the caller with `MarkOffline`,
- not reported down by active health checks (`HealthUnknown` and `HealthUp`
  both take traffic), and
- not ejected by passive health checking. A half-open backend takes up to
  `HalfOpenRequests` trial requests at a time.

Each of the last three is a veto of its own, and none overrides another:
`MarkOnline` lifts only the caller's mark, and a backend that
checks report down stays out. A backend's `State` names the first that applies,
in the order draining, offline, unhealthy, ejected, half-open.

`MarkOffline` is for what the caller knows better than any probe, such as a
backend reached through a session that dropped. Like `ReportCheck`, it takes
the `*Backend` and ignores calls for a member that has since been removed or
added again, so a late call from an old session cannot take a new one out.
The mark follows the member's ID across updates, emits `EventStateChanged`
with the given cause (or `ErrOffline`), and rebuilds the view like any other
change. A backend that comes back online ramps up under slow start.

**When too few backends are eligible**, `Config.Fallback` decides:

- `FallbackNone` (default): `Pick` returns `ErrUnavailable` when no member can
  take the request.
- `FallbackAll`: panic mode. `Pick` chooses among all members that are
  neither draining nor offline, ignoring health, on the grounds that a signal
  marking everything down is more often wrong than right; tiers still apply.
  The pool panics when no member is eligible, when fewer than
  `Config.PanicThreshold` percent of the members that are neither draining
  nor offline are, and for a pick that every eligible member refuses, such as
  half-open members out of trial slots after a whole-pool ejection. Outcomes
  still count. Offline marks are not health signals: panic mode never picks
  an offline member.

An empty pool returns `ErrNoBackends`; a closed one `ErrClosed`.

### Passive health

`OutlierConfig` ejects a backend after `ConsecutiveFailures` failures in a row,
or when `FailurePercent` of the requests in a sliding `Window` failed (once
`MinRequests` were seen). The ejection lasts `BaseEjection`, doubling with each
ejection that is not followed by a recovery, up to `MaxEjection`. Then the
backend is half-open: `HalfOpenSuccesses` successful trials restore it and
reset the backoff; a failed trial ejects it again. Each ejection is one event,
however many failures follow. `MaxEjectionPercent` caps the share of the pool
that may be ejected or half-open at once, so a fault that all backends share
cannot eject them all; refused ejections are counted in
`OutlierStatus.Refused`.

`OutlierDetector` is the same logic for a single target, usable without a pool:
`Admit` before a request, `Record` its outcome after.

### Configuration files

`ParseBalancer` and `ParseFallback` take the names that the built-in balancers
and `Fallback` print, such as `least-conn` and `all`. `State`, `Health`,
`OutlierState`, `Outcome`, `EventKind`, `Fallback` and `health.Reason` encode
and decode as their names in JSON and text. A `Balancer` is an interface: a
configuration names it and `ParseBalancer` builds it.

`Pool.SetConfig` applies a reloaded configuration to a running pool, keeping
its members, counters, ejections and leases: a new balancer builds a new view,
new outlier settings apply from the next request, disabling passive health
checking clears ejections, and retry settings apply from the next retry.
`Checker.SetConfig` does the same for active checks.

### Active health

```go
watcher, err := health.Watch(ctx, pool, nil, // nil: TCP connect check of each value
	health.Config[string]{Options: health.Options{Interval: 5 * time.Second, Rise: 2, Fall: 3}})
if err != nil {
	return err
}
defer watcher.Close()
```

`Watch` follows the pool's membership and reports each result with
`Pool.ReportCheck`, matching reports to members by generation so that a
re-added member never takes an old verdict. Give a target function to choose
the address, a `Probe` (such as `uphttp.Probe`) or per-member options.
Thresholds apply in the checker: before its first check a target is
`HealthUnknown`; the first check decides at once; after that `Rise`
consecutive passes bring it up and `Fall` consecutive failures take it down.
Transitions are reported once each. Closing the watcher before the pool sets
the members' health back to `HealthUnknown`, so no member stays out of
rotation on a verdict nothing will revise.

`Attach` connects a pool to one group of a shared checker instead, for
example one pool per tunnel. The checker then holds targets by address
(`TargetKey`): an address that several pools or members use is checked once
and reported to each of them, members with one ID but different addresses stay
apart, and a member that joins an address already checked takes its verdict at
once. Closing a pool, or calling the function `Attach` returns, ends its group.
`Attach` fails with `health.ErrClosed` for a closed checker and `Watch` with
the context's error once it is done. An empty network and `tcp` are the same
network, so pools that spell it differently share one check.

### Draining

`Drain(id)` stops new picks while the backend stays a member; `Remove(id)`
also takes it out of the pool. Both return a `*Drain` whose `Done` channel is
closed, and whose `Wait` returns, when the backend's last lease is released.
`Resume` ends a drain early; the `Drain` then reports `Canceled` and `Wait`
returns `ErrDrainCanceled`. Members that
`Sync` removes drain the same way.

### Slow start

With `Config.SlowStart`, a backend that joins or becomes healthy again starts
with a small share of traffic that grows linearly to its full weight over the
given duration.

### Events and status

`Subscribe` delivers `EventAdded`, `EventUpdated`, `EventRemoved`,
`EventStateChanged` and `EventBalancerPanic` in order, starting with the
current members. A Balancer that panics while the pool builds its view does
not crash the program or reach the caller that changed the pool: the pool keeps
routing with its last view, reports a `*BalancerPanicError` with the panic value
and stack in an `EventBalancerPanic`, and builds again on its next change. Callbacks run
without any lock held and may call the pool. `Status` and `Backend.Status`
return in-flight counts, outcome counters (successes, failures and neutral
outcomes, which with the in-flight count make up every pick), the latest check
result, the outlier state and the routing state of every backend.
`Available` counts the members that are eligible for traffic, for readiness
checks; it is zero in panic mode, when `FallbackAll` still lets picks through.

## Health checks on their own

The checker works on any set of targets. Each target has a key of your
choosing and may belong to groups; a key used by several groups is checked
once and stops when its last group lets go of it.

```go
checker := health.New[string, int](ctx, health.Config[string]{
	Options: health.Options{Interval: 10 * time.Second, Timeout: 3 * time.Second},
	OnChange: func(r health.Report[string]) {
		report(r.Key, r.Health, r.Reason, r.Latency) // one call per transition
	},
})
defer checker.Close()

// Apply a full list for tunnel 7: adds new targets, keeps unchanged ones
// with their state, removes the rest. Put and AddToGroup add one target.
checker.SyncGroup(7, map[string]health.Target{
	"10.0.0.5:22":  {Address: "10.0.0.5:22"},
	"db.lan:5432":  {Address: "db.lan:5432", Options: health.Options{Fall: 5}},
	"10.0.0.9:53":  {Network: "udp", Address: "10.0.0.9:53"}, // not checked
})

checker.RemoveGroup(7) // tunnel deleted: its checks stop at once
```

- **TCP check** (the default for `tcp`, `tcp4`, `tcp6`): connects and closes
  at once, without writing or reading application data. Host names are
  resolved by every check; the result names the address that answered. The
  latency covers the connection to that address only, not name resolution,
  and is measured on the checker's `Clock`, which also runs check timeouts: the
  probe's context carries the check's deadline.
  The normal close leaves a short-lived TIME_WAIT entry on the checking host;
  `TCPOptions.Reset` closes with a reset instead, which avoids that but may be
  logged by the target. `TCPOptions.Dial` plugs in your own dialer or
  resolver.
- **HTTP check**: `uphttp.Probe(uphttp.Check{...})` as a target's `Probe`,
  with method, path, Host header, TLS server name (by default the Host
  header's host), expected status range, body match and your own client for
  other TLS settings. It runs only for targets that name it. The default
  client connects directly, ignoring proxy settings in the environment.
- **UDP** has no handshake to observe, so there is no generic UDP check. UDP
  targets without a `Probe` are not checked and stay `HealthUnknown`; give a
  `Probe` if you know the protocol.
- **Results** carry the health before and after the check (`Changed` compares
  them), the latency and its statistics, the answering address and a
  `Reason` (`refused`, `timeout`,
  `unreachable` for unreachable or down hosts and networks and unusable local
  addresses, `dns`, `reset`, `response`, `other`) derived from error values
  and system error numbers. `Status` adds the last latency, a moving average
  and the minimum and maximum over a recent window.
- **Options** (interval, unhealthy interval, timeout, jitter, rise, fall,
  disabled) can be set globally and per target, merged field by field before
  defaults apply, so a target's own interval also sets its default timeout and
  jitter, and changed at any time with
  `Put`, `AddToGroup`, `SyncGroup` or `SetConfig`; the next check is
  rescheduled at once. `CheckNow` checks a target ahead of schedule, for
  example after real traffic to it failed. A target whose checks stop goes
  back to `HealthUnknown`.
- **Removal** cancels the target's check in progress through its context,
  discards the result, stops its timer and frees its slot. Once `Remove`,
  `RemoveFromGroup`, `RemoveGroup` or `SyncGroup` returns, no callback for the
  removed target starts (one already running may finish, as with
  `time.Timer.Stop`); `OnRemove` reports its key and generation last.
  Adding the key again, or changing its network or address, starts a new
  generation; `OnRemove` reports the end of the old one too, and every report
  carries the generation so that buffered reports of an old target can be
  told apart.
- **Callbacks** run one at a time, in order, on a goroutine the checker owns,
  without locks held. They may call any method, `Close` included. A blocked
  callback holds back later callbacks but not checks; results that change
  nothing are coalesced per target meanwhile.

`Config.Concurrency` bounds how many checks run at once; a hanging target holds
one slot and never delays the others.

## HTTP transports

`uphttp.TransportCache` keeps one `*http.Transport` per key (for example per
backend ID), dropping transports that have been idle for `IdleTTL` and the
least recently used beyond `MaxEntries`. Dropping a transport closes its idle
connections; give transports an `IdleConnTimeout` so that connections still in
use when it was dropped close once they go idle. Keyed by backend ID, remove a
transport on `EventRemoved` and on `EventUpdated`, which a pool emits only when
a member's value, weight, priority or cap really changed.

## Concurrency and lifetimes

- All types are safe for concurrent use.
- No lock is held while calling a balancer, a clock, a probe or a callback, so
  callbacks may call back into the pool or the checker.
- No method waits for a probe or a callback, so none deadlocks when called
  re-entrantly, `Close` included.
- The pool owns its ejection and slow-start timers, which `Close` stops.
  `Pool.Done` reports that the pool was closed.
- A pool rebuilds its view on the goroutine that changed it, without locks
  held; a change that arrives during a build is left to the building
  goroutine, so the method returns before its new members are visible to
  picks. A goroutine builds at most twice per change, its own and one for
  the changes that arrived meanwhile, and hands anything later to a
  short-lived goroutine of the pool, so a request whose release ejects a
  backend is not held up by a stream of changes. A Balancer panic is
  recovered on every build path and reported as `EventBalancerPanic`; changes
  that arrived during that build are still built, and otherwise the next
  change retries. Removals, drains and health verdicts take effect for picks
  at once regardless.
- The checker owns its timers, one goroutine per running check and one
  goroutine for callbacks. `Close` (or the end of its context) stops them and
  returns at once; `Done` is closed, and `Wait` returns, once every one of them
  has ended. Waiting on `Done` from inside a probe or callback waits for
  itself.
- A watcher started with `Watch` belongs to its pool and closes with it; a
  pool attached with `Attach` leaves its checker when it closes.
- Time comes from an injectable `Clock`, so behaviour over time can be tested
  without waiting.

## Limits

- Weights range from 1 to `MaxWeight` (2³¹−1); zero means 1.
- Smooth weighted round-robin precomputes its cycle when the cycle times the
  number of backends is at most 2²²; beyond that, which takes extreme weight
  ratios, it picks at random by weight, lock-free, so proportions hold on
  average.
- Bounded-load consistent hashing reads every backend's in-flight count on
  each pick; retries (not first picks) on rings of more than 256 backends
  allocate.
- A `Request` allocates from the fifth attempt of one request on; `Pool.Do`
  does not allocate, `Request.Do` allocates once.
- Slow start works through weights within a tier, so it has no effect with
  `RoundRobin`, on a backend alone in its tier, or between tiers: traffic
  moves back to a recovered tier at once.
- The failure-rate window counts up to 2³²−1 requests per window and backend;
  the retry budget, up to 2³²−1 requests per window and pool.
- The checker compares targets by key; two groups that give different
  targets for one key share the latest one.

## License

Apache License 2.0
