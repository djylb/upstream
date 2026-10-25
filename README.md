# upstream

[![Go Reference](https://pkg.go.dev/badge/github.com/djylb/upstream.svg)](https://pkg.go.dev/github.com/djylb/upstream)

A backend pool for proxies, tunnels and load balancers. It picks a backend for
every connection or request, tracks backend health actively (probes) and
passively (real traffic), drains and removes backends gracefully, and reports
the state of every backend.

- Lock-free, allocation-free picks; membership and health changes publish a new
  immutable view atomically, so updates never disturb picks in flight.
- Balancers: round-robin, smooth weighted round-robin, weighted random,
  power-of-two-choices, least connections, failover (active-backup) and
  consistent hashing with optional bounded loads, or your own.
- Passive health: ejection on consecutive failures or a failure rate, with
  exponential, capped backoff and half-open trials.
- Active health: TCP connect checks by default, HTTP checks or any function on
  request; rise/fall thresholds, jitter, bounded concurrency, latency
  statistics and failure classification.
- Retries on backends not tried yet, bounded by attempts and a retry budget;
  a dialer and an `http.RoundTripper` over a pool.
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
	return err // ErrNoBackends, ErrUnavailable or ErrClosed
}
conn, err := net.Dial("tcp", lease.Value())
lease.Release(upstream.OutcomeOf(err))
```

A `Lease` holds one in-flight slot of its backend until it is released, with
`Success`, `Failure` or `Neutral` (for example when the client went away).
Release every lease exactly once. Releasing the same lease variable twice is a
no-op. When the outcome is known long before the lease ends, as for a
connection that stays open, `Report` records it at once and `Release` later
only returns the slot; a half-open backend's trial then ends with the report.

`Sync` replaces the membership in one step; `Put` adds or replaces one
backend. A backend keeps its counters and health across updates as long as its
ID stays the same. Leases keep the version of the backend they were issued
with. An empty key carries no affinity: `PickKey("")` is `Pick`.

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

`Attempts` is the same for leases that outlive a function: each `Next` leases
a backend not returned before, or fails with `ErrRetryLimit`,
`ErrRetryBudget` or `ErrUnavailable`.

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
`TLSClientConfig.ServerName` says otherwise.

A panic in a function given to `Do`, `upnet.Dialer` or `uphttp.RoundTripper`
releases the backend's lease and goes on. Where a pool's values cannot serve
as addresses and no function says how to use them, `health.Watch` and
`health.Attach` return `health.ErrNoTarget`, `upnet` returns `ErrNoAddress`
and `uphttp` returns `ErrNoRewrite`.

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
| `Failover()`           | First eligible backend in membership order (active-backup).               |
| `ConsistentHash(cfg)`  | Hash ring keyed by `PickKey`/`PickHash`; optional bounded loads.          |

Consistent hashing places backends by ID, so every process with the same IDs
maps a key to the same backend, and adding or removing one of n backends moves
only about 1/n of the keys. With `LoadFactor` set, a backend already carrying
that multiple of its fair share is passed over for the next one on the ring.

A `Balancer` builds a `Picker` from `Candidate` values and can be used without
a pool. Implement the two interfaces for custom strategies.

## Health and eligibility

A backend takes new picks when it is

- not draining or removed,
- not reported down by active health checks (`HealthUnknown` and `HealthUp`
  both take traffic), and
- not ejected by passive health checking. A half-open backend takes up to
  `HalfOpenRequests` trial requests at a time.

**When no backend is eligible**, `Config.Fallback` decides:

- `FallbackNone` (default): `Pick` returns `ErrUnavailable`.
- `FallbackAll`: panic mode. `Pick` chooses among all members that are not
  draining, ignoring health, on the grounds that a signal marking everything
  down is more often wrong than right. Outcomes still count.

An empty pool returns `ErrNoBackends`; a closed one `ErrClosed`.

### Passive health

`OutlierConfig` ejects a backend after `ConsecutiveFailures` failures in a row,
or when `FailurePercent` of the requests in a sliding `Window` failed (once
`MinRequests` were seen). The ejection lasts `BaseEjection`, doubling with each
ejection that is not followed by a recovery, up to `MaxEjection`. Then the
backend is half-open: `HalfOpenSuccesses` successful trials restore it and
reset the backoff; a failed trial ejects it again. Each ejection is one event,
however many failures follow.

`OutlierDetector` is the same logic for a single target, usable without a pool:
`Admit` before a request, `Record` its outcome after.

### Configuration files

`ParseBalancer` and `ParseFallback` take the names that the built-in balancers
and `Fallback` print, such as `least-conn` and `all`. `State`, `Health`,
`OutlierState`, `Outcome`, `EventKind`, `Fallback` and `health.Reason` encode
and decode as their names in JSON and text. A `Balancer` is an interface: a
configuration names it and `ParseBalancer` builds it.

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

`Subscribe` delivers `EventAdded`, `EventUpdated`, `EventRemoved` and
`EventStateChanged` in order, starting with the current members. Callbacks run
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
  latency covers the connection to that address only, not name resolution.
  The normal close leaves a short-lived TIME_WAIT entry on the checking host;
  `TCPOptions.Reset` closes with a reset instead, which avoids that but may be
  logged by the target. `TCPOptions.Dial` plugs in your own dialer or
  resolver.
- **HTTP check**: `uphttp.Probe(uphttp.Check{...})` as a target's `Probe`,
  with method, path, Host header, expected status range, body match and your
  own client for TLS. It runs only for targets that name it. The default
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
  Adding the key again starts a new generation, and every report carries the
  generation so that buffered reports of an old target can be told apart.
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
use when it was dropped close once they go idle.

## Concurrency and lifetimes

- All types are safe for concurrent use.
- No lock is held while calling a balancer, a clock, a probe or a callback, so
  callbacks may call back into the pool or the checker.
- No method waits for a probe or a callback, so none deadlocks when called
  re-entrantly, `Close` included.
- The pool starts no goroutines; it owns its ejection and slow-start timers,
  which `Close` stops. `Pool.Done` reports that the pool was closed.
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
  each pick; retries on rings of more than 256 backends allocate.
- `Attempts` allocates from the fifth attempt of one request on.
- The failure-rate window counts up to 2³²−1 requests per window and backend;
  the retry budget, up to 2³²−1 requests per window and pool.
- The checker compares targets by key; two groups that give different
  targets for one key share the latest one.

## License

Apache License 2.0
