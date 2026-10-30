package health

import (
	"context"
	"time"

	"github.com/djylb/upstream"
)

// A Probe checks one target. It returns nil if the target is healthy, and
// may say which address answered and how long that took. It must return
// promptly once ctx is done: ctx ends at the check timeout and when the
// target is removed.
type Probe func(ctx context.Context, t Target) (Observation, error)

// An Observation is what a Probe saw.
type Observation struct {
	// Addr is the address that answered, such as the IP address and port a
	// host name resolved to. It may be empty.
	Addr string

	// Latency is the time the target took to answer. Zero means the
	// duration of the whole check.
	Latency time.Duration
}

// A Target is something to check.
type Target struct {
	// Probe checks the target. Nil means the default TCP check for the
	// networks "tcp", "tcp4" and "tcp6" (and an empty Network), and no
	// check at all for other networks.
	Probe Probe

	// Network is the target's network, as for net.Dial. Empty means "tcp":
	// the Checker treats the two as the same network.
	Network string

	// Address is the target's address, usually a host and port. A host
	// name is resolved again by every check.
	Address string

	// Options override the Checker's defaults for this target.
	Options Options
}

// Options are the per-target check settings. A target's options are merged
// over the Config's field by field, zero values inheriting, and the
// documented defaults then fill what is still zero, so that defaults
// derived from Interval follow the merged interval.
type Options struct {
	// Interval is the time from the end of one check to the start of the
	// next. The default is 10s.
	Interval time.Duration

	// UnhealthyInterval replaces Interval while the target is down, to
	// detect recovery sooner or to back off. The default is Interval.
	UnhealthyInterval time.Duration

	// Timeout bounds each check. The default is the smaller of 5s and
	// Interval. The probe's context comes from the Checker's
	// Clock.WithTimeout, so its Deadline reports when the check ends; the
	// error of a probe that fails after that wraps
	// context.DeadlineExceeded, so it reads as ReasonTimeout.
	Timeout time.Duration

	// Jitter changes each interval by a random amount between -Jitter and
	// +Jitter, so that targets added together are not checked in step. The
	// default is a tenth of the interval; a negative value disables it.
	Jitter time.Duration

	// Rise is the number of consecutive passing checks that bring a down
	// target up. The default is 2.
	Rise int

	// Fall is the number of consecutive failing checks that take an up
	// target down. The default is 3.
	Fall int

	// Disabled stops checks, canceling any in progress. In a Config it
	// stops them for every target. The health of a target whose checks
	// stop becomes upstream.HealthUnknown.
	Disabled bool
}

// over returns o with its zero fields taken from d.
func (o Options) over(d Options) Options {
	if o.Interval <= 0 {
		o.Interval = d.Interval
	}
	if o.UnhealthyInterval <= 0 {
		o.UnhealthyInterval = d.UnhealthyInterval
	}
	if o.Timeout <= 0 {
		o.Timeout = d.Timeout
	}
	if o.Jitter == 0 {
		o.Jitter = d.Jitter
	}
	if o.Rise <= 0 {
		o.Rise = d.Rise
	}
	if o.Fall <= 0 {
		o.Fall = d.Fall
	}
	o.Disabled = o.Disabled || d.Disabled
	return o
}

// withDefaults fills the zero fields that inherit nothing.
func (o Options) withDefaults() Options {
	if o.Interval <= 0 {
		o.Interval = 10 * time.Second
	}
	if o.UnhealthyInterval <= 0 {
		o.UnhealthyInterval = o.Interval
	}
	if o.Timeout <= 0 {
		o.Timeout = min(5*time.Second, o.Interval)
	}
	if o.Jitter == 0 {
		o.Jitter = o.Interval / 10
	}
	if o.Rise <= 0 {
		o.Rise = 2
	}
	if o.Fall <= 0 {
		o.Fall = 3
	}
	return o
}

// Config configures a Checker. Clock, Concurrency and LatencyWindow are
// fixed when the Checker is created; SetConfig changes the rest.
type Config[K comparable] struct {
	// Clock schedules checks and timestamps results. Nil means
	// upstream.SystemClock. It is the Checker's only time source: check
	// timeouts and the latency the default TCP check measures use it too,
	// whatever TCP.Clock says.
	Clock upstream.Clock

	// OnResult, if not nil, is called after every check, and when a
	// target's health becomes upstream.HealthUnknown because its checks
	// stopped or its network or address changed.
	//
	// All callbacks run one at a time, in the order of the events, on one
	// goroutine the Checker owns, without any lock held. They may call any
	// method of the Checker, including Close; waiting on Done from a
	// callback waits for itself. Once Remove, RemoveFromGroup,
	// RemoveGroup, SyncGroup or an address change has removed a target, no
	// callback for it starts; one already running may finish, as with
	// time.Timer.Stop. Reports carry the target's Generation so that
	// consumers that buffer them can tell an old target from a new one under
	// the same key. A callback that blocks holds back later callbacks but
	// not checks; while it blocks, results that change nothing are
	// coalesced per target, so only transitions accumulate.
	OnResult func(Report[K])

	// OnChange is like OnResult but is called only for reports that
	// change the health: one call per transition.
	OnChange func(Report[K])

	// OnRemove, if not nil, is called once for every generation of a
	// target that ends: when the target leaves the Checker by Remove,
	// RemoveFromGroup, RemoveGroup or SyncGroup, and when a change of its
	// network or address starts a new generation under the same key. It
	// gets the key and the generation that ended; no other callback for
	// that generation follows it.
	OnRemove func(key K, generation uint64)

	// TCP configures the default TCP check.
	TCP TCPOptions

	// Options are the defaults for every target.
	Options

	// Concurrency is the most checks that run at the same time; further
	// checks wait for a free slot without their timeout running. A slow
	// target holds one slot and does not delay others while slots are
	// free. The default is 16.
	Concurrency int

	// LatencyWindow is the number of recent latencies that Latency.Min and
	// Latency.Max cover. The default is 16.
	LatencyWindow int
}

// Reason classifies why a check failed.
type Reason uint8

const (
	// ReasonNone means the check passed.
	ReasonNone Reason = iota

	// ReasonRefused means the target refused the connection: the host is
	// up but nothing listens on the port.
	ReasonRefused

	// ReasonTimeout means the check timed out.
	ReasonTimeout

	// ReasonUnreachable means the host or network is unreachable or down,
	// or no local address can reach it.
	ReasonUnreachable

	// ReasonDNS means the host name could not be resolved.
	ReasonDNS

	// ReasonReset means the connection was reset or aborted.
	ReasonReset

	// ReasonResponse means the target answered, but not as expected, such
	// as with a wrong HTTP status.
	ReasonResponse

	// ReasonOther means any other failure.
	ReasonOther
)

// Result is the outcome of one check and the target's health after it.
//
// The embedded CheckResult holds what the probe saw, as a pool keeps it:
// Time is when the check started, Err why it failed, Addr the address
// that answered, if the probe reported it, and Latency the time the target
// took to answer; for the default TCP check, the time from starting the
// connection to the address that answered until it was established,
// excluding name resolution. Latency has the resolution of the platform's
// monotonic clock, which is coarse on Windows, so a local target may read
// as zero.
type Result struct {
	upstream.CheckResult

	// Reason classifies Err.
	Reason Reason

	// Health is the target's health after this result. Before the first
	// check it is upstream.HealthUnknown; the first check decides it at
	// once; after that Rise or Fall consecutive results are needed to
	// change it.
	Health upstream.Health

	// Previous is the target's health before this result.
	Previous upstream.Health
}

// Changed reports whether this result changed the health. Changes are
// edge-triggered: a target that keeps failing is reported down once.
func (r Result) Changed() bool { return r.Health != r.Previous }

// A Report is a Result together with the target it belongs to.
type Report[K comparable] struct {
	Result

	// Stats summarizes the target's latencies after this result, for
	// callers that report latency changes without calling Status.
	Stats LatencyStats

	// Target is the target as it was checked.
	Target Target

	// Key is the target's key.
	Key K

	// Generation identifies the target under its key: it is new every
	// time the key is added, and when its address changes.
	Generation uint64
}

// LatencyStats summarizes the latencies of passing checks.
type LatencyStats struct {
	// Last is the latest latency.
	Last time.Duration

	// Average is an exponentially weighted moving average in which each
	// new latency has a weight of one quarter.
	Average time.Duration

	// Min and Max are the extremes of the last Samples latencies.
	Min, Max time.Duration

	// Samples is the number of latencies Min and Max cover, at most
	// Config.LatencyWindow.
	Samples int
}

// Status is a snapshot of one target.
type Status[K comparable] struct {
	// Last is the latest check result; its Time is zero before the first
	// check.
	Last Result

	// Stats summarizes the latencies of the passing checks.
	Stats LatencyStats

	// Target is the target.
	Target Target

	// Key is the target's key.
	Key K

	// Generation identifies the target under its key; see Report.
	Generation uint64

	// Checks and Failures count the checks run and failed.
	Checks, Failures uint64

	// Health is the target's current health.
	Health upstream.Health

	// Checking reports whether checks are scheduled: the target is enabled
	// and has a check for its network.
	Checking bool
}
