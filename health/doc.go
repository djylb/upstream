// Package health runs active health checks: it probes targets on jittered
// intervals, applies rise and fall thresholds, measures latency and
// classifies failures, and reports results and transitions.
//
// A Checker holds targets under caller-chosen keys. Each target may belong
// to any number of groups, such as the tunnels that use it; a key added by
// several groups is checked once and stops being checked when its last
// group lets go of it. Groups can be synchronized with a full list in one
// call, which suits configuration pushed from elsewhere.
//
// By default a target is checked by connecting over TCP and closing the
// connection at once, without sending any data. Other checks, such as the
// HTTP check in package uphttp, are used only when a target names them.
// There is no generic check for UDP, which has no handshake to observe: UDP
// targets without a custom Probe are not checked and their health stays
// upstream.HealthUnknown.
//
// Watch connects a Checker to an upstream.Pool so that it follows the
// pool's membership and takes failing members out of rotation.
package health
