// Package leakcheck checks test binaries for leaked goroutines: Find lists
// the goroutines still running besides the caller, the test framework's and
// the runtime's own, waiting a while for them to end first, and
// VerifyTestMain fails a whole test binary with them. Where the runtime has
// the goroutineleak profile, the report adds it: it marks the goroutines
// blocked forever on something no running goroutine can reach. It depends on
// the standard library only.
package leakcheck

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// settleWait bounds how long Find waits for goroutines to end: a goroutine
// that was told to stop may still be on its way out, more so under the race
// detector on a loaded machine.
const settleWait = 5 * time.Second

// Find returns an error listing the goroutines that are still running after
// up to settleWait, besides the caller, the test framework's and the
// runtime's, or nil once there are none.
func Find() error {
	return find(settleWait, nil)
}

// find is Find with its wait, ignoring the goroutines whose ids are in
// ignore.
func find(wait time.Duration, ignore map[uint64]bool) error {
	deadline := time.Now().Add(wait)
	pause := time.Millisecond
	for {
		left := slices.DeleteFunc(goroutines(), func(g goroutine) bool { return ignore[g.id] || g.expected() })
		if len(left) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			var b strings.Builder
			_, _ = fmt.Fprintf(&b, "leakcheck: %d goroutine(s) still running:", len(left))
			for _, g := range left {
				b.WriteString("\n\n")
				b.WriteString(g.stack)
			}
			if p := leakProfile(); p != "" {
				b.WriteString("\n\n")
				b.WriteString(p)
			}
			return fmt.Errorf("%s", b.String())
		}
		t := time.NewTimer(pause)
		<-t.C
		pause = min(2*pause, 100*time.Millisecond)
	}
}

// VerifyTestMain runs the tests of m and, when they passed, fails the binary
// if goroutines are left running. Call it from TestMain:
//
//	func TestMain(m *testing.M) { leakcheck.VerifyTestMain(m) }
func VerifyTestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if err := Find(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err) // nowhere to report a failed write
			code = 1
		}
	}
	os.Exit(code)
}

// leakProfile returns the goroutineleak profile when it lists goroutines,
// else "". Taking it runs a garbage collection. Runtimes without the profile
// return "".
func leakProfile() string {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return ""
	}
	var b bytes.Buffer
	if err := p.WriteTo(&b, 1); err != nil || strings.HasPrefix(b.String(), "goroutineleak profile: total 0\n") {
		return ""
	}
	return b.String()
}

// goroutine is one goroutine of a runtime.Stack dump.
type goroutine struct {
	id    uint64
	top   string // the function running, without its arguments
	stack string // the goroutine's whole dump
}

// expected reports a goroutine that is no leak: one of the test framework
// waiting for its tests, or the runtime's signal, trace and js/wasm event
// handling.
func (g goroutine) expected() bool {
	switch g.top {
	case "testing.RunTests", "testing.(*T).Run", "testing.(*T).Parallel", "testing.runFuzzTests", "testing.runFuzzing",
		"testing.(*F).Fuzz",
		"os/signal.signal_recv", "os/signal.loop", "runtime.ensureSigM", "runtime.ReadTrace":
		return true
	}
	return strings.Contains(g.stack, "runtime/trace.Start") || strings.Contains(g.stack, "runtime.handleEvent")
}

// goroutines returns every goroutine but the caller's.
func goroutines() []goroutine {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var out []goroutine
	for i, dump := range strings.Split(string(buf), "\n\n") {
		if i == 0 {
			continue // the first is the caller's own
		}
		if g, ok := parse(dump); ok {
			out = append(out, g)
		}
	}
	return out
}

// parse parses one dump: "goroutine N [state]:", then the running function
// with its arguments.
func parse(dump string) (goroutine, bool) {
	head, rest, ok := strings.Cut(dump, "\n")
	if !ok {
		return goroutine{}, false
	}
	idText, _, ok := strings.Cut(strings.TrimPrefix(head, "goroutine "), " ")
	id, err := strconv.ParseUint(idText, 10, 64)
	if !ok || err != nil {
		return goroutine{}, false
	}
	fn, _, _ := strings.Cut(rest, "\n")
	if i := strings.LastIndexByte(fn, '('); i > 0 {
		fn = fn[:i]
	}
	return goroutine{id: id, top: fn, stack: dump}, true
}
