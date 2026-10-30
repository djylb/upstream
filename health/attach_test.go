package health

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
)

func mustWatch[T any](t *testing.T, ctx context.Context, p *upstream.Pool[T], target func(*upstream.Backend[T]) Target, cfg Config[string]) *Watcher {
	t.Helper()
	w, err := Watch(ctx, p, target, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustAttach[G comparable, T any](t *testing.T, c *Checker[string, G], g G, p *upstream.Pool[T], target func(*upstream.Backend[T]) Target) func() {
	t.Helper()
	detach, err := Attach(c, g, p, target)
	if err != nil {
		t.Fatal(err)
	}
	return detach
}

func newPool(t *testing.T, ts ...upstream.Target[string]) *upstream.Pool[string] {
	t.Helper()
	p := upstream.New[string](upstream.Config{})
	t.Cleanup(p.Close)
	if err := p.Sync(ts); err != nil {
		t.Fatal(err)
	}
	return p
}

func healthOf(t *testing.T, p *upstream.Pool[string], id string) upstream.Health {
	t.Helper()
	b, ok := p.Backend(id)
	if !ok {
		t.Fatalf("no member %s", id)
	}
	return b.Status().Health
}

// TestCloseResetsPoolHealth checks that a Watcher closed before its pool,
// by Close or by its context, does not leave members out of rotation on
// its last verdict.
func TestCloseResetsPoolHealth(t *testing.T) {
	for _, viaContext := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			p := newPool(t, upstream.Target[string]{ID: "a", Value: "a"})
			sw := newSwitchable()
			sw.set("a", true)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w := mustWatch(t, ctx, p, func(b *upstream.Backend[string]) Target { return sw.target(b.Value()) },
				Config[string]{Clock: fakeclock.New()})
			synctest.Wait()
			if _, err := p.Pick(); !errors.Is(err, upstream.ErrUnavailable) {
				t.Fatalf("Pick with the only member down: %v", err)
			}
			if viaContext {
				cancel()
			} else {
				w.Close()
			}
			<-w.Done()
			if h := healthOf(t, p, "a"); h != unknown {
				t.Fatalf("health %v after the watcher closed", h)
			}
			b, _ := p.Backend("a")
			if !errors.Is(b.Status().LastCheck.Err, errDown) {
				t.Fatalf("the last check result was lost: %+v", b.Status().LastCheck)
			}
			l, err := p.Pick()
			if err != nil {
				t.Fatalf("Pick after the watcher closed: %v", err)
			}
			l.Release(upstream.Success)
		})
	}
}

// TestAttachSharesChecks attaches two pools that share a member ID to one
// checker: the target is checked once and reported to both.
func TestAttachSharesChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		sw.set("shared", true)
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Second, Jitter: -1}})
		defer c.Close()
		p1 := newPool(t, upstream.Target[string]{ID: "shared", Value: "shared"}, upstream.Target[string]{ID: "one", Value: "one"})
		p2 := newPool(t, upstream.Target[string]{ID: "shared", Value: "shared"})
		target := func(b *upstream.Backend[string]) Target { return sw.target(b.Value()) }
		detach1 := mustAttach(t, c, 1, p1, target)
		mustAttach(t, c, 2, p2, target)
		synctest.Wait()
		if sw.count("shared") != 1 || healthOf(t, p1, "shared") != down || healthOf(t, p2, "shared") != down || healthOf(t, p1, "one") != up {
			t.Fatalf("checks %d, health %v %v %v", sw.count("shared"), healthOf(t, p1, "shared"), healthOf(t, p2, "shared"), healthOf(t, p1, "one"))
		}
		if keys := c.GroupKeys(1); len(keys) != 2 {
			t.Fatalf("group 1: %v", keys)
		}

		detach1()
		detach1()
		synctest.Wait()
		if healthOf(t, p1, "shared") != unknown || healthOf(t, p1, "one") != unknown {
			t.Fatal("detached pool kept its health verdicts")
		}
		if c.Len() != 1 || len(c.GroupKeys(1)) != 0 {
			t.Fatalf("after detach: %d keys, group 1 %v", c.Len(), c.GroupKeys(1))
		}
		sw.set("shared", false)
		clk.Advance(time.Second)
		synctest.Wait()
		if healthOf(t, p2, "shared") != down { // Rise is 2
			t.Fatal("rose after one check")
		}
		clk.Advance(time.Second)
		synctest.Wait()
		if healthOf(t, p2, "shared") != up || healthOf(t, p1, "shared") != unknown {
			t.Fatalf("health %v, detached %v", healthOf(t, p2, "shared"), healthOf(t, p1, "shared"))
		}

		// Membership changes follow the pool; closing it ends the group.
		if err := p2.Put(upstream.Target[string]{ID: "two", Value: "two"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if keys := c.GroupKeys(2); len(keys) != 2 {
			t.Fatalf("group 2: %v", keys)
		}
		p2.Close()
		synctest.Wait()
		if c.Len() != 0 || len(c.Groups()) != 0 {
			t.Fatalf("after the pool closed: %d keys, groups %v", c.Len(), c.Groups())
		}
	})
}

// TestAttachEndsBeforeAdd checks that a member whose target is computed
// while the attachment ends does not stay in the checker.
func TestAttachEndsBeforeAdd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New[string, int](t.Context(), Config[string]{Clock: fakeclock.New()})
		defer c.Close()
		p := newPool(t)
		inTarget := make(chan struct{})
		release := make(chan struct{})
		detach := mustAttach(t, c, 1, p, func(b *upstream.Backend[string]) Target {
			if b.ID() == "late" {
				close(inTarget)
				<-release
			}
			return Target{Network: "udp", Address: b.ID()}
		})
		go func() { _ = p.Put(upstream.Target[string]{ID: "late"}) }()
		<-inTarget
		detach()
		close(release)
		synctest.Wait()
		if c.Len() != 0 || len(c.GroupKeys(1)) != 0 {
			t.Fatalf("left behind: %d keys", c.Len())
		}
	})
}

// TestAttachReplaysMissedReport checks that a verdict reported before the
// member recorded its generation reaches the member once it has, through
// the Checker's replay, in order with later reports.
func TestAttachReplaysMissedReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sw := newSwitchable()
		sw.set("a", true)
		c := New[string, int](t.Context(), Config[string]{Clock: fakeclock.New(), Options: Options{Interval: time.Hour}})
		defer c.Close()
		p := newPool(t, upstream.Target[string]{ID: "a", Value: "a"})
		b, _ := p.Backend("a")
		target := func(*upstream.Backend[string]) Target { return sw.target("a") }
		a := newAttachment(c, 1, p, target, func(id string, _ Target) string { return id })
		c.mu.Lock()
		c.sinks = append(c.sinks, a.sink)
		c.mu.Unlock()
		gen := c.AddToGroup(1, "a", sw.target("a"))
		synctest.Wait() // the check ran and its report was dropped
		if b.Status().Health != unknown || sw.count("a") != 1 {
			t.Fatalf("health %v after %d checks", b.Status().Health, sw.count("a"))
		}
		a.onEvent(upstream.Event[string]{Kind: upstream.EventAdded, Backend: b})
		synctest.Wait()
		if b.Status().Health != down || sw.count("a") != 1 {
			t.Fatalf("health %v after %d checks: the missed verdict was not replayed", b.Status().Health, sw.count("a"))
		}
		// A report of another generation is dropped.
		a.report(Report[string]{Result: Result{Health: up}, Key: "a", Generation: gen - 1})
		if b.Status().Health != down {
			t.Fatal("an old generation reached the member")
		}
	})
}

func TestAttachNilTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPool(t, upstream.Target[string]{ID: "a", Value: "192.0.2.1:22"})
		w := mustWatch(t, t.Context(), p, nil, Config[string]{Clock: fakeclock.New(), Options: Options{Disabled: true}})
		defer w.Close()
		synctest.Wait()
		s, ok := w.Status("a")
		if !ok || s.Target.Address != "192.0.2.1:22" || s.Target.Network != "" {
			t.Fatalf("status %+v", s)
		}
	})
	ints := upstream.New[int](upstream.Config{})
	defer ints.Close()
	if _, err := Watch(t.Context(), ints, nil, Config[string]{}); !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Watch of ints without a target: %v", err)
	}
	c := New[string, int](t.Context(), Config[string]{})
	defer c.Close()
	if _, err := Attach(c, 1, ints, nil); !errors.Is(err, upstream.ErrNoAddress) {
		t.Fatalf("Attach of ints without a target: %v", err)
	}
}

func TestCheckNowAndReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, Config[string]{Options: Options{Interval: time.Hour}})
		h.sw.latency["a"] = 3 * time.Millisecond
		h.c.Put("a", h.sw.target("a"))
		synctest.Wait()
		if !h.c.CheckNow("a") {
			t.Fatal("CheckNow found no target")
		}
		synctest.Wait()
		if n := h.sw.count("a"); n != 2 {
			t.Fatalf("%d checks after CheckNow", n)
		}
		if h.c.CheckNow("missing") {
			t.Fatal("CheckNow of a missing key")
		}
		h.c.Put("u", Target{Network: "udp", Address: "u"})
		if h.c.CheckNow("u") {
			t.Fatal("CheckNow of an unchecked target")
		}
		h.rep.mu.Lock()
		list := slices.Clone(h.rep.list)
		h.rep.mu.Unlock()
		if len(list) != 2 || list[0].Previous != unknown || list[0].Health != up || !list[0].Changed() || list[1].Changed() {
			t.Fatalf("reports %+v", list)
		}
		if s := list[1].Stats; s.Samples != 2 || s.Last != 3*time.Millisecond {
			t.Fatalf("stats %+v", s)
		}
	})
}

// fakeAddrConn is a connection of a custom dialer without a remote address.
type fakeAddrConn struct{ net.Conn }

func (fakeAddrConn) RemoteAddr() net.Addr { return nil }

func TestTCPCustomDialWithoutAddress(t *testing.T) {
	ctx := context.Background()
	check := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		_ = b.Close()
		return fakeAddrConn{a}, nil
	}})
	obs, err := check(ctx, Target{Address: "x"})
	if err != nil || obs.Addr != "" {
		t.Fatalf("check = %+v, %v", obs, err)
	}
	none := TCP(TCPOptions{Dial: func(context.Context, string, string) (net.Conn, error) { return nil, nil }})
	if _, err := none(ctx, Target{Address: "x"}); !errors.Is(err, errNoConn) {
		t.Fatalf("check of a dial without a connection: %v", err)
	}
}

func TestReasonText(t *testing.T) {
	b, err := ReasonRefused.MarshalText()
	if err != nil || string(b) != "refused" {
		t.Fatalf("%s, %v", b, err)
	}
	var r Reason
	if err := r.UnmarshalText([]byte("response")); err != nil || r != ReasonResponse {
		t.Fatalf("%v, %v", r, err)
	}
	if err := r.UnmarshalText([]byte("weather")); err == nil {
		t.Fatal("decoded an unknown reason")
	}
}

// TestAttachKeysByAddress checks that Attach holds targets by address:
// members with one address share a check, and a member that moves to
// another address leaves the old one.
func TestAttachKeysByAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Second, Jitter: -1}})
		defer c.Close()
		p := newPool(t, upstream.Target[string]{ID: "a", Value: "addr"}, upstream.Target[string]{ID: "b", Value: "addr"})
		mustAttach(t, c, 1, p, func(b *upstream.Backend[string]) Target {
			if b.ID() == "u" {
				return Target{Network: "udp", Address: b.Value()}
			}
			return sw.target(b.Value())
		})
		synctest.Wait()
		if sw.count("addr") != 1 || healthOf(t, p, "a") != up || healthOf(t, p, "b") != up {
			t.Fatalf("checks %d, health %v %v", sw.count("addr"), healthOf(t, p, "a"), healthOf(t, p, "b"))
		}
		p.Remove("a")
		synctest.Wait()
		if keys := c.GroupKeys(1); !slices.Equal(keys, []string{"addr"}) {
			t.Fatalf("keys %v after one of two members left", keys)
		}
		if err := p.Put(upstream.Target[string]{ID: "b", Value: "other"}); err != nil {
			t.Fatal(err)
		}
		if err := p.Put(upstream.Target[string]{ID: "u", Value: "dns"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		keys := c.GroupKeys(1)
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"other", "udp/dns"}) {
			t.Fatalf("keys %v after a move", keys)
		}
	})
}

// Pool 1 has "x" at address "old" (down). Pool 2 then gives "x" another
// address. Pool 1's member never hears again, and stays down.
func TestAttachSharedIDDivergentTargets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		sw.set("old", true)
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Second, Jitter: -1, Rise: 1, Fall: 1}})
		defer c.Close()
		p1 := newPool(t, upstream.Target[string]{ID: "x", Value: "old"}, upstream.Target[string]{ID: "y", Value: "y"})
		target := func(b *upstream.Backend[string]) Target { return sw.target(b.Value()) }
		mustAttach(t, c, 1, p1, target)
		synctest.Wait()
		if healthOf(t, p1, "x") != down {
			t.Fatalf("setup: %v", healthOf(t, p1, "x"))
		}
		p2 := newPool(t, upstream.Target[string]{ID: "x", Value: "new"})
		mustAttach(t, c, 2, p2, target)
		synctest.Wait()
		sw.set("old", false)
		for range 5 {
			clk.Advance(time.Second)
			synctest.Wait()
		}
		if h := healthOf(t, p1, "x"); h == down {
			t.Fatalf("pool 1 member stuck %v; checks of old %d", h, sw.count("old"))
		}
	})
}

// A pool attached after the shared key's first check gets no verdict until
// the next interval.
func TestLateAttachTakesVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		sw.set("s", true)
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Hour, Jitter: -1, Rise: 1, Fall: 1}})
		defer c.Close()
		target := func(b *upstream.Backend[string]) Target { return sw.target(b.Value()) }
		p1 := newPool(t, upstream.Target[string]{ID: "s", Value: "s"})
		mustAttach(t, c, 1, p1, target)
		synctest.Wait()
		p2 := newPool(t, upstream.Target[string]{ID: "s", Value: "s"})
		mustAttach(t, c, 2, p2, target)
		synctest.Wait()
		if healthOf(t, p1, "s") != down || healthOf(t, p2, "s") != down {
			t.Fatalf("p1 %v p2 %v", healthOf(t, p1, "s"), healthOf(t, p2, "s"))
		}
	})
}

func TestWatchEndsWithPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "a"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		clk := fakeclock.New()
		c := mustWatch(t, t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: clk})
		synctest.Wait()
		p.Close()
		<-c.Done()
		if len(c.Statuses()) != 0 || clk.Pending() != 0 {
			t.Fatalf("checker of a closed pool: %d targets, %d timers", len(c.Statuses()), clk.Pending())
		}
	})
}

func TestWatchMatchesGenerations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := upstream.New[string](upstream.Config{})
		defer p.Close()
		if err := p.Sync([]upstream.Target[string]{{ID: "a", Value: "x"}}); err != nil {
			t.Fatal(err)
		}
		sw := newSwitchable()
		c := mustWatch(t, t.Context(), p, func(b *upstream.Backend[string]) Target { return Target{Probe: sw.probe, Address: b.Value()} },
			Config[string]{Clock: fakeclock.New()})
		defer c.Close()
		synctest.Wait()
		old, _ := c.Status("a")
		// Remove and re-add with the same address: a new generation.
		p.Remove("a")
		if err := p.Put(upstream.Target[string]{ID: "a", Value: "x"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		cur, _ := c.Status("a")
		if cur.Generation == old.Generation {
			t.Fatal("re-added member kept its generation")
		}
		c.c.mu.Lock()
		sink := c.c.sinks[0].fn
		c.c.mu.Unlock()
		sink(Report[string]{Result: Result{Health: down, Previous: up}, Key: "a", Target: old.Target, Generation: old.Generation})
		if b, _ := p.Backend("a"); b.Status().Health == down {
			t.Fatal("a report of the old generation reached the re-added member")
		}
	})
}

// TestAttachNetworkSpellings checks that pools that write the network of
// one address as "" and as "tcp" share one generation, so that neither
// pool's reports are dropped as stale: the member of the pool that was
// down before the other attached recovers with the address.
func TestAttachNetworkSpellings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := fakeclock.New()
		sw := newSwitchable()
		sw.set("x", true)
		c := New[string, int](t.Context(), Config[string]{Clock: clk, Options: Options{Interval: time.Second, Jitter: -1, Rise: 1, Fall: 1}})
		defer c.Close()
		pa := newPool(t, upstream.Target[string]{ID: "a", Value: "x"})
		pb := newPool(t, upstream.Target[string]{ID: "b", Value: "x"})
		mustAttach(t, c, 2, pb, func(b *upstream.Backend[string]) Target { return sw.target(b.Value()) })
		synctest.Wait()
		if healthOf(t, pb, "b") != down {
			t.Fatal("b is not down")
		}
		mustAttach(t, c, 1, pa, func(b *upstream.Backend[string]) Target {
			t := sw.target(b.Value())
			t.Network = "tcp"
			return t
		})
		synctest.Wait()
		_ = pa.Put(upstream.Target[string]{ID: "a", Value: "x", Weight: 2}) // another AddToGroup
		synctest.Wait()
		sw.set("x", false)
		clk.Advance(time.Second)
		synctest.Wait()
		s, _ := c.Status("x")
		if s.Generation != 1 || healthOf(t, pa, "a") != up || healthOf(t, pb, "b") != up {
			t.Fatalf("generation %d, health a %v, b %v", s.Generation, healthOf(t, pa, "a"), healthOf(t, pb, "b"))
		}
		if _, err := pb.Pick(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestAttachClosed checks that Attach reports a closed Checker or pool
// instead of returning a detach function that does nothing.
func TestAttachClosed(t *testing.T) {
	c := New[string, int](t.Context(), Config[string]{Clock: fakeclock.New()})
	p := newPool(t)
	c.Close()
	if _, err := Attach(c, 1, p, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Attach to a closed checker = %v", err)
	}
	c = New[string, int](t.Context(), Config[string]{Clock: fakeclock.New()})
	defer c.Close()
	p.Close()
	if _, err := Attach(c, 1, p, nil); !errors.Is(err, upstream.ErrClosed) {
		t.Fatalf("Attach of a closed pool = %v", err)
	}
}

// TestAttachAddresser checks that a pool of values with an Address method
// is checked without a target function.
func TestAttachAddresser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New[string, int](t.Context(), Config[string]{Clock: fakeclock.New(), Options: Options{Disabled: true}})
		defer c.Close()
		p := upstream.New[zoned](upstream.Config{})
		defer p.Close()
		_ = p.Put(upstream.Target[zoned]{ID: "a", Value: zoned{addr: "10.0.0.1:80", zone: "z1"}})
		mustAttach(t, c, 1, p, nil)
		synctest.Wait()
		if keys := c.GroupKeys(1); !slices.Equal(keys, []string{"10.0.0.1:80"}) {
			t.Fatalf("keys %v", keys)
		}
	})
}

type zoned struct{ addr, zone string }

func (z zoned) Address() string { return z.addr }
