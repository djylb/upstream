package health

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/upstream"
	"github.com/djylb/upstream/internal/fakeclock"
)

// Pool 1 has "x" at address "old" (down). Pool 2 then gives "x" another
// address. Pool 1's member never hears again, and stays down.
func TestReviewSharedIDDivergentTargetStuck(t *testing.T) {
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
func TestReviewLateAttachMissesVerdict(t *testing.T) {
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
