package fakeclock

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWithTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		want := c.Now().Add(time.Minute)
		ctx, cancel := c.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		child, stop := context.WithCancel(ctx)
		defer stop()
		if d, ok := ctx.Deadline(); !ok || !d.Equal(want) {
			t.Fatalf("deadline %v %v, want %v", d, ok, want)
		}
		c.Advance(time.Minute - time.Nanosecond)
		synctest.Wait()
		if ctx.Err() != nil {
			t.Fatal("ended early")
		}
		c.Advance(time.Nanosecond)
		<-ctx.Done()
		<-child.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatalf("err %v, cause %v", ctx.Err(), context.Cause(ctx))
		}
		if !errors.Is(child.Err(), context.DeadlineExceeded) {
			t.Fatalf("child err %v", child.Err())
		}
	})
}

func TestWithTimeoutCancel(t *testing.T) {
	c := New()
	parent, pcancel := c.WithTimeout(context.Background(), time.Second)
	defer pcancel()
	ctx, cancel := c.WithTimeout(parent, time.Hour)
	if d, _ := ctx.Deadline(); !d.Equal(c.Now().Add(time.Second)) {
		t.Fatalf("deadline %v, want the parent's", d)
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("err %v", ctx.Err())
	}
	<-ctx.Done()
	if c.Pending() != 1 {
		t.Fatalf("%d timers pending, want the parent's only", c.Pending())
	}
}
