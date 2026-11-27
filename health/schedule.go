package health

import (
	"time"

	"github.com/djylb/upstream"
)

// A schedule is a min-heap of deadlines. Each slot stores its index in the
// entry or in the entry's damping state so either event can be removed.
type schedule[K, G comparable] []slot[K, G]

type slot[K, G comparable] struct {
	due   time.Time
	e     *entry[K, G]
	index *int
}

func (s *schedule[K, G]) add(due time.Time, e *entry[K, G], index *int) {
	*s = append(*s, slot[K, G]{due: due, e: e, index: index})
	*index = len(*s)
	s.up(*index - 1)
}

func (s *schedule[K, G]) remove(index int) {
	if index == 0 {
		return
	}
	i, last := index-1, len(*s)-1
	s.swap(i, last)
	*(*s)[last].index = 0
	(*s)[last] = slot[K, G]{}
	*s = (*s)[:last]
	if i < last {
		s.down(i)
		s.up(i)
	}
}

func (s *schedule[K, G]) less(i, j int) bool { return (*s)[i].due.Before((*s)[j].due) }

func (s *schedule[K, G]) swap(i, j int) {
	slots := *s
	slots[i], slots[j] = slots[j], slots[i]
	*slots[i].index, *slots[j].index = i+1, j+1
}

func (s *schedule[K, G]) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !s.less(i, parent) {
			return
		}
		s.swap(i, parent)
		i = parent
	}
}

func (s *schedule[K, G]) down(i int) {
	for {
		l := 2*i + 1
		if l >= len(*s) {
			return
		}
		m := l
		if r := l + 1; r < len(*s) && s.less(r, l) {
			m = r
		}
		if !s.less(m, i) {
			return
		}
		s.swap(m, i)
		i = m
	}
}

// schedule replaces the next check, or cancels it and clears its verdict
// and damping history when the target stops being checkable.
func (c *Checker[K, G]) schedule(e *entry[K, G]) {
	now := c.clock.Now()
	c.mu.Lock()
	if c.closed || e.gone {
		c.mu.Unlock()
		return
	}
	var w work[K, G]
	if !c.checkableLocked(e) {
		c.stopCheckingLocked(e, now, &w)
	} else if !e.running {
		c.sched.remove(e.slot)
		e.timer++
		c.sched.add(now.Add(c.delayLocked(e, now)), e, &e.slot)
	}
	a := c.armLocked()
	c.mu.Unlock()
	c.finish(&w)
	c.arm(a)
}

func (c *Checker[K, G]) stopCheckingLocked(e *entry[K, G], now time.Time, w *work[K, G]) {
	c.sched.remove(e.slot)
	e.timer++
	c.clearDampingLocked(e)
	c.stopIdleTimerLocked(w)
	if e.cancel != nil {
		w.cancels = append(w.cancels, e.cancel)
		e.cancel = nil
	}
	e.ok, e.fail, e.hasTouch = 0, 0, false
	if e.health != upstream.HealthUnknown {
		prev := e.health
		e.health = upstream.HealthUnknown
		c.reportLocked(e, Result{CheckResult: upstream.CheckResult{Time: now}, Health: upstream.HealthUnknown, Previous: prev}, false)
	}
}

func (c *Checker[K, G]) stopIdleTimerLocked(w *work[K, G]) {
	if len(c.sched) == 0 && len(c.releases) == 0 && c.armed {
		if c.armStop != nil {
			w.stops = append(w.stops, c.armStop)
		}
		c.armStop, c.armed = nil, false
		c.armSeq++
	}
}

// An arming is a timer change decided under the mutex and made without it.
type arming struct {
	due   time.Time
	stop  func() bool
	seq   uint64
	start bool
}

// armLocked chooses the earliest release or check that can run. Releases
// remain eligible even when all concurrency slots are occupied by probes.
func (c *Checker[K, G]) armLocked() arming {
	check := len(c.sched) != 0 && c.active < c.limit
	release := len(c.releases) != 0
	if c.closed || (!check && !release) {
		return arming{}
	}
	var due time.Time
	if release && (!check || c.releases[0].due.Before(c.sched[0].due)) {
		due = c.releases[0].due
	} else {
		due = c.sched[0].due
	}
	if c.armed && !due.Before(c.armAt) {
		return arming{}
	}
	c.armSeq++
	a := arming{due: due, stop: c.armStop, seq: c.armSeq, start: true}
	c.armAt, c.armStop, c.armed = due, nil, true
	c.pending++
	return a
}

func (c *Checker[K, G]) arm(a arming) {
	if a.stop != nil && a.stop() {
		c.unannounce()
	}
	if !a.start {
		return
	}
	stop := c.clock.AfterFunc(max(a.due.Sub(c.clock.Now()), 0), func() { c.fire(a.seq) })
	c.mu.Lock()
	if c.closed || !c.armed || c.armSeq != a.seq {
		c.mu.Unlock()
		if stop() {
			c.unannounce()
		}
		return
	}
	c.armStop = stop
	c.mu.Unlock()
}

func (c *Checker[K, G]) fire(seq uint64) {
	c.enter()
	defer c.leave()
	now := c.clock.Now()
	c.mu.Lock()
	if c.armSeq == seq {
		c.armStop, c.armed = nil, false
	}
	c.startDueLocked(now)
}

// startDueLocked releases held verdicts, fills free probe slots and lets go
// of mu before arming the timer.
func (c *Checker[K, G]) startDueLocked(now time.Time) {
	if c.closed {
		c.mu.Unlock()
		return
	}
	for len(c.releases) > 0 && !c.releases[0].due.After(now) {
		c.releaseDampingLocked(c.releases[0].e)
	}
	for c.active < c.limit && len(c.sched) > 0 && !c.sched[0].due.After(now) {
		c.active++
		e := c.sched[0].e
		c.sched.remove(e.slot)
		c.pending++
		go c.run(e, e.timer)
	}
	var w work[K, G]
	c.stopIdleTimerLocked(&w)
	a := c.armLocked()
	c.mu.Unlock()
	c.finish(&w)
	c.arm(a)
}
