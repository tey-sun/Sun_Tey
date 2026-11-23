package httpx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// IDM/aria2-style connection budget (M1 / R1).
//
// aria2 treats the connection budget as TWO independent knobs: -k/--min-split-size
// (never split a range below 2x that size), -x (cap per single server) and -s
// (total for the whole download). The previous implementation had one knob only
// (task.Conn) and always split the file into exactly Conn equal halves, so a
// 2 MB file became 16 x 128 KB requests while a 20 GB file was still only cut
// into 16 huge blocks - the tail of one slow connection held the whole task up.
// ---------------------------------------------------------------------------

const (
	// MinSplitSizeDefault is aria2's min-split-size equivalent: a resource
	// smaller than 2x this is downloaded on ONE connection (no split), and no
	// segment is ever smaller than this.
	MinSplitSizeDefault int64 = 4 << 20
	// MaxConnPerHostDefault caps simultaneous connections to ONE server
	// (aria2 -x). Anti-leech CDNs throttle per-IP connection counts, so more
	// legs than this is counter-productive (that was "??????").
	MaxConnPerHostDefault = 8
	// MaxConnsDefault caps connections across ALL tasks of this core
	// (aria2 -s). One task may not starve the queue of the others.
	MaxConnsDefault = 16
	// SegmentsPerLegDefault is how many work units one connection is given on
	// average. Fan-out > 1 is what makes "steal" possible at all: a static
	// one-chunk-per-connection layout cannot rebalance a slow connection.
	SegmentsPerLegDefault = 8
	// MaxSegmentsDefault bounds the persisted snapshot size.
	MaxSegmentsDefault = 512
	// SegmentFloorDefault is the smallest segment a SPLIT download may use
	// (aria2 ships this as the default --min-split-size). Without such a
	// floor, 8 legs x 8 segments on a 40 MB file means 400 KB fragments and
	// more request overhead than bytes.
	SegmentFloorDefault int64 = 1 << 20
	// StealThresholdDefault is gopeed's stealThresholdSeconds: a connection is
	// only worth stealing from when it still needs longer than this.
	StealThresholdDefault = 3 * time.Second
	// StealMinChunkDefault is gopeed's stealMinChunkSize: never create segments
	// smaller than this just to rebalance (request overhead beats the gain).
	StealMinChunkDefault int64 = 512 << 10
	// StealMaxPerChunkDefault bounds steal ping-pong on one segment.
	StealMaxPerChunkDefault = 8
	// ThrottleCooldownDefault is how long a 429/503 freezes slow-start growth.
	ThrottleCooldownDefault = 10 * time.Second
)

// nextBatch is gopeed's geometric batch growth (1 -> 2 -> 4 -> 16 -> 256,
// i.e. square the batch, special-casing the first step because 1*1 == 1 would
// never grow). Anything past 256 opens the remaining budget at once.
func nextBatch(batch int, max int) int {
	if batch <= 1 {
		batch = 2
	} else if batch <= 16 {
		batch = batch * batch
	} else {
		batch = max
	}
	if batch > max {
		batch = max
	}
	if batch < 1 {
		batch = 1
	}
	return batch
}

// ---------------------------------------------------------------------------
// slowStart: geometric connection ramp-up.
//
// A cold task opens ONE connection. Every time a whole wave of opened
// connections has resolved (response headers seen, or the attempt failed), the
// allowed concurrency grows by the next batch. That behaves like IDM on a fast
// link (reaches the cap in a fraction of a second) and like a polite client on
// a hostile one (a server that refuses connections never sees a thundering
// herd of retries, and a 429 freezes growth entirely).
//
// A leg holds its slot for the whole run (it never re-requests one per
// segment), so the ramp-up is about connections, not work items, and there is
// no grant-fairness problem.
// ---------------------------------------------------------------------------

type ssWaiter struct {
	ch   chan struct{}
	done bool // granted or cancelled: never granted twice
}

type slowStart struct {
	mu          sync.Mutex
	max         int
	allowed     int
	active      int
	pending     int // opened, waiting for the first response byte of the wave
	batch       int
	pausedUntil time.Time
	waiters     []*ssWaiter
	woken       bool // the run is over: every leg may pass through and exit
}

func newSlowStart(max int) *slowStart {
	if max < 1 {
		max = 1
	}
	return &slowStart{max: max, allowed: 1, batch: 1}
}

// acquire parks until this leg may open its connection. Cancellation (pause,
// shutdown) always releases the caller.
func (s *slowStart) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.woken || (s.active < s.allowed && !s.queuedLocked()) {
		s.active++
		s.mu.Unlock()
		return nil
	}
	w := &ssWaiter{ch: make(chan struct{})}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		w.done = true
		s.dropWaiterLocked(w)
		s.dispatchLocked()
		s.mu.Unlock()
		return ctx.Err()
	}
}

// exit releases a leg slot permanently (that leg finished its last segment),
// so a parked leg can take over its slot.
func (s *slowStart) exit() {
	s.mu.Lock()
	if s.active > 0 {
		s.active--
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

func (s *slowStart) queuedLocked() bool {
	for _, w := range s.waiters {
		if !w.done {
			return true
		}
	}
	return false
}

func (s *slowStart) dropWaiterLocked(w *ssWaiter) {
	for i, c := range s.waiters {
		if c == w {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

// wakeAll releases every leg parked in the ramp-up. The run calls it the moment
// the work queue closes: a parked leg must not sit there forever holding the
// run's WaitGroup - the queue says "no more work", so granting it lets the very
// next pop return false and the leg exit.
func (s *slowStart) wakeAll() {
	s.mu.Lock()
	s.woken = true
	for _, w := range s.waiters {
		if !w.done {
			w.done = true
			close(w.ch)
		}
	}
	s.waiters = nil
	s.mu.Unlock()
}

func (s *slowStart) dispatchLocked() {
	for s.active < s.allowed && s.active < s.max {
		var w *ssWaiter
		for _, c := range s.waiters {
			if !c.done {
				w = c
				break
			}
		}
		if w == nil {
			return
		}
		w.done = true
		s.dropWaiterLocked(w)
		s.active++
		close(w.ch)
	}
}

// opened marks one outgoing connection attempt (paired with confirmed/failed).
func (s *slowStart) opened() {
	s.mu.Lock()
	s.pending++
	s.mu.Unlock()
}

// confirmed is gopeed's onConnectSuccess: the server answered, so the next
// batch may open. A wave only advances once every opened connection of the
// previous wave has resolved, which keeps the ramp geometric instead of
// explosive.
func (s *slowStart) confirmed() {
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	if s.pending == 0 && time.Now().After(s.pausedUntil) && s.allowed < s.max {
		s.batch = nextBatch(s.batch, s.max)
		s.allowed += s.batch
		if s.allowed > s.max {
			s.allowed = s.max
		}
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

// failed resolves an attempt that never produced a response. Decrementing
// pending is mandatory (gopeed does the same): a connection that dies in the
// dial must not freeze the ramp-up for everybody else.
func (s *slowStart) failed() {
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

// throttled is 429/503 handling: growth is paused, and the allowed
// concurrency is frozen at what is running right now (in-flight legs keep
// their slots, no new ones start until the cooldown expires).
func (s *slowStart) throttled(cooldown time.Duration) {
	s.mu.Lock()
	s.pausedUntil = time.Now().Add(cooldown)
	if s.allowed > s.active {
		s.allowed = s.active
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

func (s *slowStart) snapshot() (allowed, active, pending int, paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allowed, s.active, s.pending, time.Now().Before(s.pausedUntil)
}

// ---------------------------------------------------------------------------
// connPool: the process-wide connection cap (MaxConns).
// ---------------------------------------------------------------------------

type connPool struct {
	mu      sync.Mutex
	max     int
	used    int
	waiters []chan struct{}
}

func newConnPool(max int) *connPool {
	if max < 1 {
		max = 1
	}
	return &connPool{max: max}
}

// setMax re-sizes the budget (settings change at runtime) and wakes waiters
// when it grew.
func (p *connPool) setMax(max int) {
	if max < 1 {
		max = 1
	}
	p.mu.Lock()
	p.max = max
	p.dispatchLocked()
	p.mu.Unlock()
}

func (p *connPool) limit() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max
}

func (p *connPool) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.used < p.max && len(p.waiters) == 0 {
		p.used++
		p.mu.Unlock()
		return p.releaseFunc(), nil
	}
	w := make(chan struct{})
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case <-w:
		return p.releaseFunc(), nil
	case <-ctx.Done():
		p.mu.Lock()
		for i, c := range p.waiters {
			if c == w {
				p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
				break
			}
		}
		p.dispatchLocked()
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (p *connPool) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			if p.used > 0 {
				p.used--
			}
			p.dispatchLocked()
			p.mu.Unlock()
		})
	}
}

func (p *connPool) dispatchLocked() {
	for p.used < p.max && len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		p.used++
		close(w)
	}
}

// ---------------------------------------------------------------------------
// workQueue: segment queue with requeue support (the steal mechanism) and an
// idle counter (a parked worker is a candidate stealer).
// ---------------------------------------------------------------------------

type workQueue struct {
	mu      sync.Mutex
	items   []int
	waiters []chan struct{}
	closed  bool
	idle    atomic.Int32
}

func (q *workQueue) push(idx int) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, idx)
	q.dispatchLocked()
	q.mu.Unlock()
}

// holds reports whether a segment currently sits in the queue (nobody is
// fetching it). Used by the low-progress signal so queued-but-not-started
// segments are not counted as stalled legs.
func (q *workQueue) holds(idx int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, c := range q.items {
		if c == idx {
			return true
		}
	}
	return false
}

func (q *workQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// idleCount is how many legs are parked waiting for work. >0 with unfinished
// segments means somebody is fast enough to take over a slow connection's
// remainder.
func (q *workQueue) idleCount() int { return int(q.idle.Load()) }

func (q *workQueue) pop(ctx context.Context) (int, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			idx := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return idx, true
		}
		if q.closed {
			q.mu.Unlock()
			return 0, false
		}
		w := make(chan struct{})
		q.waiters = append(q.waiters, w)
		q.mu.Unlock()

		q.idle.Add(1)
		select {
		case <-w:
			q.idle.Add(-1)
		case <-ctx.Done():
			q.idle.Add(-1)
			q.mu.Lock()
			for i, c := range q.waiters {
				if c == w {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
			q.mu.Unlock()
			return 0, false
		}
	}
}

func (q *workQueue) close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	pending := q.waiters
	q.waiters = nil
	for _, w := range pending {
		close(w)
	}
	q.mu.Unlock()
}

func (q *workQueue) dispatchLocked() {
	for len(q.items) > 0 && len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		close(w)
	}
}
