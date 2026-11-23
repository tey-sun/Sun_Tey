package httpx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// errStolen ends one leg's ownership of a segment: a faster connection asked
// for it (stealPass) and the remainder goes back to the shared queue. It is
// NOT a download error - the bytes already written stay valid and the next leg
// resumes at the current offset.
var errStolen = errors.New("segment reassigned to a faster connection")

// runCtx is the shared state of ONE runRanges execution: the segment layout,
// per-segment atomic progress, the connection ramp-up, the work queue and the
// steal registry (one cancel func per in-flight attempt).
//
// Layout immutability is a deliberate rule: segments are planned once by
// planSegments and never re-split while the run lives. Rebalancing hands the
// WHOLE remaining segment to another leg instead of shrinking a range in
// place, so two connections can never be told to write overlapping ranges of a
// re-split, and the persisted snapshot always describes the file on disk
// (M1.5 / R11 depends on that).
type runCtx struct {
	d    *Downloader
	task *model.Task
	f    *os.File
	opts *RunOptions
	wd   *watchdog

	cancel context.CancelFunc

	// layout, immutable after planning (the snapshot builder is the only other
	// reader, and it never mutates)
	chunks []model.ChunkState
	done   []*atomic.Int64

	// steal registry (guarded by mu)
	mu      sync.Mutex
	attempt []context.CancelFunc
	stolen  []bool
	steals  []int

	// lock-free progress clock: written by the copying worker on every
	// buffer, read by the steal pass (no mutex on the hot path)
	lastProgNano []*atomic.Int64

	q  *workQueue
	ss *slowStart

	total   atomic.Int64
	rangeOK atomic.Bool

	errMu  sync.Mutex
	runErr error
}

func (rc *runCtx) chunk(i int) model.ChunkState { return rc.chunks[i] }

func (rc *runCtx) chunkLen(i int) int64 {
	c := rc.chunks[i]
	return c.End - c.Start + 1
}

func (rc *runCtx) fail(err error) {
	rc.errMu.Lock()
	if rc.runErr == nil {
		rc.runErr = err
	}
	rc.errMu.Unlock()
	rc.cancel()
}

func (rc *runCtx) err() error {
	rc.errMu.Lock()
	defer rc.errMu.Unlock()
	return rc.runErr
}

// remaining counts segments that still need bytes.
func (rc *runCtx) remaining() int {
	n := 0
	for i := range rc.chunks {
		if rc.done[i].Load() < rc.chunkLen(i) {
			n++
		}
	}
	return n
}

// registerAttempt records the cancel func of one in-flight request so the
// coordinator can hand that segment to another leg. The returned func clears
// the slot and consumes a pending steal mark, which is how an attempt that
// ended on its own (short body, idle cut) avoids being reported as stolen.
func (rc *runCtx) registerAttempt(idx int, cancel context.CancelFunc) func() {
	rc.mu.Lock()
	rc.attempt[idx] = cancel
	rc.mu.Unlock()
	return func() {
		// Only the attempt slot is released here. The steal MARK survives: the
		// leg that owned the segment must see it (stealRequested) and hand the
		// remainder back, otherwise a cancel that lands one buffer before the
		// body ended would silently drop the hand-over and the tail would never
		// move again while idle legs watched.
		rc.mu.Lock()
		rc.attempt[idx] = nil
		rc.mu.Unlock()
	}
}

// stealRequested reports (and clears) a steal mark for this attempt. Called by
// downloadChunk at the top of every retry round, so a steal that landed while
// the leg was sleeping between retries still hands the segment over.
func (rc *runCtx) stealRequested(idx int) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.stolen[idx] && rc.attempt[idx] == nil {
		rc.stolen[idx] = false
		return true
	}
	return rc.stolen[idx]
}

// snapshot is the durable progress view: layout + completed bytes per segment.
func (rc *runCtx) snapshot() []model.ChunkState {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := make([]model.ChunkState, len(rc.chunks))
	for i := range rc.chunks {
		c := rc.chunks[i]
		c.Downloaded = rc.done[i].Load()
		out[i] = c
	}
	return out
}

// touch stamps the "this segment is still moving" clock. Called from the copy
// loop: one atomic store per buffer, so the steal pass can tell a slow
// connection from a merely long one without taking any lock.
func (rc *runCtx) touch(idx int) {
	rc.lastProgNano[idx].Store(time.Now().UnixNano())
}

// stealPass implements gopeed's slow-connection stealing (its
// stealThresholdSeconds / stealMinChunkSize pair) on top of the segment queue:
// when at least one leg is idle and another leg holds a still-large segment
// that has not moved for the threshold, that attempt is cut and its remainder
// returns to the queue for the idle leg.
//
// The threshold is what keeps this from turning into thrash: a segment that is
// merely long but healthy is never interrupted, and a segment below
// stealMinChunkSize is cheaper to finish than to hand over. Returns the number
// of segments handed over (0 in the normal case).
func (rc *runCtx) stealPass(threshold time.Duration, minChunk int64, maxSteals int) int {
	if rc.q.idleCount() <= 0 {
		return 0
	}
	now := time.Now()
	bestIdx, bestIdle := -1, time.Duration(0)
	var bestLeft int64
	rc.mu.Lock()
	for i := range rc.chunks {
		if rc.stolen[i] || rc.attempt[i] == nil {
			continue // nobody is fetching it right now
		}
		left := rc.chunkLen(i) - rc.done[i].Load()
		if left < 2*minChunk {
			continue // too small to be worth a hand-over
		}
		if rc.steals[i] >= maxSteals {
			continue // ping-pong guard
		}
		idle := now.Sub(time.Unix(0, rc.lastProgNano[i].Load()))
		if idle < threshold {
			continue // moving: leave it alone
		}
		if idle > bestIdle {
			bestIdx, bestIdle, bestLeft = i, idle, left
		}
	}
	if bestIdx < 0 {
		rc.mu.Unlock()
		return 0
	}
	rc.stolen[bestIdx] = true
	rc.steals[bestIdx]++
	cancel := rc.attempt[bestIdx]
	idx := bestIdx
	rc.mu.Unlock()

	cancel() // the owner returns errStolen and requeues the segment
	rc.opts.emit(model.Event{Type: "steal", TaskID: rc.task.ID, Status: "running",
		Downloaded: rc.total.Load(),
		Err: fmt.Sprintf("慢连接：分块 %d 已 %.0fs 无进展（剩余 %s），转交更快的连接",
			idx, bestIdle.Seconds(), humanBytes(bestLeft)),
		TS: time.Now().UnixMilli()})
	return 1
}

// checkSlow keeps the pre-M1 degradation signal: when, after SlowWindow, half
// or more of the unfinished legs gained zero bytes, the server throttles per
// connection and the manager may reduce the connection count next start.
func (rc *runCtx) checkSlow(startTS time.Time, fired *atomic.Bool) {
	if fired.Load() || time.Since(startTS) < rc.d.SlowWindow {
		return
	}
	var stalled, active int
	var idx []int
	for i := range rc.chunks {
		if rc.done[i].Load() >= rc.chunkLen(i) {
			continue // already complete
		}
		if rc.attemptOf(i) == nil && rc.q.holds(i) {
			continue // queued, not fetching yet
		}
		active++
		idle := time.Since(time.Unix(0, rc.lastProgNano[i].Load()))
		if idle >= rc.d.SlowWindow {
			stalled++
			idx = append(idx, rc.chunks[i].Index)
		}
	}
	if active > 0 && stalled*2 >= active && !fired.Swap(true) {
		rc.opts.slow(idx)
		rc.opts.emit(model.Event{Type: "slow", TaskID: rc.task.ID, Status: "running",
			Downloaded: rc.total.Load(),
			Err: fmt.Sprintf("低进度信号：%d/%d 条连接在 %v 内零进展（腿 %v），服务器按连接限速",
				stalled, active, rc.d.SlowWindow, idx),
			TS: time.Now().UnixMilli()})
	}
}

// finish closes the work queue AND releases every leg parked in the slow-start
// ramp-up. The two must always happen together: a leg waiting for a connection
// grant can only learn "there is no more work" after it is allowed to call pop.
func (rc *runCtx) finish() {
	rc.q.close()
	rc.ss.wakeAll()
}

func (rc *runCtx) attemptOf(i int) context.CancelFunc {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.attempt[i]
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// ---------------------------------------------------------------------------
// Segment planning (R1: three independent knobs, no tiny fragments)
// ---------------------------------------------------------------------------

func (d *Downloader) minSplit() int64 {
	if d.MinSplitSize > 0 {
		return d.MinSplitSize
	}
	return MinSplitSizeDefault
}

// segFloor is the smallest segment the planner lays out once splitting has been
// justified: minSplitSize decides WHETHER a file is worth splitting (aria2 rule:
// below 2x that size, one connection), segFloor decides the smallest piece after
// that (default 1 MiB, so a 6 MiB file still spreads over several legs).
func (d *Downloader) segFloor() int64 {
	if d.SegmentFloor > 0 {
		return d.SegmentFloor
	}
	return SegmentFloorDefault
}

func (d *Downloader) maxPerHost() int {
	if d.MaxConnPerHost > 0 {
		return d.MaxConnPerHost
	}
	return MaxConnPerHostDefault
}

func (d *Downloader) segsPerLeg() int {
	if d.SegmentsPerLeg > 0 {
		return d.SegmentsPerLeg
	}
	return SegmentsPerLegDefault
}

func (d *Downloader) maxSegments() int {
	if d.MaxSegments > 0 {
		return d.MaxSegments
	}
	return MaxSegmentsDefault
}

func (d *Downloader) stealThreshold() time.Duration {
	if d.StealThreshold > 0 {
		return d.StealThreshold
	}
	return StealThresholdDefault
}

func (d *Downloader) stealMinChunk() int64 {
	if d.StealMinChunkSize > 0 {
		return d.StealMinChunkSize
	}
	return StealMinChunkDefault
}

func (d *Downloader) stealMax() int {
	if d.StealMaxPerChunk > 0 {
		return d.StealMaxPerChunk
	}
	return StealMaxPerChunkDefault
}

// planLegs is the effective connection count of one task: the user's setting,
// capped by the per-server cap, and collapsed to ONE connection for anything
// smaller than 2x min-split-size (aria2's rule: splitting a small file only
// multiplies handshake overhead).
func (d *Downloader) planLegs(size int64, conn int) int {
	legs := conn
	if legs < 1 {
		legs = 1
	}
	if capN := d.maxPerHost(); legs > capN {
		legs = capN
	}
	ms := d.minSplit()
	if size < 2*ms {
		return 1
	}
	if maxBySize := int(size / ms); legs > maxBySize {
		legs = maxBySize
		if legs < 1 {
			legs = 1
		}
	}
	return legs
}

// planSegments lays the file out as fixed segments (>= min-split-size, at most
// max-segments of them) and keeps every byte already verified on disk, so
// changing the connection count - or upgrading from the old one-chunk-per-
// connection layout - never throws resume progress away. It returns the
// effective connection count for this run.
func (d *Downloader) planSegments(task *model.Task) int {
	legs := d.planLegs(task.Size, task.Conn)
	segSize := d.segmentSize(task.Size, legs)
	count := int((task.Size + segSize - 1) / segSize)
	if count < 1 {
		count = 1
	}
	if layoutValid(task.Chunks, task.Size) && len(task.Chunks) == count {
		return legs // same plan as the persisted one: progress untouched
	}

	chunks := make([]model.ChunkState, 0, count)
	var pos int64
	for i := 0; i < count; i++ {
		start := pos
		end := pos + segSize - 1
		if i == count-1 || end > task.Size-1 {
			end = task.Size - 1
		}
		if start > end {
			break
		}
		chunks = append(chunks, model.ChunkState{Index: i, Start: start, End: end})
		pos = end + 1
	}
	preservePrior(chunks, task.Chunks)
	task.Chunks = chunks
	var sum int64
	for _, c := range chunks {
		sum += c.Downloaded
	}
	task.Downloaded = sum
	return legs
}

// segmentSize spreads the file over legs x SegmentsPerLeg segments (that
// fan-out is what makes rebalancing possible at all) while honouring the
// min-split floor and the max-segments bound.
func (d *Downloader) segmentSize(size int64, legs int) int64 {
	if legs < 1 {
		legs = 1
	}
	floor := d.segFloor()
	target := size / int64(legs*d.segsPerLeg())
	if target < floor {
		target = floor
	} else if target >= 1<<20 && target%(1<<20) != 0 {
		// Round up to whole MiB above the floor: starts stay readable in the
		// snapshot and survive a connection-count change with fewer re-splits.
		target = (target/(1<<20) + 1) * (1 << 20)
	}
	if maxN := int64(d.maxSegments()); (size+target-1)/target > maxN {
		target = (size + maxN - 1) / maxN
		if target < floor {
			target = floor
		}
	}
	if target < 1 {
		target = 1
	}
	return target
}

// layoutValid checks a persisted chunk list still tiles [0,size-1] with
// contiguous, non-overlapping, increasing ranges. Anything else (the source
// changed size, a snapshot from another plan) is rebuilt, with preservePrior
// carrying over the bytes that are provably still valid.
func layoutValid(chunks []model.ChunkState, size int64) bool {
	if size <= 0 || len(chunks) == 0 {
		return false
	}
	var pos int64
	for i := range chunks {
		c := chunks[i]
		if c.Start != pos || c.End < c.Start || c.End >= size {
			return false
		}
		if c.Downloaded > c.End-c.Start+1 {
			return false
		}
		pos = c.End + 1
	}
	return pos == size
}

// ---------------------------------------------------------------------------
// runRanges: the dynamic scheduler (slow-start ramp + shared queue + stealing)
// ---------------------------------------------------------------------------

func (d *Downloader) runRanges(ctx context.Context, task *model.Task, f *os.File, opts *RunOptions) error {
	legs := d.planSegments(task)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	n := len(task.Chunks)
	rc := &runCtx{
		d: d, task: task, f: f, opts: opts, wd: newWatchdog(d.StallGlobal), cancel: cancel,
		chunks: make([]model.ChunkState, n), done: make([]*atomic.Int64, n),
		attempt: make([]context.CancelFunc, n), stolen: make([]bool, n),
		steals: make([]int, n), lastProgNano: make([]*atomic.Int64, n),
		q:  &workQueue{},
		ss: newSlowStart(legs),
	}
	rc.rangeOK.Store(true)
	copy(rc.chunks, task.Chunks)
	now := time.Now()
	for i := range rc.chunks {
		rc.done[i] = &atomic.Int64{}
		rc.done[i].Store(task.Chunks[i].Downloaded)
		rc.lastProgNano[i] = &atomic.Int64{}
		rc.lastProgNano[i].Store(now.UnixNano())
		rc.total.Add(task.Chunks[i].Downloaded)
	}

	// Leave the task with consistent progress on EVERY return path - pause,
	// error, watchdog, completion. Without this a cancelled run left
	// task.Downloaded at its pre-run value, so callers reading it (CLI resume,
	// tests, the manager fallback) saw 0 and re-downloaded from scratch.
	defer func() {
		var s int64
		for i := range task.Chunks {
			if i < len(rc.done) {
				task.Chunks[i].Downloaded = rc.done[i].Load()
			}
			s += task.Chunks[i].Downloaded
		}
		task.Downloaded = s
	}()

	for i := range rc.chunks {
		if rc.done[i].Load() < rc.chunkLen(i) {
			rc.q.push(i)
		}
	}
	if rc.remaining() == 0 {
		rc.finish()
	}

	rc.wd.start(ctx, cancel, &rc.total, opts, task.ID)
	defer rc.wd.halt()

	stop := make(chan struct{})
	rDone := make(chan struct{})
	go rc.reporter(stop, rDone)
	defer func() { close(stop); <-rDone }() // no persist call races the final save

	g := sync.WaitGroup{}
	workerLegs := legs
	if w := rc.q.size(); w > 0 && workerLegs > w {
		workerLegs = w // never more goroutines than there is work for them
	}
	for i := 0; i < workerLegs; i++ {
		g.Add(1)
		go func() {
			defer g.Done()
			defer rc.ss.exit()
			d.legWorker(ctx, rc)
		}()
	}
	g.Wait()
	rc.finish()

	if !rc.rangeOK.Load() { // server ignored/misaligned Range in production
		return ErrServerNotRange
	}
	if rc.wd.fired.Load() { // global watchdog: zero progress => dead link
		return fmt.Errorf("服务器连接不稳定/已断开：整任务 %v 内零进展（停滞检测）: %w", d.StallGlobal, ErrStalled)
	}
	if err := rc.err(); err != nil { // first worker error
		return err
	}
	// A run cancelled from OUTSIDE (user pause, app shutdown) must surface the
	// cancellation instead of "incomplete: x/y": workers swallow
	// context.Canceled on purpose, because a pause is not an error.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	dn := rc.total.Load()
	if task.Size >= 0 && dn < task.Size {
		return fmt.Errorf("incomplete: %d/%d", dn, task.Size)
	}
	return nil
}

// legWorker occupies one connection slot: it waits for slow-start to grant it,
// then pulls segments off the shared queue until the run ends. Finishing a
// segment never releases the slot (the leg keeps pulling), so a task with fewer
// segments than connections still opens every leg it is allowed to.
func (d *Downloader) legWorker(ctx context.Context, rc *runCtx) {
	// ONE ramp-up slot per leg, held until this goroutine exits (the caller
	// defers rc.ss.exit()). Re-acquiring per segment used to park every leg
	// that had finished its segment inside acquire() forever, because `active`
	// only drops when a goroutine exits: a task with more segments than
	// connections stalled after one segment per leg, and the steal pass never
	// saw an idle leg (M1 acceptance bug, TestStealHandoverSlowLeg).
	if err := rc.ss.acquire(ctx); err != nil {
		return
	}
	for {
		idx, ok := rc.q.pop(ctx)
		if !ok {
			return // queue closed (all done) or cancelled
		}
		if rc.done[idx].Load() >= rc.chunkLen(idx) {
			continue // finished while it waited in the queue
		}
		err := d.downloadChunk(ctx, rc, idx)
		switch {
		case err == nil:
		case errors.Is(err, errStolen):
			rc.q.push(idx) // hand the remainder back to a faster leg
		case errors.Is(err, context.Canceled):
			return
		default:
			rc.fail(fmt.Errorf("chunk %d: %w", idx, err))
			return
		}
	}
}

// reporter is the 500 ms heartbeat of a run: instant-speed progress, the
// persistence snapshot, the low-progress signal, the steal pass and the
// "everything is done" wake-up for parked legs.
func (rc *runCtx) reporter(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	startTS := time.Now()
	pticks := rc.d.persistTicks()
	lastDN := rc.total.Load()
	lastTS := time.Now()
	ticks := 0
	var slowFired atomic.Bool

	for {
		select {
		case <-stop:
			return
		case <-t.C:
			dn := rc.total.Load()
			now := time.Now()

			// Instant speed over the reporting interval. dn/elapsed is a
			// cumulative average: after a resume it stays pinned at the old
			// value for minutes, and users read that as "卡在 0 B/s 不动了".
			var speed float64
			if dt := now.Sub(lastTS).Seconds(); dt > 0 {
				speed = float64(dn-lastDN) / dt
			}
			lastDN, lastTS = dn, now
			rc.opts.emit(model.Event{Type: "progress", TaskID: rc.task.ID, Downloaded: dn,
				Size: rc.task.Size, Speed: speed, TS: now.UnixMilli()})

			rc.checkSlow(startTS, &slowFired)

			// Stealing only helps when the queue cannot feed the idle legs by
			// itself, i.e. at the tail of the download.
			if rc.q.size() == 0 && rc.remaining() > 0 {
				rc.stealPass(rc.d.stealThreshold(), rc.d.stealMinChunk(), rc.d.stealMax())
			}
			if rc.remaining() == 0 {
				rc.finish() // wake parked legs so the run can finish
			}

			ticks++
			if ticks >= pticks {
				ticks = 0
				rc.opts.persistSnapshot(dn, rc.snapshot())
			}
		}
	}
}
