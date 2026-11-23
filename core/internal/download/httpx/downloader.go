package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

const minChunk = 1 << 16 // 64 KiB floor per connection

// ErrServerNotRange signals that a connection received a full 200 body (or a
// misaligned 206) for a Range request: the server confirmed Range in the probe
// (bytes=0-0 -> 206) but ignores/corrupts Range in production. The
// multi-connection run aborts immediately (no N-way duplicate fetch) and the
// manager downgrades to one sequential stream.
var ErrServerNotRange = errors.New("server returned full body for a range request")

// ErrStalled signals a dead link: attempts keep opening connections that
// deliver no bytes (idle-guard fired) and cumulative progress stays under
// StalledMinProgress, or the whole run made zero progress for StallGlobal.
// The manager should map it to Status=Error with a "服务器连接不稳定/已断开"
// reason (IDM-style), NOT auto-restart into the old endless loop.
var ErrStalled = errors.New("stalled connection: server keeps opening connections that send no data")

// Stall / loop-prevention tunables (defaults).
const (
	MaxStallStreakDefault     = 3                // consecutive idle-guard firings => dead link
	StalledMinProgressDefault = 32 << 10         // cumulative bytes over that streak
	StallGlobalDefault        = 60 * time.Second // whole-run zero-progress watchdog
	SlowWindowDefault         = 60 * time.Second // runRanges low-progress window
	FullRestartMaxDefault     = 3                // 200-ignores-Range full restarts per run
)

// retryableErr marks a transient HTTP status (5xx/429/408) worth retrying.
// A Retry-After value (429/503) rides along, so the wait matches what the
// server asked for instead of the generic exponential backoff - hammering a
// throttling CDN with the old 8 s cap is how an IP gets temporarily banned.
type retryableErr struct {
	status int
	delay  time.Duration
}

func (e *retryableErr) Error() string {
	if e.delay > 0 {
		return fmt.Sprintf("HTTP %d?????? %v ????", e.status, e.delay)
	}
	return fmt.Sprintf("HTTP %d", e.status)
}

// fatalErr marks an attempt error that must end the run with a clear,
// actionable reason: retrying cannot help (dead link, changed content).
type fatalErr struct{ msg string }

func (e *fatalErr) Error() string { return e.msg }

func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	default:
		return code >= 500
	}
}

// idleGuard cancels an attempt context once its stream stops delivering new
// bytes within the window. Without it a "206 headers then silence" connection
// hung stream() forever — the "stuck at 0 B/s in downloading state" failure.
type idleGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	last   atomic.Int64
	fired  atomic.Bool
	done   chan struct{}
	once   sync.Once
}

func newIdleGuard(parent context.Context, window time.Duration) *idleGuard {
	g := &idleGuard{done: make(chan struct{})}
	g.ctx, g.cancel = context.WithCancel(parent)
	g.last.Store(time.Now().UnixNano())
	go func() {
		interval := window / 4
		if interval < 200*time.Millisecond {
			interval = 200 * time.Millisecond
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-g.done:
				return
			case <-t.C:
				if time.Since(time.Unix(0, g.last.Load())) >= window {
					g.fired.Store(true)
					g.cancel()
					return
				}
			}
		}
	}()
	return g
}

func (g *idleGuard) touch() { g.last.Store(time.Now().UnixNano()) }

// add is the stream-facing tick: it resets the idle clock only when new
// bytes actually arrived (cumulative counter advanced), so a connection that
// trickles headers/1-byte frames forever without payload still dies on
// schedule — the IDM-style dead-link cut.
func (g *idleGuard) add(n int64) {
	if n > 0 {
		g.touch()
	}
}
func (g *idleGuard) stop() {
	g.once.Do(func() { close(g.done); g.cancel() })
}

// Downloader performs Range-split parallel HTTP downloads with resume.
type Downloader struct {
	Client *http.Client
	UA     string

	// Resilience tunables. Bounded, backed-off retries replace the old
	// infinite retry loop, which spun for hours with ~0 progress against
	// throttling anti-leech CDNs (the "下载一直循环" symptom).
	MaxAttemptsPerChunk int           // lifetime attempts per chunk per run (IDM-like cap, default 5)
	LowStreakLimit      int           // consecutive attempts gaining < MinUseful AND below MinUsefulRate => give up
	MinUseful           int64         // progress floor that counts as "useful"
	MinUsefulRate       int64         // bytes/sec floor: under this an attempt is a crawl, not progress
	IdleTimeout         time.Duration // abort an attempt after this long w/o new bytes
	RetryBase           time.Duration // first backoff step (x2 growth, +/-20% jitter)
	RetryMax            time.Duration // backoff ceiling (8s)
	PersistEvery        time.Duration // mid-run progress snapshot cadence
	BufferKB            int           // copy buffer per connection (KB, 0 = 128)

	MaxStallStreak     int           // consecutive idle-guard firings before ErrStalled
	StalledMinProgress int64         // streak cumulative progress under which a stall streak is a dead link
	StallGlobal        time.Duration // whole-run watchdog: zero progress this long => ErrStalled
	SlowWindow         time.Duration // runRanges: window in which >= half legs at zero => OnSlow
	FullRestartMax     int           // sequential: max "200 ignores Range" full restarts per run (anti-loop)

	// --- M1 (R1): IDM/aria2-style dynamic connection budget ---------------
	// Three INDEPENDENT knobs, mirroring aria2 --min-split-size / -x / -s, so a
	// 2 MB file is not chopped into 16 fragments while a 20 GB file stays one
	// stream. Zero values fall back to the defaults in dynamic.go.
	MinSplitSize      int64         // never split below 2x this; segments >= this
	SegmentFloor      int64         // smallest segment once a split is justified
	MaxConnPerHost    int           // simultaneous connections to ONE server
	MaxConns          int           // simultaneous connections across ALL tasks
	SegmentsPerLeg    int           // work units per connection on average
	MaxSegments       int           // snapshot-size bound
	StealThreshold    time.Duration // victim must be idle this long to be stolen
	StealMinChunkSize int64         // never hand over a remainder below this
	StealMaxPerChunk  int           // steal ping-pong guard per segment
	ThrottleCooldown  time.Duration // 429/503 freeze window for new connections

	slots     *connPool // MaxConns budget, shared by every task of this core
	slotsOnce sync.Once
}

func NewDownloader(c *http.Client, ua string) *Downloader {
	if c == nil {
		c = NewClient()
	}
	return &Downloader{
		Client:              c,
		UA:                  ua,
		MaxAttemptsPerChunk: 5,
		LowStreakLimit:      3,
		MinUseful:           1 << 16,
		MinUsefulRate:       2 << 10,
		IdleTimeout:         30 * time.Second,
		RetryBase:           500 * time.Millisecond,
		RetryMax:            8 * time.Second,
		PersistEvery:        2 * time.Second,
		MaxStallStreak:      MaxStallStreakDefault,
		StalledMinProgress:  StalledMinProgressDefault,
		StallGlobal:         StallGlobalDefault,
		SlowWindow:          SlowWindowDefault,
		FullRestartMax:      FullRestartMaxDefault,
		MinSplitSize:        MinSplitSizeDefault,
		SegmentFloor:        SegmentFloorDefault,
		MaxConnPerHost:      MaxConnPerHostDefault,
		MaxConns:            MaxConnsDefault,
		SegmentsPerLeg:      SegmentsPerLegDefault,
		MaxSegments:         MaxSegmentsDefault,
		StealThreshold:      StealThresholdDefault,
		StealMinChunkSize:   StealMinChunkDefault,
		StealMaxPerChunk:    StealMaxPerChunkDefault,
		ThrottleCooldown:    ThrottleCooldownDefault,
	}
}

// progressOK reports whether one attempt counted as REAL forward progress:
// it moved >= MinUseful bytes, or it sustained a rate >= MinUsefulRate.
//
// This is the fix for the "下一半就停 / 反复重试后报限速" class of failures. The
// old rule was bytes-only ("attempt < 64 KiB => the CDN throttles me"), so:
//   - a flaky CDN that cuts every connection at 16 KiB looked like throttling,
//     and after 3 such attempts the whole task was aborted (byte progress was
//     actually fine — each drop was instantaneous), and
//   - a big task split over 16 legs on a normal link, where each leg only
//     moves a little per attempt, was killed for the same "reason".
//
// A genuinely dead link (1 KiB in 30 s) looked identical, so the two cases
// could not be separated. Judging bytes AND rate separates them:
// healthy-but-flaky keeps resuming to completion, dead links still fail fast
// with an actionable reason (bounded, never an endless loop).
func (d *Downloader) progressOK(added int64, dur time.Duration) bool {
	if added <= 0 {
		return false
	}
	if added >= d.MinUseful {
		return true
	}
	if d.MinUsefulRate <= 0 {
		return true
	}
	if dur < time.Millisecond {
		dur = time.Millisecond
	}
	return float64(added)/dur.Seconds() >= float64(d.MinUsefulRate)
}

// RunOptions carries the streaming callbacks for one Run.
type RunOptions struct {
	// Emit streams progress/status events (may be nil).
	Emit func(model.Event)
	// Persist receives the authoritative total and, for Range tasks, the
	// per-chunk completed bytes. It is called every PersistEvery WHILE the
	// download runs, so a hard kill/crash loses at most one interval — the
	// fix for "resume restarts from zero after the app was killed".
	// chunks is the WHOLE segment layout with the completed bytes per
	// segment (the plan changes with the connection count, so the manager
	// must store layout and progress together or a resume re-fetches what
	// is already on disk). nil for sequential runs.
	Persist func(total int64, chunks []model.ChunkState)
	// OnStall fires once when the run-level watchdog sees zero progress for
	// StallGlobal (elapsed seconds). The run then settles with an error
	// wrapping ErrStalled ("服务器连接不稳定/已断开"); the manager should mark
	// the task Error with that reason and NOT auto-restart (anti-loop).
	OnStall func(elapsedSeconds float64)
	// OnSlow fires once (runRanges) when >= half of the active legs made zero
	// progress within SlowWindow. Degradation signal: the manager may reduce
	// connection count / downgrade to sequential on the next start.
	// stalledIdx lists the zero-progress leg indices.
	OnSlow func(stalledIdx []int)
}

func (o *RunOptions) emit(e model.Event) {
	if o != nil && o.Emit != nil {
		o.Emit(e)
	}
}

func (o *RunOptions) persistSnapshot(total int64, chunks []model.ChunkState) {
	if o != nil && o.Persist != nil {
		o.Persist(total, chunks)
	}
}

func (o *RunOptions) stall(elapsed float64) {
	if o != nil && o.OnStall != nil {
		o.OnStall(elapsed)
	}
}

func (o *RunOptions) slow(idx []int) {
	if o != nil && o.OnSlow != nil {
		o.OnSlow(idx)
	}
}

// watchdog is the run-level global progress sentinel: when the total stops
// advancing for window (typical: every leg is stalled/throttled), it fires
// once — calls opts.OnStall, emits a "stalled" event and cancels the run,
// which then settles with an error wrapping ErrStalled. IDM treats a dead
// link as an explicit error, not as an endless reconnect loop.
type watchdog struct {
	window   time.Duration
	lastNano atomic.Int64
	prev     atomic.Int64
	fired    atomic.Bool
	stop     chan struct{}
}

func newWatchdog(window time.Duration) *watchdog {
	w := &watchdog{window: window, stop: make(chan struct{})}
	now := time.Now().UnixNano()
	w.lastNano.Store(now)
	return w
}

// add records n newly written bytes; any advance resets the idle clock.
func (w *watchdog) add(n int64) {
	if n <= 0 {
		return
	}
	if w.prev.Add(n) > 0 {
		w.lastNano.Store(time.Now().UnixNano())
	}
}

func (w *watchdog) start(ctx context.Context, cancel context.CancelFunc, total *atomic.Int64, opts *RunOptions, taskID string) {
	if w.window <= 0 {
		return
	}
	interval := w.window / 8
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				el := time.Since(time.Unix(0, w.lastNano.Load()))
				if el >= w.window && !w.fired.Swap(true) {
					opts.stall(el.Seconds())
					opts.emit(model.Event{Type: "stalled", TaskID: taskID, Status: "error",
						Downloaded: total.Load(),
						Err:        fmt.Sprintf("下载已停滞 %.0f 秒无任何进度：服务器连接不稳定/已断开", el.Seconds()),
						TS:         time.Now().UnixMilli()})
					cancel()
					return
				}
			}
		}
	}()
}

func (w *watchdog) halt() { close(w.stop) }

// Run downloads the task to its working file (task.WorkPath = TempPath when the
// manager uses a .suntey-partial intermediate, else SavePath), streaming events via
// opts.Emit and persisting mid-run progress snapshots via opts.Persist.
// Mutates task.Chunks / task.Downloaded. Honors ctx cancellation.
func (d *Downloader) Run(ctx context.Context, task *model.Task, opts *RunOptions) error {
	f, err := os.OpenFile(task.WorkPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	if task.SupportRange && task.Size > 0 {
		if err := f.Truncate(task.Size); err != nil { // preallocate => sparse file, instant
			return fmt.Errorf("prealloc: %w", err)
		}
		return d.runRanges(ctx, task, f, opts)
	}
	return d.runSequential(ctx, task, f, opts) // no Range / unknown size
}

// persistTicks converts PersistEvery into reporter-tick units (500ms each).
func (d *Downloader) persistTicks() int {
	per := d.PersistEvery
	if per <= 0 {
		per = 2 * time.Second
	}
	n := int(per / (500 * time.Millisecond))
	if n < 1 {
		n = 1
	}
	return n
}

// preservePrior maps verified byte intervals from an old chunk layout onto a new
// one, so changing 连接数 in Settings no longer throws resume progress away. The
// old code rebuilt every chunk with Downloaded = 0: the bytes on disk stayed
// where they were, the task dropped back to 0% and re-downloaded the whole file
// (one of the "无法断点续传 / 越下越从头开始" symptoms).
//
// Only the CONTIGUOUS prefix of a new range is kept. A chunk's progress is a
// prefix [Start, Start+Downloaded-1]; after re-splitting, holes can appear
// inside a new range (old chunk A fully done, old chunk B untouched). Anything
// behind a hole must be re-fetched: the writer resumes at Start+Downloaded, so
// claiming bytes past a hole would skip the hole and leave it permanently zero.
func preservePrior(chunks, old []model.ChunkState) {
	if len(old) == 0 {
		return
	}
	iv := make([][2]int64, 0, len(old))
	for _, c := range old {
		if c.Downloaded > 0 {
			iv = append(iv, [2]int64{c.Start, c.Start + c.Downloaded - 1})
		}
	}
	if len(iv) == 0 {
		return
	}
	sort.Slice(iv, func(i, j int) bool { return iv[i][0] < iv[j][0] })
	for i := range chunks {
		c := &chunks[i]
		cur := c.Start
		for _, r := range iv {
			if r[1] < cur {
				continue // interval already consumed
			}
			if r[0] > cur {
				break // hole: everything past it must be re-fetched
			}
			if r[1] >= c.End {
				cur = c.End + 1
				break
			}
			cur = r[1] + 1
		}
		if n := cur - c.Start; n > 0 {
			c.Downloaded = n
		}
	}
}

// downloadChunk fetches [Start+done, End] with BOUNDED, backed-off retries.
// It gives up with an actionable message instead of looping forever when the
// server keeps throttling connections or the link died — the old code retried
// with no cap and no backoff, spinning for hours at ~0 progress on anti-leech
// CDNs (the "下载一直循环" symptom).
//
// On a 200 full body (or misaligned 206) for a partial Range it flips rangeOK,
// cancels the run and returns ErrServerNotRange so the manager can downgrade.
func (d *Downloader) downloadChunk(ctx context.Context, rc *runCtx, idx int) error {
	task, opts, wd := rc.task, rc.opts, rc.wd
	done := rc.done[idx]
	c := rc.chunk(idx) // fixed for the whole run (see runCtx)
	chunkLen := c.End - c.Start + 1
	attempts, failStreak, lowStreak, stallStreak := 0, 0, 0, 0
	var stallProgress int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if rc.stealRequested(idx) {
			return errStolen // a faster connection took this segment over
		}
		if done.Load() >= chunkLen {
			return nil
		}
		if attempts >= d.MaxAttemptsPerChunk {
			return fmt.Errorf("服务器连接不稳定/已断开：已重试 %d 次仍被中断，已暂停（保留已下载部分）；稍后可点击「开始」断点续传（也可在设置中减少连接数）", attempts)
		}
		attempts++
		before := done.Load()
		offset := c.Start + before
		attemptStart := time.Now()
		aerr, idle := d.fetchAttempt(ctx, rc, idx, c, offset)
		attemptDur := time.Since(attemptStart)
		if rc.stealRequested(idx) {
			return errStolen // the steal pass cut this attempt: hand the tail over
		}
		if aerr != nil {
			if errors.Is(aerr, ErrServerNotRange) {
				return aerr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var fe *fatalErr
			if errors.As(aerr, &fe) {
				return aerr
			}
			failStreak++
		} else {
			failStreak = 0 // body ended cleanly (possibly short): not a failure
		}
		added := done.Load() - before
		if added > 0 {
			wd.add(added)
		}
		healthy := d.progressOK(added, attemptDur)
		if healthy {
			// Real forward progress => this is a long download on a flaky link,
			// not a dead one. Clear the give-up budgets: a 4 GB file whose
			// connections drop every few MB must finish, where the old
			// 5-attempts-per-chunk lifetime cap aborted it (the user then saw
			// "retrying forever, never finishes"). The anti-loop guards stay
			// fully intact for links that make NO progress: idle guard, stall
			// streak, low-progress streak and the run watchdog all still fire.
			lowStreak, failStreak = 0, 0
			attempts = 0
		}
		if idle {
			stallProgress += added
			stallStreak++
			if stallStreak >= d.MaxStallStreak && stallProgress < d.StalledMinProgress {
				return fmt.Errorf("服务器连接不稳定/已断开：连续 %d 次连接零字节（%v 无任何数据），链接已死: %w",
					stallStreak, d.IdleTimeout, ErrStalled)
			}
		} else {
			stallStreak, stallProgress = 0, 0
		}
		if !healthy {
			lowStreak++
			if lowStreak >= d.LowStreakLimit {
				return fmt.Errorf("服务器对这条链接限速/中断：连续 %d 次连接每次都只传 %d 字节（平均速率不足 %d B/s，共尝试 %d 次，本段已累计 %d 字节）。建议：在设置中减少连接数后点「开始」续传，或从浏览器扩展重新发送下载（自动刷新 Cookie/Referer）",
					lowStreak, added, d.MinUsefulRate, attempts, done.Load())
			}
		} else {
			lowStreak = 0
		}
		delay := d.nextDelay(failStreak)
		var re *retryableErr
		if errors.As(aerr, &re) && re.delay > 0 {
			// The server named the wait: honour it (capped) instead of the generic
			// backoff, otherwise a 429 turns into a permanent ban.
			delay = re.delay
			if cap := d.RetryMax * 8; delay > cap {
				delay = cap
			}
		}
		level := "info"
		if failStreak >= 2 {
			level = "warn"
		}
		opts.emit(model.Event{Type: "retry", TaskID: task.ID, Status: "running",
			Attempt: attempts, Level: level,
			Err: fmt.Sprintf("分块 %d 连接中断（第 %d 次，%v 后重试）：%s", c.Index, attempts, delay.Round(10*time.Millisecond), errText(aerr)),
			TS:  time.Now().UnixMilli()})
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
	}
}

// fetchAttempt performs ONE ranged GET for [offset, c.End], writing into f as
// it streams. Returns (nil, false) when the body ended (complete or short —
// the caller re-enters and checks progress); (err, true) when the idle
// watchdog cut the stream (dead-link signal for the caller's stall streak);
// (err, false) for other classified errors (retryable transient, fatal,
// ErrServerNotRange).
func (d *Downloader) fetchAttempt(ctx context.Context, rc *runCtx, idx int, c model.ChunkState, offset int64) (error, bool) {
	task, f, done, total := rc.task, rc.f, rc.done[idx], &rc.total
	rangeOK, cancelAll, wd := &rc.rangeOK, rc.cancel, rc.wd

	// Process-wide connection budget: a leg waits here (fair FIFO) instead of
	// letting one task open every socket on the machine (MaxConns).
	release, gerr := d.pool().acquire(ctx)
	if gerr != nil {
		return gerr, false
	}
	defer release()

	// Attempt-scoped cancel: the steal pass cuts exactly this request and
	// nothing else, so the segment can move to an idle leg without touching the
	// run context or the other legs.
	actx, acancel := context.WithCancel(ctx)
	defer acancel()
	unregister := rc.registerAttempt(idx, acancel)
	defer unregister()

	guard := newIdleGuard(actx, d.IdleTimeout)
	defer guard.stop()

	req, err := http.NewRequestWithContext(guard.ctx, http.MethodGet, task.Req.URL, nil)
	if err != nil {
		return err, false
	}
	req.Header.Set("User-Agent", d.UA)
	for k, v := range task.Req.Header {
		req.Header.Set(k, v)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, c.End))

	// Slow start: this attempt is one connection of the current wave; the
	// response decides whether the next batch may open (see slowStart).
	rc.ss.opened()
	resp, err := d.Client.Do(req)
	if err != nil {
		rc.ss.failed()
		if ctx.Err() != nil {
			return ctx.Err(), false
		}
		if guard.fired.Load() {
			return fmt.Errorf("连接空闲超时（%v 内无数据）", d.IdleTimeout), true
		}
		return fmt.Errorf("request: %w", err), false
	}
	rc.ss.confirmed() // wave resolved: slow start may grow
	defer resp.Body.Close()

	fullFileChunk := c.Start == 0 && c.End == task.Size-1
	// ANY 200 to a partial Range means Range is ignored in production — this
	// now also catches chunked 200 bodies (ContentLength == -1), which used to
	// slip through and made every connection re-download the whole file at
	// wrong offsets (corrupted output + inflated progress).
	if resp.StatusCode == http.StatusOK && !fullFileChunk {
		rangeOK.Store(false)
		cancelAll()
		return ErrServerNotRange, false
	}
	// 429/503 is a different animal from a dead link: the resource is fine, the
	// server is protecting itself. Freeze connection growth (gopeed does the
	// same with paused=true), wait the requested time, and never treat it as
	// "the link is dead" nor as a reason to open MORE connections.
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		rc.ss.throttled(d.ThrottleCooldown)
		return &retryableErr{status: resp.StatusCode, delay: retryAfter(resp.Header.Get("Retry-After"))}, false
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		if retryableStatus(resp.StatusCode) {
			return &retryableErr{status: resp.StatusCode}, false
		}
		return &fatalErr{fmt.Sprintf("HTTP %d：链接无效或被拒绝（多半是直链已过期）。从浏览器扩展重新发送下载（自动刷新 Cookie/Referer），再点「开始」断点续传", resp.StatusCode)}, false
	}
	if resp.StatusCode == http.StatusPartialContent {
		if st, _, tot, ok := parseContentRange(resp.Header.Get("Content-Range")); ok {
			if st != offset { // 206 but wrong bytes: same class of lie
				rangeOK.Store(false)
				cancelAll()
				return ErrServerNotRange, false
			}
			if tot > 0 && tot != task.Size {
				return &fatalErr{fmt.Sprintf("服务器文件大小已变化（%d != %d 字节），请删除任务后重新添加", tot, task.Size)}, false
			}
		}
	}

	// Never write beyond this chunk's boundary, whatever the body claims.
	remain := c.End - offset + 1
	bw := &countingReader{r: io.LimitReader(resp.Body, remain)}
	if _, serr := d.stream(guard.ctx, f, offset, bw, done, total,
		func(n int64) { guard.add(n); wd.add(n); rc.touch(idx) }); serr != nil {
		if ctx.Err() != nil {
			return ctx.Err(), false
		}
		if guard.fired.Load() {
			return fmt.Errorf("连接空闲超时（%v 内无数据）", d.IdleTimeout), true
		}
		return serr, false
	}
	if bw.n >= remain {
		return nil, false // chunk region fully served
	}
	if bw.n > 0 {
		return nil, false // short body with real progress: resume at new offset
	}
	// 2xx accepted, body yielded zero bytes, ended cleanly => dead link (the
	// server opened the connection and served an empty body every time).
	return errors.New("空响应体（0 字节）：服务器连接不稳定/已断开"), true
}

// countingReader records how many bytes the body actually yielded, so an
// attempt can distinguish "short body with real progress" (resume point
// moved — healthy) from "2xx then nothing" (dead link — stall).
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// stream copies body to f at the absolute file offset base+written, advancing
// the per-chunk counter (done) and the run total. Both are atomic so the
// progress reporter and the persistence snapshot can read them without racing
// the workers. touch() resets the idle watchdog on every received chunk.
func (d *Downloader) stream(ctx context.Context, f *os.File, base int64, body io.Reader, done, total *atomic.Int64, touch func(n int64)) (int64, error) {
	bufSize := d.BufferKB * 1024
	if bufSize < 4096 {
		bufSize = 128 * 1024
	}
	buf := make([]byte, bufSize)
	var written int64
	for {
		// Honour cancellation mid-body: when one leg detects a Range lie
		// (200 to a partial Range), cancelAll stops the whole run; without this
		// check every other leg keeps draining its full duplicate body before
		// the run settles — 4-8x download duplication on lying CDNs.
		if ctx.Err() != nil {
			return written, ctx.Err()
		}
		rn, rerr := body.Read(buf)
		if rn > 0 {
			for off := int64(0); off < int64(rn); {
				n, werr := f.WriteAt(buf[off:rn], base+written)
				off += int64(n)
				written += int64(n)
				done.Add(int64(n))
				total.Add(int64(n))
				if werr != nil {
					return written, werr
				}
			}
			if touch != nil {
				touch(int64(rn))
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return written, nil
			}
			return written, rerr
		}
	}
}

// runSequential: single stream with bounded, backed-off retries. Resume via
// "Range: bytes=<downloaded>-" when the server honors it, restart from 0 when
// it does not. A single transient connection drop no longer kills the whole
// task — it retries from the current offset instead of erroring out.
func (d *Downloader) runSequential(ctx context.Context, task *model.Task, f *os.File, opts *RunOptions) error {
	var total atomic.Int64
	total.Store(task.Downloaded)
	chunkLocal := &atomic.Int64{} // stream accounting; snapshots use nil chunkDone
	defer func() { task.Downloaded = total.Load() }()
	// Trim the file to the true content length when the run ends: a "200 ignores
	// Range" restart (and servers that stream past Content-Length) leaves stale
	// bytes past the verified end, which shows up as a file that is a few
	// hundred KB "too large" and unplayable at the tail — the classic
	// "下载完打不开/尾部损坏" report.
	defer func() {
		if task.Size > 0 {
			if n := total.Load(); n >= task.Size {
				_ = f.Truncate(task.Size)
			}
		} else {
			_ = f.Truncate(total.Load())
		}
	}()

	stop := make(chan struct{})
	rDone := make(chan struct{})
	defer func() { close(stop); <-rDone }()
	pticks := d.persistTicks()
	go func() {
		defer close(rDone)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		lastDN := total.Load()
		lastTS := time.Now()
		ticks := 0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				dn := total.Load()
				now := time.Now()
				var speed float64
				if dt := now.Sub(lastTS).Seconds(); dt > 0 {
					speed = float64(dn-lastDN) / dt
				}
				lastDN, lastTS = dn, now
				opts.emit(model.Event{Type: "progress", TaskID: task.ID, Downloaded: dn, Size: task.Size, Speed: speed, TS: now.UnixMilli()})
				ticks++
				if ticks >= pticks {
					ticks = 0
					opts.persistSnapshot(dn, nil)
				}
			}
		}
	}()

	attempts, failStreak, lowStreak := 0, 0, 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cur := total.Load()
		if task.Size > 0 && cur >= task.Size {
			return nil // complete
		}
		if attempts >= d.MaxAttemptsPerChunk {
			return fmt.Errorf("已重试 %d 次连接仍被中断，已暂停；稍后可以点击「开始」断点续传", attempts)
		}
		attempts++
		before := total.Load()
		attemptStart := time.Now()
		aerr := d.seqAttempt(ctx, task, f, before, chunkLocal, &total)
		attemptDur := time.Since(attemptStart)
		if aerr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var fe *fatalErr
			if errors.As(aerr, &fe) {
				return aerr
			}
			failStreak++
		} else {
			failStreak = 0
			if task.Size <= 0 {
				return nil // unknown size: clean EOF ends the stream
			}
		}
		added := total.Load() - before
		healthy := d.progressOK(added, attemptDur)
		if healthy {
			// Forward progress at a sane rate: keep going (a 4 GB single-stream
			// download on a connection that drops every few minutes is normal,
			// not a dead link), while links that crawl/dribble still hit the
			// low-progress streak below and fail bounded.
			lowStreak, failStreak = 0, 0
			attempts = 0
		}
		if !healthy {
			lowStreak++
			if lowStreak >= d.LowStreakLimit {
				return fmt.Errorf("服务器对这条链接限速/中断：连续 %d 次连接每次都只传 %d 字节（平均速率不足 %d B/s，共尝试 %d 次，已下载 %d 字节）。建议稍后点「开始」续传，或从浏览器扩展重新发送下载（自动刷新 Cookie/Referer）",
					lowStreak, added, d.MinUsefulRate, attempts, total.Load())
			}
		} else {
			lowStreak = 0
		}
		delay := d.nextDelay(failStreak)
		level := "info"
		if failStreak >= 2 {
			level = "warn"
		}
		opts.emit(model.Event{Type: "retry", TaskID: task.ID, Status: "running",
			Attempt: attempts, Level: level,
			Err: fmt.Sprintf("单连接中断（第 %d 次，%v 后重试）：%s", attempts, delay.Round(10*time.Millisecond), errText(aerr)),
			TS:  time.Now().UnixMilli()})
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
	}
}

// seqAttempt performs one sequential GET (resuming at base0 when > 0),
// streaming from the verified body offset into f.
func (d *Downloader) seqAttempt(ctx context.Context, task *model.Task, f *os.File, base0 int64, chunkLocal, total *atomic.Int64) error {
	release, gerr := d.pool().acquire(ctx)
	if gerr != nil {
		return gerr
	}
	defer release()

	guard := newIdleGuard(ctx, d.IdleTimeout)
	defer guard.stop()

	req, err := http.NewRequestWithContext(guard.ctx, http.MethodGet, task.Req.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", d.UA)
	for k, v := range task.Req.Header {
		req.Header.Set(k, v)
	}
	if base0 > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", base0))
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if guard.fired.Load() {
			return fmt.Errorf("连接空闲超时（%v 内无数据）", d.IdleTimeout)
		}
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		if retryableStatus(resp.StatusCode) {
			return &retryableErr{status: resp.StatusCode, delay: retryAfter(resp.Header.Get("Retry-After"))}
		}
		return &fatalErr{fmt.Sprintf("HTTP %d：链接无效或被拒绝（多半是直链已过期）。从浏览器扩展重新发送下载（自动刷新 Cookie/Referer），再点「开始」断点续传", resp.StatusCode)}
	}

	base := base0
	if resp.StatusCode == http.StatusOK {
		if base0 > 0 { // resume Range ignored: restart from the beginning
			total.Store(0)
			base = 0
			if err := f.Truncate(0); err != nil { // drop the stale prefix bytes
				return fmt.Errorf("truncate: %w", err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("seek: %w", err)
			}
		}
	} else if st, _, _, ok := parseContentRange(resp.Header.Get("Content-Range")); ok && st != base0 {
		if st == 0 { // 206 but body actually starts at 0: re-map, don't corrupt
			total.Store(0)
			base = 0
			if err := f.Truncate(0); err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("seek: %w", err)
			}
		} else {
			return fmt.Errorf("服务器返回错误的续传偏移 %d（期望 %d），链接内容可能已变化；建议删除任务后重新下载", st, base0)
		}
	}

	var body io.Reader = resp.Body
	if task.Size > 0 && base < task.Size {
		body = io.LimitReader(resp.Body, task.Size-base)
	}
	_, serr := d.stream(guard.ctx, f, base, body, chunkLocal, total, func(n int64) { guard.add(n) })
	if serr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if guard.fired.Load() {
			return fmt.Errorf("连接空闲超时（%v 内无数据）", d.IdleTimeout)
		}
		return serr
	}
	return nil
}

// nextDelay returns the pre-retry sleep: short when the attempt was healthy
// (short body only), exponential backoff otherwise — hammering a throttling
// server with immediate reconnects is itself a disconnect trigger. A ±20%
// jitter is applied so N parallel legs of the same task do not reconnect in
// lockstep (their synchronized reconnect bursts are what anti-leech CDNs
// throttle first, showing up as the endless "下载一直循环").
func (d *Downloader) nextDelay(failStreak int) time.Duration {
	if failStreak == 0 {
		return 100 * time.Millisecond
	}
	mult := time.Duration(1 << min(failStreak-1, 4)) // 1,2,4,8,16x
	delay := d.RetryBase * mult
	if delay > d.RetryMax {
		delay = d.RetryMax
	}
	// jitter: delay * [0.8, 1.2)
	if delay > 0 {
		sp := int64(delay) / 5
		if sp > 0 {
			delay += time.Duration(rand.Int63n(2*sp) - sp)
		}
	}
	return delay
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errText(err error) string {
	if err == nil {
		return "连接提前结束（未传完整）"
	}
	return err.Error()
}

// parseContentRange parses "bytes 100-199/2000" (tolerates "bytes */2000").
// start/end are inclusive byte positions of the served body.
func parseContentRange(v string) (start, end, total int64, ok bool) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "bytes") {
		return 0, 0, 0, false
	}
	spec := strings.TrimSpace(v[len("bytes"):])
	slash := strings.IndexByte(spec, '/')
	if slash < 0 {
		return 0, 0, 0, false
	}
	tail := strings.TrimSpace(spec[slash+1:])
	if tail != "*" {
		n, err := strconv.ParseInt(tail, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, 0, false
		}
		total = n
	}
	ok = true
	rng := strings.TrimSpace(spec[:slash])
	if rng == "*" {
		return 0, 0, total, true
	}
	dash := strings.IndexByte(rng, '-')
	if dash < 0 {
		return 0, 0, 0, false
	}
	if dash > 0 {
		s, err := strconv.ParseInt(strings.TrimSpace(rng[:dash]), 10, 64)
		if err != nil {
			return 0, 0, 0, false
		}
		start = s
	}
	if dash < len(rng)-1 {
		e, err := strconv.ParseInt(strings.TrimSpace(rng[dash+1:]), 10, 64)
		if err != nil {
			return 0, 0, 0, false
		}
		end = e
	}
	return start, end, total, ok
}

// pool returns the shared connection budget. sync.Once keeps hand-built
// Downloaders (tests) working without a constructor guarantee.
func (d *Downloader) pool() *connPool {
	d.slotsOnce.Do(func() {
		n := d.MaxConns
		if n < 1 {
			n = MaxConnsDefault
		}
		d.slots = newConnPool(n)
	})
	return d.slots
}

// SetMaxConns re-sizes the whole-process connection budget (settings page).
func (d *Downloader) SetMaxConns(n int) {
	if n < 1 {
		n = 1
	}
	d.MaxConns = n
	d.pool().setMax(n)
}

// retryAfter parses a Retry-After header: delta-seconds or an HTTP date.
// Junk or a past date means "no instruction" (0), which falls back to the
// normal backoff; a bogus huge value is capped by the caller.
func retryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
