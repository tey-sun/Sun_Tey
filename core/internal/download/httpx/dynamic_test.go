package httpx

// M1 / R1 acceptance tests: the connection budget is governed by three
// INDEPENDENT knobs (min-split-size / per-server cap / total cap), the ramp-up
// is geometric, a slow connection hands its tail to an idle one, and a
// throttling server (429 + Retry-After) is obeyed instead of hammered.

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// ---------------------------------------------------------------------------
// planning (pure)
// ---------------------------------------------------------------------------

func TestNextBatchRamp(t *testing.T) {
	want := []int{2, 4, 16, 256, 512}
	b := 1
	for i, w := range want {
		if b = nextBatch(b, 512); b != w {
			t.Fatalf("step %d: batch = %d, want %d", i, b, w)
		}
	}
	if got := nextBatch(1<<40, 16); got != 16 {
		t.Fatalf("overflow guard: got %d, want 16", got)
	}
	if got := nextBatch(1, 1); got != 1 {
		t.Fatalf("max=1 must stay 1, got %d", got)
	}
}

func TestPlanLegsThreeKnobs(t *testing.T) {
	d := NewDownloader(nil, "ua")
	cases := []struct {
		size       int64
		conn, want int
		why        string
	}{
		{1 << 20, 16, 1, "1 MiB < 2x min-split: never split"},
		{7<<20 - 1, 16, 1, "still under 2x min-split"},
		{9 << 20, 16, 2, "size/min-split caps the legs at 2"},
		{1 << 30, 16, 8, "per-server cap (MaxConnPerHost) wins"},
		{1 << 30, 3, 3, "user setting below both caps wins"},
	}
	for _, c := range cases {
		if got := d.planLegs(c.size, c.conn); got != c.want {
			t.Errorf("planLegs(%d MiB, conn=%d) = %d, want %d (%s)",
				c.size>>20, c.conn, got, c.want, c.why)
		}
	}
	d.MaxConnPerHost = 4
	if got := d.planLegs(1<<30, 16); got != 4 {
		t.Errorf("MaxConnPerHost not honoured: %d, want 4", got)
	}
}

func TestPlanSegmentsBoundedTilesAndPreserves(t *testing.T) {
	d := NewDownloader(nil, "ua")
	dir := t.TempDir()
	task := &model.Task{ID: "plan", Size: 200 << 30, Conn: 16,
		SavePath: filepath.Join(dir, "big.bin")}
	legs := d.planSegments(task)
	if legs != d.maxPerHost() {
		t.Fatalf("legs = %d, want the per-server cap %d", legs, d.maxPerHost())
	}
	if n := len(task.Chunks); n > d.maxSegments() {
		t.Fatalf("%d segments exceeds the snapshot bound %d", n, d.maxSegments())
	}
	if n := len(task.Chunks); n < legs {
		t.Fatalf("%d segments cannot feed %d legs: fan-out lost", n, legs)
	}
	// the layout must tile [0,size-1] with no gaps or overlaps
	if !layoutValid(task.Chunks, task.Size) {
		t.Fatalf("layout does not tile the file: %d chunks", len(task.Chunks))
	}
	// every segment must be big enough to be worth a request
	minSeg := d.segFloor()
	for _, c := range task.Chunks[:len(task.Chunks)-1] {
		if n := c.End - c.Start + 1; n < minSeg {
			t.Fatalf("segment %d is %d bytes, below the %d byte floor", c.Index, n, minSeg)
		}
	}

	// Finish the head of the file, then change the connection count: the
	// verified bytes must survive the re-split (R11 / "改了线程数就从头下").
	for i := range task.Chunks {
		task.Chunks[i].Downloaded = task.Chunks[i].End - task.Chunks[i].Start + 1
		if i == len(task.Chunks)/2 {
			break // half the file done, the rest is a hole
		}
	}
	var half int64
	for _, c := range task.Chunks {
		half += c.Downloaded
	}
	task.Conn = 2
	d.planSegments(task)
	if task.Downloaded < half {
		t.Fatalf("re-split lost progress: %d < %d", task.Downloaded, half)
	}
	if !layoutValid(task.Chunks, task.Size) {
		t.Fatal("re-split layout invalid")
	}
}

// ---------------------------------------------------------------------------
// ramp-up + budget primitives
// ---------------------------------------------------------------------------

func TestSlowStartRampThrottleAndFailure(t *testing.T) {
	ctx := context.Background()
	s := newSlowStart(8)
	if err := s.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	a, active, _, _ := s.snapshot()
	if a != 1 || active != 1 {
		t.Fatalf("cold start must allow exactly one connection, got allowed=%d active=%d", a, active)
	}
	// The second leg may only pass once a whole wave resolved.
	granted := make(chan struct{})
	go func() {
		if err := s.acquire(ctx); err == nil {
			close(granted)
		}
	}()
	s.opened() // the first connection is in flight
	select {
	case <-granted:
		t.Fatal("second connection opened before the first wave resolved")
	case <-time.After(80 * time.Millisecond):
	}
	s.confirmed() // response seen -> grow by the next batch
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("ramp-up never grew after a confirmed connection")
	}
	// 429 freezes growth: allowed drops to what is running, no new grants.
	s.throttled(time.Minute)
	al, act, _, paused := s.snapshot()
	if !paused || al != act {
		t.Fatalf("throttled: allowed=%d active=%d paused=%v", al, act, paused)
	}
	// A dying connection must not freeze the ramp for everybody else.
	s.opened()
	s.failed()
	if _, _, pending, _ := s.snapshot(); pending != 0 {
		t.Fatalf("failed() left %d pending: the ramp would deadlock", pending)
	}
	// wakeAll releases parked legs when the run ends.
	parked := make(chan struct{})
	go func() {
		if err := s.acquire(ctx); err == nil {
			close(parked)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	s.wakeAll()
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("wakeAll left a leg parked: runRanges would hang in WaitGroup")
	}
}

func TestConnPoolCapsConcurrency(t *testing.T) {
	p := newConnPool(3)
	var mu sync.Mutex
	cur, peak := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := p.acquire(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			cur++
			if cur > peak {
				peak = cur
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			cur--
			mu.Unlock()
			release()
		}()
	}
	wg.Wait()
	if peak > 3 {
		t.Fatalf("peak concurrency %d exceeded the MaxConns budget 3", peak)
	}
	if peak < 2 {
		t.Fatalf("peak concurrency %d: the pool never let two attempts run", peak)
	}
	// shrinking the budget must be reflected in the limit (settings page)
	p.setMax(1)
	if got := p.limit(); got != 1 {
		t.Fatalf("limit = %d after setMax(1)", got)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	if got := retryAfter("7"); got != 7*time.Second {
		t.Errorf("delta-seconds: got %v", got)
	}
	if got := retryAfter(""); got != 0 {
		t.Errorf("empty header must mean no instruction, got %v", got)
	}
	if got := retryAfter("-5"); got != 0 {
		t.Errorf("negative seconds must be ignored, got %v", got)
	}
	if got := retryAfter("junk"); got != 0 {
		t.Errorf("junk must be ignored, got %v", got)
	}
	future := time.Now().UTC().Add(30 * time.Second).Format(http.TimeFormat)
	if got := retryAfter(future); got < 25*time.Second || got > 31*time.Second {
		t.Errorf("HTTP date: got %v", got)
	}
	past := time.Now().UTC().Add(-time.Hour).Format(http.TimeFormat)
	if got := retryAfter(past); got != 0 {
		t.Errorf("past HTTP date must be 0, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// end-to-end: stealing + throttle handling + "no split for small files"
// ---------------------------------------------------------------------------

// TestStealHandoverSlowLeg reproduces the IDM behaviour the static splitter
// could not do: one connection stops moving while others are done. The slow
// connection is cut and an idle leg takes its remainder, so the task finishes
// instead of idling until the watchdog kills it.
func TestStealHandoverSlowLeg(t *testing.T) {
	const size = 1 << 20 // 4 x 256 KiB after smallSplits
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const victim = 3 * (size / 4) // start of the last segment
	var mu sync.Mutex
	hits := map[int64]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			start, end = 0, size-1
		}
		if end < 0 || end >= size {
			end = size - 1
		}
		mu.Lock()
		hits[start]++
		first := hits[start] == 1
		mu.Unlock()
		if start == victim && first {
			time.Sleep(1500 * time.Millisecond) // the "CDN throttled this leg" moment
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer srv.Close()

	d := smallSplits(NewDownloader(srv.Client(), "suntey-test"))
	d.StealThreshold = 300 * time.Millisecond
	d.StealMinChunkSize = 16 << 10
	d.StealMaxPerChunk = 2

	var steals int32
	opts := &RunOptions{Emit: func(e model.Event) {
		if e.Type == "steal" {
			atomic.AddInt32(&steals, 1)
		}
	}}
	save := filepath.Join(t.TempDir(), "steal.bin")
	task := &model.Task{ID: "st1", Req: model.Request{URL: srv.URL + "/s.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	start := time.Now()
	if err := d.Run(context.Background(), task, opts); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	elapsed := time.Since(start)
	if n := atomic.LoadInt32(&steals); n == 0 {
		t.Fatal("no steal event: the slow connection was never handed over")
	}
	mu.Lock()
	secondTry := hits[victim]
	mu.Unlock()
	if secondTry < 2 {
		t.Fatalf("victim segment was fetched %d times: nobody re-fetched the handed-over tail", secondTry)
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("file mismatch after steal (%d vs %d bytes)", len(got), size)
	}
	// The hand-over has to BEAT the slow connection: if the stolen leg had not
	// been cut at the first reporter tick (~500ms), the run could not end
	// before that request finished its 1500ms sleep on its own.
	if elapsed > 1200*time.Millisecond {
		t.Fatalf("steal did not speed anything up: finished in %v, the slow leg alone needs 1500ms", elapsed)
	}
	t.Logf("steal OK: %d handovers, victim fetched %dx, %d bytes byte-exact in %v",
		atomic.LoadInt32(&steals), secondTry, len(got), elapsed.Round(time.Millisecond))
}

// TestThrottle429HonoursRetryAfter: a server that answers 429 + Retry-After must
// be waited out (not hammered), the ramp-up must freeze, and the download must
// still finish.
func TestThrottle429HonoursRetryAfter(t *testing.T) {
	const size = 1 << 20
	data := bytes.Repeat([]byte("T"), size)
	var n429 int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n429, 1) <= 2 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		start, end, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			start, end = 0, size-1
		}
		if end < 0 || end >= size {
			end = size - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer srv.Close()

	d := smallSplits(NewDownloader(srv.Client(), "suntey-test"))
	d.MaxConnPerHost = 1 // single leg: the retry wait is measurable, not parallel
	d.SegmentsPerLeg = 1
	save := filepath.Join(t.TempDir(), "429.bin")
	task := &model.Task{ID: "429", Req: model.Request{URL: srv.URL + "/t.bin"}, Size: size,
		SupportRange: true, Conn: 1, SavePath: save}
	start := time.Now()
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("429 handling failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < time.Second {
		t.Fatalf("Retry-After: 1 ignored - the run took only %v", elapsed)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("waited %v for a 1s Retry-After, giving up somewhere?", elapsed)
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file mismatch after 429 retries")
	}
}

// TestSmallFileNeverSplits is the other half of the min-split rule: on a 1 MiB
// file the client must open ONE connection, because 16 handshakes for 64 KiB
// fragments is slower than one stream (and looks like an attack to the CDN).
func TestSmallFileNeverSplits(t *testing.T) {
	const size = 1 << 20
	data := bytes.Repeat([]byte("S"), size)
	var rangeReqs, fullReqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			atomic.AddInt32(&rangeReqs, 1)
		} else {
			atomic.AddInt32(&fullReqs, 1)
		}
		start, end, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			w.Header().Set("Content-Length", strconv.Itoa(size))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		if end < 0 || end >= size {
			end = size - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer srv.Close()

	d := NewDownloader(srv.Client(), "suntey-test") // production defaults
	save := filepath.Join(t.TempDir(), "small.bin")
	task := &model.Task{ID: "sm", Req: model.Request{URL: srv.URL + "/s.bin"}, Size: size,
		SupportRange: true, Conn: 16, SavePath: save}
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatal(err)
	}
	if n := len(task.Chunks); n != 1 {
		t.Fatalf("1 MiB file was split into %d segments; min-split-size must prevent that", n)
	}
	if n := atomic.LoadInt32(&rangeReqs); n != 1 {
		t.Fatalf("%d range requests for a file that must not be split", n)
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("single-connection download mismatch")
	}
}

func TestNoRangeServerFallsBackToSingleStream(t *testing.T) {
	const size = 6 << 20
	data := bytes.Repeat([]byte("N"), size)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// lies: always answers the FULL body, ignoring Range
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer srv.Close()

	d := smallSplits(NewDownloader(srv.Client(), "suntey-test"))
	save := filepath.Join(t.TempDir(), "norrange.bin")
	task := &model.Task{ID: "nr", Req: model.Request{URL: srv.URL + "/n.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	err := d.Run(context.Background(), task, quiet())
	if !errors.Is(err, ErrServerNotRange) {
		t.Fatalf("want ErrServerNotRange so the manager downgrades to one stream, got %v", err)
	}
}
