package httpx

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// quiet is the minimal RunOptions used by integrity tests.
func quiet() *RunOptions { return &RunOptions{Emit: func(model.Event) {}} }

func testRangeOf(rngHeader string) (start, end int64, has bool) {
	if !strings.HasPrefix(rngHeader, "bytes=") {
		return 0, -1, false
	}
	spec := strings.TrimPrefix(rngHeader, "bytes=")
	ab := strings.SplitN(spec, "-", 2)
	if len(ab) != 2 {
		return 0, -1, false
	}
	s, err := strconv.ParseInt(strings.TrimSpace(ab[0]), 10, 64)
	if err != nil {
		return 0, -1, false
	}
	if ab[1] == "" {
		return s, -1, true
	}
	e, err := strconv.ParseInt(strings.TrimSpace(ab[1]), 10, 64)
	if err != nil {
		return 0, -1, false
	}
	return s, e, true
}

// dropAtServer mimics throttling anti-leech CDNs (mxcontent-style): it honors
// Range but cuts the body after at most perConn bytes (handler return = drop).
func dropAtServer(data []byte, perConn int64) *httptest.Server {
	size := int64(len(data))
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, e, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Write(data)
			return
		}
		if s >= size {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if e < 0 || e >= size {
			e = size - 1
		}
		if e > s+perConn-1 {
			e = s + perConn - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[s : e+1])
	}))
}

// dripAtServer is dropAtServer plus a sleep between 1 KiB writes: the served
// rate (perConn / delay) is what distinguishes a dead link from a healthy
// download. Bytes-per-attempt alone cannot: a CDN that cuts connections
// instantly after 4 KiB transfers a whole file perfectly well, and aborting
// that (the old behaviour) is exactly the "下几点就失败/反复重试" complaint.
func dripAtServer(data []byte, perConn int64, delay time.Duration) *httptest.Server {
	size := int64(len(data))
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, e, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Write(data)
			return
		}
		if s >= size {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if e < 0 || e >= size {
			e = size - 1
		}
		if e > s+perConn-1 {
			e = s + perConn - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, size))
		w.WriteHeader(http.StatusPartialContent)
		f, _ := w.(http.Flusher)
		for off := s; off <= e; off += 1024 {
			stop := off + 1024
			if stop > e+1 {
				stop = e + 1
			}
			w.Write(data[off:stop])
			if f != nil {
				f.Flush()
			}
			time.Sleep(delay)
		}
	}))
}

// TestChurnSmallDropsComplete: connections cut after 4 KiB, delivered at full
// speed. The old bytes-only rule aborted such a task ("服务器限速"), while the
// whole file is perfectly downloadable — it just needs more connections, which
// is exactly what resume does. Bounded work, no give-up.
func TestChurnSmallDropsComplete(t *testing.T) {
	skipLong(t, "server drops small ranges; the retry budget needs real time")
	const size = 1 << 20 // 1 MiB
	data := bytes.Repeat([]byte("C"), size)
	srv := dropAtServer(data, 4<<10)
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "churn.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	d.RetryBase = time.Millisecond
	d.RetryMax = 4 * time.Millisecond

	task := &model.Task{ID: "ch1", Req: model.Request{URL: srv.URL + "/c.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), task, quiet()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("instant 4 KiB drops must complete, got: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("churn run hung")
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("churn bytes mismatch: %d vs %d", len(got), size)
	}
}

// TestFlakyDropsResumeToCompletion: a server that drops every connection after
// 16 KiB must be survived via bounded resume (byte-exact), NOT loop forever.
func TestFlakyDropsResumeToCompletion(t *testing.T) {
	const size = 100 << 10 // 100 KiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	srv := dropAtServer(data, 16<<10)
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "flaky.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	d.RetryBase = time.Millisecond
	d.RetryMax = 4 * time.Millisecond

	task := &model.Task{ID: "fx1", Req: model.Request{URL: srv.URL + "/f.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), task, quiet()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flaky drops must be survivable, got: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run hung (unbounded loop?)")
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("byte mismatch after flaky resume: %d vs %d", len(got), size)
	}
}

// TestThrottleGivesUpBounded: hard throttling (4 KiB per connection on a 2 MiB
// file) must produce a BOUNDED give-up with an actionable error — never the
// hours-long silent spin the old code did. Partial progress must survive so a
// later resume against a healthy server completes byte-exact.
func TestThrottleGivesUpBounded(t *testing.T) {
	skipLong(t, "bounded give-up walks the whole backoff ladder")
	const size = 1 << 20 // 1 MiB
	data := bytes.Repeat([]byte("T"), size)
	// 4 KiB per connection AND 800 ms per 1 KiB written: ~1.2 KiB/s effective.
	// The give-up rule is bytes-per-attempt AND rate: a CDN that cuts
	// connections instantly (fast small bursts) is churn, not a dead link, and
	// must be completed (see TestChurnSmallDropsComplete); only a genuine crawl
	// is reported.
	srv := dripAtServer(data, 4<<10, 800*time.Millisecond)
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "thr.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	d.RetryBase = time.Millisecond
	d.RetryMax = 2 * time.Millisecond
	d.LowStreakLimit = 3
	d.MinUseful = 64 << 10
	d.MinUsefulRate = 2 << 10

	task := &model.Task{ID: "thr1", Req: model.Request{URL: srv.URL + "/t.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	start := time.Now()
	err := d.Run(context.Background(), task, quiet())
	el := time.Since(start)
	if err == nil {
		t.Fatal("expected bounded give-up error, got nil (would loop forever)")
	}
	if el > 30*time.Second {
		t.Fatalf("give-up took %v, want bounded", el)
	}
	var sum int64
	for _, c := range task.Chunks {
		sum += c.Downloaded
	}
	if sum == 0 {
		t.Fatal("partial chunk progress lost after give-up")
	}
	t.Logf("bounded give-up after %v: %v (kept %d partial bytes)", el.Round(time.Millisecond), err, sum)

	// Resume proof: healthy server, same payload -> completes byte-exact.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, e, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			w.Write(data)
			return
		}
		if e < 0 || e >= size {
			e = size - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[s : e+1])
	}))
	defer srv2.Close()
	task.Req.URL = srv2.URL + "/t.bin"
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("resume after give-up failed: %v", err)
	}
	got, _ := os.ReadFile(save)
	if !bytes.Equal(got, data) {
		t.Fatalf("final bytes mismatch after resume: %d vs %d", len(got), size)
	}
}

// TestChunkedFullBody200Downgrade closes the old detection hole: a chunked 200
// (no Content-Length) for a partial Range used to slip past the ContentLength
// check, making every connection re-download the whole file at wrong offsets.
// Now it must abort fast with zero bytes streamed, for downgrade to sequential.
func TestChunkedFullBody200Downgrade(t *testing.T) {
	const size = 1 << 20 // 1 MiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, has := testRangeOf(r.Header.Get("Range")); has {
			// Lie: 200 with chunked body (no Content-Length -> -1 client-side).
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.Write(data)
	}))
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "chunked200.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	task := &model.Task{ID: "ch1", Req: model.Request{URL: srv.URL + "/c.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}

	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), task, quiet()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrServerNotRange) {
			t.Fatalf("want ErrServerNotRange, got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("chunked 200 lie not detected fast enough")
	}
	var sum int64
	for _, c := range task.Chunks {
		sum += c.Downloaded
	}
	if sum != 0 {
		t.Fatalf("no bytes may be written for the chunked 200 lie, got %d", sum)
	}

	// Downgrade (manager-equivalent) then sequential completion.
	task.SupportRange = false
	task.Conn = 1
	task.Chunks = nil
	task.Downloaded = 0
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("downgrade sequential run failed: %v", err)
	}
	got, _ := os.ReadFile(save)
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes mismatch after downgrade: %d vs %d", len(got), size)
	}
}

// TestIdleStallWatchdog: "206 headers then silence" must NOT hang forever —
// the idle watchdog aborts each attempt and the run settles with an error.
func TestIdleStallWatchdog(t *testing.T) {
	skipLong(t, "idle guard + run watchdog are wall-clock timeouts")
	const size = 1 << 20
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, e, has := testRangeOf(r.Header.Get("Range"))
		if !has {
			w.WriteHeader(http.StatusOK)
			w.Write(make([]byte, 10))
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("0123456789"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(15 * time.Second) // silence: the watchdog must cut this
	}))
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "stall.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	d.IdleTimeout = 800 * time.Millisecond
	d.LowStreakLimit = 2
	d.MinUseful = 1 << 20
	d.RetryBase = time.Millisecond

	task := &model.Task{ID: "st1", Req: model.Request{URL: srv.URL + "/s.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), task, quiet()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled server must end in error, got nil")
		}
		if el := time.Since(start); el > 10*time.Second {
			t.Fatalf("watchdog too slow: %v", el)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("idle watchdog never fired (run hangs at 0 B/s forever)")
	}
}

// TestPersistDuringRun proves mid-run snapshots: a crash/kill now loses at
// most one PersistEvery interval, instead of all in-flight progress.
func TestPersistDuringRun(t *testing.T) {
	const size = 1 << 20 // 1 MiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	srv := slowRangeServer(data, 16<<10, 80*time.Millisecond) // ~1.3s wall for 4 conns
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "persist.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	d.PersistEvery = 500 * time.Millisecond

	var mu sync.Mutex
	var totals []int64
	var chunkLens []int
	opts := &RunOptions{
		Emit: func(model.Event) {},
		Persist: func(total int64, chunks []model.ChunkState) {
			mu.Lock()
			defer mu.Unlock()
			totals = append(totals, total)
			chunkLens = append(chunkLens, len(chunks))
		},
	}
	task := &model.Task{ID: "p1", Req: model.Request{URL: srv.URL + "/p.bin"}, Size: size,
		SupportRange: true, Conn: 4, SavePath: save}
	if err := d.Run(context.Background(), task, opts); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(totals) < 2 {
		t.Fatalf("expected mid-run persist snapshots, got %d", len(totals))
	}
	sawPartial := false
	for i, tot := range totals {
		if chunkLens[i] != 4 {
			t.Fatalf("snapshot %d carries %d chunk counters, want 4", i, chunkLens[i])
		}
		if tot > 0 && tot < size {
			sawPartial = true
		}
	}
	if !sawPartial {
		t.Fatalf("no snapshot captured partial in-flight progress: %v", totals)
	}
	t.Logf("mid-run snapshots: %v", totals)
}
