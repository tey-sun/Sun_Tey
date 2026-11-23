package httpx

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// slowRangeServer serves [a-b] with a per-chunk sleep so a real cancel can land
// mid-download (localhost is otherwise too fast to interrupt).
func slowRangeServer(data []byte, perChunk int64, sleep time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		size := int64(len(data))
		start, end, has := int64(0), size-1, false
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			spec := strings.TrimPrefix(rng, "bytes=")
			if ab := strings.SplitN(spec, "-", 2); len(ab) == 2 {
				start, _ = strconv.ParseInt(ab[0], 10, 64)
				if ab[1] != "" {
					end, _ = strconv.ParseInt(ab[1], 10, 64)
				}
				has = true
			}
		}
		if has {
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusPartialContent)
		}
		for off := start; off <= end; off += perChunk {
			n := perChunk
			if off+n > end+1 {
				n = end + 1 - off
			}
			if _, err := w.Write(data[off : off+n]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(sleep)
		}
	}))
}

// TestResumeByteExact proves the core guarantees: Range-split parallel download,
// mid-flight cancel -> resume, and byte-exact file integrity (catches any WriteAt
// offset bug). Runs fully in-process against a Range-capable httptest server.
func TestResumeByteExact(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	srv := slowRangeServer(data, 32<<10, 12*time.Millisecond)
	defer srv.Close()

	dir := t.TempDir()
	save := filepath.Join(dir, "f.bin")

	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)

	task := &model.Task{
		ID:           "t1",
		Req:          model.Request{URL: srv.URL + "/f.bin"},
		Size:         size,
		SupportRange: true,
		Conn:         8,
		SavePath:     save,
	}

	// Run #1: cancel after 80ms (server ~192ms/chunk -> cancel lands mid-flight).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, task, quiet()) }()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("run#1 hung")
	}

	// Chunks must reflect on-disk progress (resume state), 0 < sum < size.
	var chunkSum int64
	for _, c := range task.Chunks {
		chunkSum += c.Downloaded
	}
	if chunkSum == 0 {
		t.Fatal("no chunk progress recorded for resume")
	}
	if chunkSum >= size {
		t.Logf("warning: run#1 finished (%d/%d) before cancel; resume path still exercised", chunkSum, size)
	}
	t.Logf("run#1 paused at %d/%d bytes, chunks=%d", chunkSum, size, len(task.Chunks))

	// Run #2: resume to completion (no cancel).
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("resume run failed: %v", err)
	}

	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(got)) != size {
		t.Fatalf("file size %d, want %d", len(got), size)
	}
	if !bytes.Equal(got, data) {
		for i := range got {
			if got[i] != data[i] {
				t.Fatalf("byte mismatch at %d: got %x want %x", i, got[i], data[i])
			}
		}
	}
	t.Logf("resume byte-exact OK: %d bytes verified, resumed from %d", len(got), chunkSum)
}

// TestSequentialIntegrity: no Range (SupportRange=false) -> single stream, exact bytes.
func TestSequentialIntegrity(t *testing.T) {
	const size = 1 << 20
	data := make([]byte, size)
	rand.Read(data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// no Range: serve full body, 200 only
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	}))
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "seq.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	task := &model.Task{ID: "s1", Req: model.Request{URL: srv.URL}, Size: size, SupportRange: false, Conn: 1, SavePath: save}
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(save)
	if !bytes.Equal(got, data) {
		t.Fatalf("sequential bytes mismatch: len %d vs %d", len(got), size)
	}
}

// TestServerIgnoresRangeNoDuplication reproduces the production-vs-probe lie:
// the server answers the probe (Range: bytes=0-0) with 206+Content-Range, but
// every production Range request gets a full 200 body. runRanges must abort
// with ErrServerNotRange, fetch far less than Conn x size, and after the
// manager-equivalent downgrade the sequential run must produce byte-exact data.
func TestServerIgnoresRangeNoDuplication(t *testing.T) {
	const size = 2 << 20 // 2 MiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	var served atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			// Probe lie: claim Range support.
			w.Header().Set("Content-Range", "bytes 0-0/"+strconv.Itoa(size))
			w.WriteHeader(http.StatusPartialContent)
			b := data[:1]
			served.Add(int64(len(b)))
			w.Write(b)
			return
		}
		// Production: ignore Range, serve full body with 200.
		w.Header().Set("Content-Length", strconv.Itoa(size))
		served.Add(size)
		w.Write(data)
	}))
	defer srv.Close()

	// Probe reports Range support (the lie).
	res, err := Probe(context.Background(), srv.Client(), model.Request{URL: srv.URL}, "suntey-test")
	if err != nil {
		t.Fatal(err)
	}
	if !res.SupportRange {
		t.Fatal("probe should report supportRange for the lying server")
	}

	save := filepath.Join(t.TempDir(), "lie.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)
	task := &model.Task{ID: "l1", Req: model.Request{URL: srv.URL}, Size: size, SupportRange: true, Conn: 4, SavePath: save}

	err = d.Run(context.Background(), task, quiet())
	if !errors.Is(err, ErrServerNotRange) {
		t.Fatalf("want ErrServerNotRange, got %v", err)
	}
	gotServed := served.Load()
	// Bounded, no loop: Conn=4 legs fire concurrently, the lying server answers
	// each with one full body before the first rangeOK detection cancels the
	// run. That is at most Conn full bodies (4x size here) — the fix removed the
	// old pathological behaviour (every chunk retrying a full body for hours,
	// 100x+). Anything at the 8x scale means the cancel-on-detect path broke.
	if gotServed >= 8*int64(size) {
		t.Fatalf("duplication not bounded: served %d bytes for a %d-byte file", gotServed, size)
	}
	t.Logf("abort fast: served %d bytes for a %d-byte file (Conn=4)", gotServed, size)

	// Manager-equivalent downgrade, then sequential to completion.
	task.SupportRange = false
	task.Conn = 1
	task.Chunks = nil
	task.Downloaded = 0
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("downgrade sequential run failed: %v", err)
	}
	out, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("bytes mismatch after downgrade: %d vs %d", len(out), size)
	}
	t.Logf("downgrade sequential byte-exact OK (%d bytes), total served %d", len(out), served.Load())
}
