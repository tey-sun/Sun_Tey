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
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// healthyRangeServer serves Range requests honestly and completely.
func healthyRangeServer(data []byte) *httptest.Server {
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
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[s : e+1])
	}))
}

// TestEnsureChunksPreservesProgress is the resume-integrity rule for the
// "改了线程数就得从 0 重下" bug: re-splitting must carry the verified bytes over,
// and may only claim the CONTIGUOUS prefix of each new range.
func TestEnsureChunksPreservesProgress(t *testing.T) {
	d := NewDownloader(nil, "ua")
	smallSplits(d)
	task := &model.Task{ID: "rp", Size: 1000 << 10, Conn: 4, SavePath: filepath.Join(t.TempDir(), "rp.bin")}
	d.planSegments(task)
	if len(task.Chunks) != 4 {
		t.Fatalf("want 4 chunks, got %d", len(task.Chunks))
	}
	// chunk0 fully done, chunk1 half, chunk2 untouched, chunk3 a quarter done.
	task.Chunks[0].Downloaded = task.Chunks[0].End - task.Chunks[0].Start + 1
	task.Chunks[1].Downloaded = 100 << 10
	task.Chunks[3].Downloaded = 50 << 10

	task.Conn = 2 // user changed 线程数 in Settings, then pressed 开始
	d.planSegments(task)
	if len(task.Chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(task.Chunks))
	}
	// New chunk 0 = [0, 500K): done prefix = 0..350K (250K + 100K), then a hole.
	if got := task.Chunks[0].Downloaded; got != 350<<10 {
		t.Fatalf("chunk0 prefix = %d, want %d", got, 350<<10)
	}
	// New chunk 1 = [500K, 1000K): starts on a hole -> must be re-fetched.
	if got := task.Chunks[1].Downloaded; got != 0 {
		t.Fatalf("chunk1 prefix = %d, want 0 (hole at range start)", got)
	}
	if task.Downloaded != 350<<10 {
		t.Fatalf("task.Downloaded = %d, want %d", task.Downloaded, 350<<10)
	}
}

// TestResumeAfterConnChange is the end-to-end proof: pause mid-download,
// change the connection count, resume, and the finished file must be byte-exact.
// Before the fix this re-downloaded from 0 (progress lost) and, worse, the
// re-split ranges silently trusted stale chunk counters.
func TestResumeAfterConnChange(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	// Slow enough that a pause at ~1/4 progress is observable (legs: 16 KiB/60ms).
	srv := slowRangeServer(data, 16<<10, 60*time.Millisecond)
	defer srv.Close()

	save := filepath.Join(t.TempDir(), "cc.bin")
	d := NewDownloader(srv.Client(), "suntey-test")
	smallSplits(d)

	task := &model.Task{ID: "cc1", Req: model.Request{URL: srv.URL + "/cc.bin"}, Size: size,
		SupportRange: true, Conn: 8, SavePath: save}

	// Pause on purpose partway through.
	ctx, cancel := context.WithCancel(context.Background())
	var fired atomic.Bool
	prog := &RunOptions{Emit: func(e model.Event) {
		if e.Downloaded > size/4 && fired.CompareAndSwap(false, true) {
			cancel()
		}
	}}
	if err := d.Run(ctx, task, prog); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	partial := task.Downloaded
	if partial <= 0 || partial >= size {
		t.Fatalf("expected a partial pause, got %d/%d", partial, size)
	}

	// Change connections (Settings -> 线程数) and resume against a healthy server.
	task.Conn = 2
	d.planSegments(task)
	if task.Downloaded <= 0 {
		t.Fatalf("resume progress lost after conn change: %d", task.Downloaded)
	}
	srv2 := healthyRangeServer(data)
	defer srv2.Close()
	task.Req.URL = srv2.URL + "/cc.bin"
	if err := d.Run(context.Background(), task, quiet()); err != nil {
		t.Fatalf("resume after conn change failed: %v", err)
	}
	got, err := os.ReadFile(save)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > size {
		got = got[:size]
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes mismatch after conn-change resume (%d vs %d)", len(got), size)
	}
	t.Logf("conn-change resume: paused at %d bytes, completed byte-exact", partial)
}
