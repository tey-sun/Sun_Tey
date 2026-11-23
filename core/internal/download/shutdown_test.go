package download

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/store"
)

// TestGracefulShutdownPersistsProgress: chunked download of a slow Range
// server, GracefulShutdown mid-flight. Expect a paused task whose progress was
// persisted, and a resume that never re-fetches finished bytes (resumed served
// <= remaining + slack) and completes byte-exact. Regression guard for the
// sum(nil)==0 overwrite that zeroed progress after graceful stop.
func TestGracefulShutdownPersistsProgress(t *testing.T) {
	_ = atomic.Int64{}
	payload := bytes.Repeat([]byte("G"), 800<<10)
	sum := sha256.Sum256(payload)
	want := hex.EncodeToString(sum[:])

	var served atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		w.Header().Set("Accept-Ranges", "bytes")
		if strings.HasPrefix(rng, "bytes=") {
			s, e := parseRangeTest(rng[6:])
			if s >= int64(len(payload)) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if e < 0 || e >= int64(len(payload)) {
				e = int64(len(payload) - 1)
			}
			body := payload[s : e+1]
			served.Add(int64(len(body)))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			fl, _ := w.(http.Flusher)
			for i := 0; i < len(body); i += 4 << 10 {
				j := i + (4 << 10)
				if j > len(body) {
					j = len(body)
				}
				if _, err := w.Write(body[i:j]); err != nil {
					return
				}
				if fl != nil {
					fl.Flush()
				}
				time.Sleep(40 * time.Millisecond) // ~100KB/s per conn: shutdown lands mid-flight
			}
			return
		}
		served.Add(int64(len(payload)))
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Write(payload)
	}))
	defer ts.Close()

	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m := NewManager(st, event.NewBus(), model.Config{DownloadDir: dir, DefaultConn: 4})
	ch, cancel := m.Bus.Subscribe(4096)
	defer cancel()
	prog := make(chan int64, 256)
	go func() {
		for e := range ch {
			if e.Type == "progress" {
				select {
				case prog <- e.Downloaded:
				default:
				}
			}
		}
	}()

	task, err := m.Create(t.Context(), model.Request{URL: ts.URL + "/g.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}

	// store snapshots stay stale while Running (progress flows over the bus),
	// so watch SSE progress events for the shutdown window
	deadline := time.Now().Add(8 * time.Second)
	sawTarget := false
	for !sawTarget && time.Now().Before(deadline) {
		select {
		case dn := <-prog:
			if dn >= 60<<10 {
				sawTarget = true
			}
		case <-time.After(2 * time.Second):
			cur, err := m.Store.GetTask(task.ID)
			if err == nil && (cur.Status == model.StatusComplete || cur.Status == model.StatusError) {
				t.Fatalf("task ended %s before shutdown window (%s)", cur.Status, cur.Err)
			}
		}
	}
	if !sawTarget {
		t.Fatal("never observed >=60KB progress")
	}

	if n := m.GracefulShutdown(); n != 1 {
		t.Fatalf("GracefulShutdown stopped %d, want 1", n)
	}
	var paused int64 = -1
	for i := 0; i < 240; i++ { // up to 12s: cancelled streams must settle + snapshot
		cur, err := m.Store.GetTask(task.ID)
		if err == nil && cur.Status == model.StatusPaused {
			paused = cur.Downloaded
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if paused <= 0 {
		t.Fatalf("expected paused with persisted progress > 0, got %d", paused)
	}

	servedBefore := served.Load()
	if err := m.Start(task.ID); err != nil {
		t.Fatalf("resume start: %v", err)
	}
	deadline = time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := m.Store.GetTask(task.ID)
		if err == nil && (cur.Status == model.StatusComplete || cur.Status == model.StatusError) {
			if cur.Status != model.StatusComplete {
				t.Fatalf("resume ended %s: %s", cur.Status, cur.Err)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	resumed := served.Load() - servedBefore
	if limit := int64(len(payload)) - paused + 4096; resumed > limit {
		t.Fatalf("resume served %d bytes, limit %d (re-downloaded finished data)", resumed, limit)
	}
	got, err := os.ReadFile(filepath.Join(dir, "g.bin"))
	if err != nil {
		t.Fatal(err)
	}
	gs := sha256.Sum256(got)
	if hex.EncodeToString(gs[:]) != want {
		t.Fatalf("final sha mismatch: size=%d", len(got))
	}
}

func parseRangeTest(v string) (start, end int64) {
	start, end = 0, -1
	dash := strings.IndexByte(v, '-')
	if dash < 0 {
		return 0, -1
	}
	if dash > 0 {
		start, _ = strconv.ParseInt(v[:dash], 10, 64)
	}
	if dash < len(v)-1 {
		end, _ = strconv.ParseInt(v[dash+1:], 10, 64)
	}
	return start, end
}
