package download

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/store"
)

// TestStoreSnapshotDuringRun: while a download is RUNNING the bbolt store must
// already carry growing per-chunk progress, so a hard kill (taskkill / crash /
// power loss — no graceful stop) loses at most one snapshot interval.
// Regression guard for "断点续传无效：进程被杀后从 0 重来".
func TestStoreSnapshotDuringRun(t *testing.T) {
	payload := bytes.Repeat([]byte("P"), 256<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if strings.HasPrefix(rng, "bytes=") {
			s, e := parseRangeTest(rng[6:])
			if s >= int64(len(payload)) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if e < 0 || e >= int64(len(payload)) {
				e = int64(len(payload) - 1)
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			body := payload[s : e+1]
			fl, _ := w.(http.Flusher)
			for i := 0; i < len(body); i += 8 << 10 {
				j := i + (8 << 10)
				if j > len(body) {
					j = len(body)
				}
				if _, err := w.Write(body[i:j]); err != nil {
					return
				}
				if fl != nil {
					fl.Flush()
				}
				time.Sleep(150 * time.Millisecond) // slow enough to poll mid-run
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m := NewManager(st, event.NewBus(), model.Config{DownloadDir: dir, DefaultConn: 4})
	m.dl.PersistEvery = 300 * time.Millisecond

	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/snap.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}

	sawLive := false
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := m.Store.GetTask(task.ID)
		if err == nil {
			if cur.Status == model.StatusRunning && cur.Downloaded > 0 {
				sawLive = true
			}
			if cur.Status == model.StatusComplete || cur.Status == model.StatusError {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !sawLive {
		t.Fatal("store never showed in-flight progress while Running (a hard kill would lose everything)")
	}
	for time.Now().Before(deadline) {
		cur, err := m.Store.GetTask(task.ID)
		if err == nil && cur.Status == model.StatusComplete {
			return
		}
		if err == nil && cur.Status == model.StatusError {
			t.Fatalf("task errored: %s", cur.Err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("task did not complete in time")
}
