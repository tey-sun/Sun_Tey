package download

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/store"
)

// TestAutoDowngradeEndToEnd: probe says Range (206 on 1B), production GET
// returns 200 full body. Expect: multi-conn run aborts, manager auto-relaunches
// once as sequential, task completes byte-exact without duplication.
func TestAutoDowngradeEndToEnd(t *testing.T) {
	payload := bytes.Repeat([]byte("L"), 300<<10)
	var relaunched atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng == "bytes=0-0" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload[:1])
			return
		}
		if rng == "" {
			// first lying full GET aborts multi-conn; a later no-Range GET is
			// the sequential relaunch. Ranges straggling from the first
			// attempt's 4-conn fan-out are legitimate until that point.
			relaunched.Store(true)
		} else if relaunched.Load() {
			t.Errorf("sequential relaunch must not send Range (task no longer claims Range)")
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer ts.Close()

	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := model.Config{DownloadDir: dir, DefaultConn: 4}
	m := NewManager(st, event.NewBus(), cfg)
	// M1 only splits files >= 2x min-split-size (4 MiB by default, aria2 rule).
	// Shrink it so this 300 KiB fixture really is a multi-segment plan and the
	// lying server is really caught.
	m.dl.MinSplitSize, m.dl.SegmentFloor = 64<<10, 64<<10

	task, err := m.Create(t.Context(), model.Request{URL: ts.URL + "/f.bin"})
	if err != nil {
		t.Fatal(err)
	}
	ch, cancel := m.Bus.Subscribe(512)
	defer cancel()

	done := make(chan model.Task, 1)
	go func() {
		for e := range ch {
			if e.Type == "done" || e.Type == "error" {
				if tt, err := m.Store.GetTask(e.TaskID); err == nil {
					done <- *tt
				}
				return
			}
		}
	}()

	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}

	select {
	case ft := <-done:
		if ft.Status != model.StatusComplete {
			t.Fatalf("status = %v (err=%q), want Complete", ft.Status, ft.Err)
		}
		if ft.Conn != 1 || ft.SupportRange {
			t.Fatalf("expected downgrade flags: Conn=1 SupportRange=false, got Conn=%d SR=%v", ft.Conn, ft.SupportRange)
		}
		data, err := os.ReadFile(ft.SavePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("file mismatch: got %d bytes, want %d", len(data), len(payload))
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for completion (auto-relaunch loop?)")
	}
}

// TestUAPropagationThroughUpdateConfig: the UI setting "UA" must reach every
// outbound request of a download started after the change — probe, full GET,
// and per-chunk Range GETs — and both downloader copies (httpx + hlsx).
func TestUAPropagationThroughUpdateConfig(t *testing.T) {
	payload := bytes.Repeat([]byte("U"), 300<<10)
	var mu sync.Mutex
	var seen []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("User-Agent"))
		mu.Unlock()
		rng := r.Header.Get("Range")
		if rng == "bytes=0-0" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload[:1])
			return
		}
		if lo, ok := parseRangeStart(rng); ok && lo > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, len(payload)-1, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload[lo:])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.WriteHeader(http.StatusOK)
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
	if _, err := m.UpdateConfig(model.Config{UserAgent: "SunTey-UA-Test/9.9"}); err != nil {
		t.Fatal(err)
	}
	task, err := m.Create(t.Context(), model.Request{URL: ts.URL + "/ua.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := m.Store.GetTask(task.ID); got != nil && got.Status == model.StatusComplete {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("server saw no requests")
	}
	for _, ua := range seen {
		if ua != "SunTey-UA-Test/9.9" {
			t.Fatalf("a request carried UA %q, want the configured UA (all %d requests must match)", ua, len(seen))
		}
	}
	if m.dl.UA != "SunTey-UA-Test/9.9" || m.hls.UA != "SunTey-UA-Test/9.9" {
		t.Fatalf("downloader copies not updated: dl=%q hls=%q", m.dl.UA, m.hls.UA)
	}
}

func parseRangeStart(rng string) (int, bool) {
	// "bytes=12-..." → 12
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, false
	}
	rest := strings.TrimPrefix(rng, "bytes=")
	i := strings.IndexByte(rest, '-')
	if i <= 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:i])
	return n, err == nil
}
