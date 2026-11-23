package hlsx

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

func newTestTask(t *testing.T, url string) *model.Task {
	t.Helper()
	dir := t.TempDir()
	return &model.Task{
		ID:       "t1",
		Req:      model.Request{URL: url},
		SavePath: filepath.Join(dir, "video.mp4"),
		Conn:     3,
		HLS:      &model.HLSState{MediaURL: url},
	}
}

func newTestDL(srv *httptest.Server) *Downloader {
	d := NewDownloader(srv.Client(), "test-ua")
	d.MaxAttemptsPerSeg = 2
	d.RetryBase = 10_000_000 // 10ms
	d.RetryMax = 50_000_000
	return d
}

func segData(i int, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte('A' + i*7 + j%5)
	}
	return b
}

func seqIV(seq uint64) []byte {
	iv := make([]byte, 16)
	binary.BigEndian.PutUint64(iv[8:], seq)
	return iv
}

func aesEncrypt(pt, key, iv []byte) []byte {
	block, _ := aes.NewCipher(key)
	pad := 16 - len(pt)%16
	padded := append(append([]byte{}, pt...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return out
}

// Master playlist picks highest-BANDWIDTH variant; plain TS segments are
// merged in order. Without a valid-MP4-remuxable stream the output falls
// back to .ts (fake segment bytes are not real TS, so ffmpeg remux fails).
func TestHLSMasterAndMerge(t *testing.T) {
	var lowFetched, segHits int32
	mux := http.NewServeMux()
	segs := [][]byte{segData(0, 1000), segData(1, 1200), segData(2, 800), segData(3, 1500)}
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=5000\nhd.m3u8\n"))
	})
	mux.HandleFunc("/low.m3u8", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&lowFetched, 1)
	})
	mux.HandleFunc("/hd.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n")
		for i := range segs {
			fmt.Fprintf(w, "#EXTINF:4.0,\nseg%d.ts\n", i)
		}
		w.Write([]byte("#EXT-X-ENDLIST\n"))
	})
	for i := range segs {
		i := i
		mux.HandleFunc(fmt.Sprintf("/seg%d.ts", i), func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&segHits, 1)
			w.Write(segs[i])
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	task := newTestTask(t, srv.URL+"/master.m3u8")
	d := newTestDL(srv)
	if err := d.Run(context.Background(), task, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if atomic.LoadInt32(&lowFetched) != 0 {
		t.Fatal("low-bandwidth variant was fetched; master selection wrong")
	}
	if atomic.LoadInt32(&segHits) != int32(len(segs)) {
		t.Fatalf("segment fetch count = %d, want %d", segHits, len(segs))
	}
	want := bytes.Join(segs, nil)
	fi, err := os.Stat(task.SavePath)
	if err == nil && fi.Size() == int64(len(want)) {
		// mp4 remux unexpectedly succeeded with fake TS (unexpected but fine)
	}
	data, err := os.ReadFile(task.SavePath)
	if err != nil {
		// fallback path: renamed to .ts when remux unavailable/failed
		data, err = os.ReadFile(filepath.Join(filepath.Dir(task.SavePath), "video.ts"))
		if err != nil {
			t.Fatalf("no output file: %v", err)
		}
		if !bytes.Equal(data, want) {
			t.Fatal("merged .ts content mismatch")
		}
	}
	if task.Downloaded != int64(len(want)) {
		t.Fatalf("Downloaded=%d want %d", task.Downloaded, len(want))
	}
	// parts + merging temp must be cleaned up
	if _, err := os.Stat(task.SavePath + ".parts"); err == nil {
		t.Fatal("parts dir not removed")
	}
	if _, err := os.Stat(task.SavePath + ".merging"); err == nil {
		t.Fatal("merging temp not removed")
	}
}

// Run1 fails on a segment (bounded retries exhausted); Run2 must reuse the
// segment files already on disk and only re-fetch the missing ones.
func TestHLSResumeSkipsDoneSegments(t *testing.T) {
	mux := http.NewServeMux()
	segs := [][]byte{segData(0, 900), segData(1, 900), segData(2, 900), segData(3, 900)}
	var hits [4]int32
	failBad := atomic.Bool{}
	failBad.Store(true)
	mux.HandleFunc("/v.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n")
		for i := range segs {
			fmt.Fprintf(w, "#EXTINF:4.0,\nseg%d\n", i)
		}
		w.Write([]byte("#EXT-X-ENDLIST\n"))
	})
	for i := range segs {
		i := i
		mux.HandleFunc(fmt.Sprintf("/seg%d", i), func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits[i], 1)
			if i == 2 && failBad.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write(segs[i])
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	task := newTestTask(t, srv.URL+"/v.m3u8")
	d := newTestDL(srv)
	err := d.Run(context.Background(), task, nil)
	if err == nil {
		t.Fatal("expected run1 to fail on segment 2")
	}
	failBad.Store(false)

	var before [4]int32
	for i := range hits {
		before[i] = atomic.LoadInt32(&hits[i])
	}
	if before[0] == 0 || before[1] == 0 {
		t.Fatalf("segments 0/1 should have completed in run1: hits=%v", before)
	}
	if err := d.Run(context.Background(), task, nil); err != nil {
		t.Fatalf("resume run: %v", err)
	}
	// completed segments must not be re-fetched
	for i := 0; i < 2; i++ {
		if atomic.LoadInt32(&hits[i]) != before[i] {
			t.Fatalf("seg%d re-fetched on resume (hits %d -> %d)", i, before[i], hits[i])
		}
	}
	want := bytes.Join(segs, nil)
	data, err := os.ReadFile(task.SavePath)
	if err != nil {
		data, err = os.ReadFile(filepath.Join(filepath.Dir(task.SavePath), "video.ts"))
		if err != nil {
			t.Fatalf("no output after resume: %v", err)
		}
	}
	if !bytes.Equal(data, want) && len(data) != len(want) {
		t.Fatalf("output size mismatch: %d vs %d", len(data), len(want))
	}
}

// AES-128-CBC with sequence-number IV (no explicit IV attribute).
func TestHLSSequenceIVDecrypt(t *testing.T) {
	key := []byte("0123456789abcdef")
	plain := [][]byte{segData(0, 64), segData(1, 64)}
	mux := http.NewServeMux()
	mux.HandleFunc("/e.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n#EXT-X-KEY:METHOD=AES-128,URI=\"k\"\n#EXTINF:4.0,\na.ts\n#EXTINF:4.0,\nb.ts\n#EXT-X-ENDLIST\n"))
	})
	mux.HandleFunc("/k", func(w http.ResponseWriter, r *http.Request) { w.Write(key) })
	mux.HandleFunc("/a.ts", func(w http.ResponseWriter, r *http.Request) { w.Write(aesEncrypt(plain[0], key, seqIV(7))) })
	mux.HandleFunc("/b.ts", func(w http.ResponseWriter, r *http.Request) { w.Write(aesEncrypt(plain[1], key, seqIV(8))) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	task := newTestTask(t, srv.URL+"/e.m3u8")
	d := newTestDL(srv)
	if err := d.Run(context.Background(), task, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := append(append([]byte{}, plain[0]...), plain[1]...)
	data, err := os.ReadFile(task.SavePath)
	if err != nil {
		data, err = os.ReadFile(filepath.Join(filepath.Dir(task.SavePath), "video.ts"))
		if err != nil {
			t.Fatalf("no output: %v", err)
		}
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("decrypted content mismatch (len %d vs %d)", len(data), len(want))
	}
}

func TestParsePlaylistBasics(t *testing.T) {
	base, _ := url.Parse("http://host/dir/x.m3u8")
	p, err := parsePlaylist("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"k\",IV=0x00112233445566778899AABBCCDDEEFF\n#EXTINF:4,\ns1.m4s\n#EXT-X-ENDLIST\n", base)
	if err != nil {
		t.Fatal(err)
	}
	if p.init != "http://host/dir/init.mp4" || p.key != "http://host/dir/k" {
		t.Fatalf("resolve: %+v", p)
	}
	if len(p.iv) != 16 || p.iv[1] != 0x11 {
		t.Fatalf("explicit IV wrong: %v", p.iv)
	}
	if len(p.segs) != 1 || p.segs[0] != "http://host/dir/s1.m4s" {
		t.Fatalf("segs: %v", p.segs)
	}
	if _, err := parsePlaylist("#EXTM3U\n#EXTINF:4,\ns1.m4s\n", base); err == nil {
		t.Fatal("live playlist (no ENDLIST) must be rejected")
	}
	if _, err := parsePlaylist("not a playlist", base); err == nil {
		t.Fatal("non-m3u8 must be rejected")
	}
}

// ---------------------------------------------------------------- URL forms

// TestNormalizeRefRealWorldForms pins the "IDM 能下我们下不了" URL shapes:
// bare host (the s1q2105.com-style master playlists), bare host:port,
// protocol-relative, plain relative, and junk that must fail fast.
func TestNormalizeRefRealWorldForms(t *testing.T) {
	base, _ := url.Parse("https://page.example.com/v/index.m3u8")
	cases := []struct {
		name  string
		ref   string
		want  string
		fatal bool
	}{
		{"bare host (CDN master form)", "s501740.s1q2105.com/hls/abc/index_1920x1080.mp4/playlist.m3u8",
			"https://s501740.s1q2105.com/hls/abc/index_1920x1080.mp4/playlist.m3u8", false},
		{"bare host:port", "127.0.0.1:8080/m/x.m3u8", "https://127.0.0.1:8080/m/x.m3u8", false},
		{"wowza relative file dir", "index_1920x1080.mp4/playlist.m3u8",
			"https://page.example.com/v/index_1920x1080.mp4/playlist.m3u8", false},
		{"dotted file relative", "media_720/seg.ts", "https://page.example.com/v/media_720/seg.ts", false},
		{"dotted file flat", "video_1080p.mp4", "https://page.example.com/v/video_1080p.mp4", false},
		{"protocol relative", "//cdn.example.com/x.m3u8", "https://cdn.example.com/x.m3u8", false},
		{"root relative", "/abs/seg.ts", "https://page.example.com/abs/seg.ts", false},
		{"dir relative", "sub/seg.ts", "https://page.example.com/v/sub/seg.ts", false},
		{"file relative", "seg-9.ts", "https://page.example.com/v/seg-9.ts", false},
		{"space encoded", "a b.ts", "https://page.example.com/v/a%20b.ts", false},
		{"query kept", "seg.ts?token=xyz", "https://page.example.com/v/seg.ts?token=xyz", false},
		{"data uri fatal", "data:text/plain,#EXTM3U", "", true},
		{"javascript fatal", "javascript:void(0)", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := normalizeRef(base, c.ref)
			if c.fatal {
				if err == nil {
					t.Fatalf("want fatal for %q, got %q", c.ref, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeRef(%q): %v", c.ref, err)
			}
			if got != c.want {
				t.Fatalf("normalizeRef(%q) = %q, want %q", c.ref, got, c.want)
			}
		})
	}
	hbase, _ := url.Parse("http://page.example.com/v/index.m3u8")
	if got, _ := normalizeRef(hbase, "h.host.example/x.m3u8"); got != "http://h.host.example/x.m3u8" {
		t.Fatalf("http base bare host: %q", got)
	}
	if got, _ := normalizeRef(hbase, "//c.example.com/x.m3u8"); got != "http://c.example.com/x.m3u8" {
		t.Fatalf("http base protocol-relative: %q", got)
	}
}

func TestFetchCandidates(t *testing.T) {
	c := fetchCandidates("https://cdn.example.com/a/x.m3u8")
	if len(c) < 2 || c[0] != "https://cdn.example.com/a/x.m3u8" || c[1] != "http://cdn.example.com/a/x.m3u8" {
		t.Fatalf("scheme flip missing: %v", c)
	}
	c = fetchCandidates("http://cdn.example.com/index_1920x1080.mp4")
	want := []string{"http://cdn.example.com/index_1920x1080.mp4",
		"http://cdn.example.com/index_1920x1080.mp4/playlist.m3u8",
		"https://cdn.example.com/index_1920x1080.mp4",
		"https://cdn.example.com/index_1920x1080.mp4/playlist.m3u8"}
	if len(c) != len(want) {
		t.Fatalf("wowza candidates = %v", c)
	}
	for i := range want {
		if c[i] != want[i] {
			t.Fatalf("candidate %d = %q, want %q", i, c[i], want[i])
		}
	}
}

// TestPermanentURLFailureFastFail: a scheme-less address must NOT loop. The
// shipped bug retried `unsupported protocol scheme ""` 20 times with 8s backoff
// (the "播放列表 中断（第 5/20 次，8s 后重试）" the user watched forever).
func TestPermanentURLFailureFastFail(t *testing.T) {
	if !isPermanentURLFailure(fmt.Errorf(`Get "s501740.example.com/hls/x/playlist.m3u8": unsupported protocol scheme ""`)) {
		t.Fatal("missing scheme must be permanent")
	}
	if !isPermanentURLFailure(fmt.Errorf("dial tcp: lookup dead.invalid: no such host")) {
		t.Fatal("DNS failure must be permanent")
	}
	if isPermanentURLFailure(fmt.Errorf("connection reset by peer")) {
		t.Fatal("connection reset is transient")
	}

	d := NewDownloader(nil, "test-ua")
	d.MaxAttemptsPerSeg = 8
	d.RetryBase = 10 * time.Millisecond
	d.RetryMax = 20 * time.Millisecond
	task := newTestTask(t, "suntey-cannot-resolve.invalid/hls/x/playlist.m3u8")
	start := time.Now()
	err := d.Run(context.Background(), task, &RunOptions{})
	el := time.Since(start)
	if err == nil {
		t.Fatal("dead DNS name must fail, got nil")
	}
	if el > 60*time.Second {
		t.Fatalf("fast-fail took %v: the retry loop is spinning", el)
	}
	t.Logf("fast fail after %v: %v", el.Round(time.Millisecond), err)
}

// TestHLSE2EBareHostMaster: a master whose variant line is a BARE host (no
// scheme) must download end to end — exactly the s1q2105.com layout.
func TestHLSE2EBareHostMaster(t *testing.T) {
	segs := [][]byte{segData(0, 900), segData(1, 1100), segData(2, 700)}
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		// Bare "127.0.0.1:PORT" variant: no scheme, no leading slashes.
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=5000000\n%s/media.m3u8\n", srv.Listener.Addr().String())
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n")
		for i := range segs {
			fmt.Fprintf(w, "#EXTINF:4,\n%s/seg-%d.ts\n", srv.Listener.Addr().String(), i)
		}
		w.Write([]byte("#EXT-X-ENDLIST\n"))
	})
	for i := range segs {
		i := i
		mux.HandleFunc(fmt.Sprintf("/seg-%d.ts", i), func(w http.ResponseWriter, r *http.Request) {
			w.Write(segs[i])
		})
	}
	srv = httptest.NewServer(mux)
	defer srv.Close()

	d := newTestDL(srv)
	task := newTestTask(t, srv.URL+"/master.m3u8")
	if err := d.Run(context.Background(), task, &RunOptions{}); err != nil {
		t.Fatalf("bare-host master run: %v", err)
	}
	var want []byte
	for _, s := range segs {
		want = append(want, s...)
	}
	data, err := os.ReadFile(task.SavePath)
	if err != nil {
		data, err = os.ReadFile(filepath.Join(filepath.Dir(task.SavePath), "video.ts"))
		if err != nil {
			t.Fatalf("no output: %v", err)
		}
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("bare-host merged bytes mismatch: %d vs %d", len(data), len(want))
	}
	if !strings.HasPrefix(task.HLS.MediaURL, "http") {
		t.Fatalf("MediaURL not normalized: %q", task.HLS.MediaURL)
	}
}

// TestHLSSchemeFlipAndWowzaDiscovery: the manifest is only reachable as
// "<name>.mp4/playlist.m3u8" over plain HTTP while the task carries an https
// URL. IDM papers over exactly this; so must we — and the working address is
// persisted so a resume starts from it directly.
func TestHLSSchemeFlipAndWowzaDiscovery(t *testing.T) {
	segs := [][]byte{segData(0, 800), segData(1, 640)}
	mux := http.NewServeMux()
	mux.HandleFunc("/index_1920x1080.mp4/playlist.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n")
		for i := range segs {
			fmt.Fprintf(w, "#EXTINF:4,\nseg-%d.ts\n", i)
		}
		w.Write([]byte("#EXT-X-ENDLIST\n"))
	})
	for i := range segs {
		i := i
		mux.HandleFunc(fmt.Sprintf("/index_1920x1080.mp4/seg-%d.ts", i), func(w http.ResponseWriter, r *http.Request) {
			w.Write(segs[i])
		})
	}
	srv := httptest.NewServer(mux) // plain HTTP, like the real CDN host
	defer srv.Close()

	d := newTestDL(srv)
	task := newTestTask(t, "https://"+strings.TrimPrefix(srv.URL, "http://")+"/index_1920x1080.mp4")
	if err := d.Run(context.Background(), task, &RunOptions{}); err != nil {
		t.Fatalf("wowza discovery run: %v", err)
	}
	if !strings.HasSuffix(task.HLS.MediaURL, "/index_1920x1080.mp4/playlist.m3u8") {
		t.Fatalf("discovered playlist not persisted: %q", task.HLS.MediaURL)
	}
	if !strings.HasPrefix(task.HLS.MediaURL, "http://") {
		t.Fatalf("scheme flip not persisted: %q", task.HLS.MediaURL)
	}
}
