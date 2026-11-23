package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tey-sun/suntey/core/internal/model"
)

const pageHTML = `<!doctype html><html><body>
<a href="files/big.zip?token=9">zip link</a>
<a href="x.pdf">pdf</a>
<a href="/y.mp4">mp4 abs</a>
<a href="/blog/post">page noise</a>
<a href="ftp://example.com/z.exe">scheme noise</a>
<video src="/shell.mp4"><source src="v.webm"></video>
<meta property="og:video" content="http://og.test/og.mp4">
<script>window._ROUTER={"playAddr":"http:\/\/v.douyin.test\/a\/b.mp4?sign=3"}</script>
</body></html>`

func pageServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(pageHTML))
	})
	mux.HandleFunc("/files/big.zip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", "2048")
		w.WriteHeader(200)
	})
	mux.HandleFunc("/chunk", func(w http.ResponseWriter, r *http.Request) {
		// No Content-Length: three 1KB writes force chunked encoding — the
		// shape of dynamic share pages (douyin/bilibili) that broke IsPage.
		w.Header().Set("Content-Type", "text/html")
		block := strings.Repeat("x", 1024)
		w.WriteHeader(200)
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte(block))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})
	return httptest.NewServer(mux)
}

func TestParsePage(t *testing.T) {
	srv := pageServer(t)
	defer srv.Close()
	cands, err := ParsePage(context.Background(), NewClient(), model.Request{URL: srv.URL + "/page"}, "test-ua")
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 7 {
		t.Fatalf("want 7 candidates, got %d: %+v", len(cands), cands)
	}
	if !strings.Contains(cands[0].URL, "files/big.zip?token=9") {
		t.Fatalf("first candidate must be the zip with query, got %q", cands[0].URL)
	}
	for _, c := range cands {
		if strings.Contains(c.URL, "blog/post") || strings.Contains(c.URL, "ftp") {
			t.Fatalf("noise leaked: %q", c.URL)
		}
	}
	if cands[0].Name != "big" {
		t.Fatalf("name must strip ext, got %q", cands[0].Name)
	}
	// New surfaces: video@src, og:video meta, JSON-embedded (douyin-style).
	want := []string{"shell.mp4", "og.test/og.mp4", "v.douyin.test/a/b.mp4?sign=3"}
	for _, w := range want {
		found := false
		for _, c := range cands {
			if strings.Contains(c.URL, w) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing candidate for %q in %+v", w, cands)
		}
	}
}

func TestProbeIsPage(t *testing.T) {
	srv := pageServer(t)
	defer srv.Close()
	res, err := Probe(context.Background(), NewClient(), model.Request{URL: srv.URL + "/page"}, "test-ua")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsPage {
		t.Fatal("small text/html body must be IsPage")
	}
}

func TestProbeIsPageChunked(t *testing.T) {
	srv := pageServer(t)
	defer srv.Close()
	res, err := Probe(context.Background(), NewClient(), model.Request{URL: srv.URL + "/chunk"}, "test-ua")
	if err != nil {
		t.Fatal(err)
	}
	if res.Size != -1 {
		t.Fatalf("chunked no Content-Length must probe size -1, got %d", res.Size)
	}
	if !res.IsPage {
		t.Fatal("chunked text/html (dynamic share page) must be IsPage")
	}
}

func TestResolvePage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body><a href="files/report.pdf">r</a><a href="/blog/post">noise</a></body></html>`))
	})
	mux.HandleFunc("/files/report.pdf", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(200)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := ResolvePage(context.Background(), NewClient(), model.Request{URL: srv.URL + "/page"}, "test-ua", Probe)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "report.pdf" && res.Name != "report" {
		t.Fatalf("name = %q", res.Name)
	}
	if res.Size != 1024 {
		t.Fatalf("size = %d, want 1024", res.Size)
	}
	if !strings.HasSuffix(res.ProbedURL, "/files/report.pdf") {
		t.Fatalf("probedUrl = %q", res.ProbedURL)
	}
}

// M6.5: the UI candidate picker needs every probed link, not just the winner.
func TestResolvePageCandidates(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body>
			<a href="a.zip">A</a>
			<a href="b.zip?token=9">B</a>
			<a href="/blog/post">noise</a>
		</body></html>`))
	})
	for _, p := range []string{"/a.zip", "/b.zip"} {
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Length", "2048")
			w.WriteHeader(200)
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := ResolvePage(context.Background(), NewClient(), model.Request{URL: srv.URL + "/page"}, "test-ua", Probe)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsPage {
		t.Fatal("result of a page parse must keep IsPage=true so the UI picker shows the candidate modal")
	}
	if len(res.Candidates) != 2 {
		t.Fatalf("candidates = %d, want 2 (noise link has no download ext)", len(res.Candidates))
	}
	if !strings.HasSuffix(res.Candidates[0].URL, "/a.zip") {
		t.Fatalf("candidate[0] = %q, want a.zip (page order)", res.Candidates[0].URL)
	}
	if !strings.HasSuffix(res.Candidates[1].URL, "/b.zip?token=9") {
		t.Fatalf("candidate[1] = %q, want b.zip?token=9", res.Candidates[1].URL)
	}
	for _, c := range res.Candidates {
		if c.Size != 2048 || c.Name == "" {
			t.Fatalf("candidate missing name/size: %+v", c)
		}
	}
}

func TestResolvePageNoCandidates(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body><a href="/only/blog">blog</a></body></html>`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	_, err := ResolvePage(context.Background(), NewClient(), model.Request{URL: srv.URL + "/page"}, "ua", Probe)
	if err == nil || !strings.Contains(err.Error(), "未找到") {
		t.Fatalf("want 未找到 error, got %v", err)
	}
}

func TestIsDownloadExt(t *testing.T) {
	yes := []string{"a.zip", "/p/q/x.MP4", "https://x/y.7z", "archive.tar.gz"}
	no := []string{"page.html", "/index.php", "noext", "x.pdf.bak"}
	for _, s := range yes {
		if !IsDownloadExt(s) {
			t.Errorf("must accept %q", s)
		}
	}
	for _, s := range no {
		if IsDownloadExt(s) {
			t.Errorf("must reject %q", s)
		}
	}
}

// osu.ppy.sh-style anti-bot hosts: bare Go clients get 403, browser-like UA
// gets a proper 206 — the shipped DefaultUserAgent is what makes pasted
// gaming/CDN links addable at all.
func TestProbeDefaultUABypassesAntiBot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/osu", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != DefaultUserAgent {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", "bytes 0-0/2097152")
		w.WriteHeader(206)
		w.Write([]byte{0})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	res, err := Probe(context.Background(), NewClient(), model.Request{URL: srv.URL + "/osu"}, DefaultUserAgent)
	if err != nil {
		t.Fatalf("browser UA must pass anti-bot probe: %v", err)
	}
	if res.Size != 2097152 || !res.SupportRange {
		t.Fatalf("res = %+v", res)
	}
	if _, err := Probe(context.Background(), NewClient(), model.Request{URL: srv.URL + "/osu"}, "Go-http-client/1.1"); err == nil {
		t.Fatal("bare Go UA must get the 403 error")
	}
}
