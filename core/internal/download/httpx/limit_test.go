package httpx

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestThrottleTokenBucket(t *testing.T) {
	t.Cleanup(func() { SetRate(0) })
	SetRate(2000) // 2 KB/s
	ResetRateBucket()
	body := bytes.Repeat([]byte("x"), 5000)
	r := newThrottle(context.Background(), bytes.NewReader(body))
	buf := make([]byte, 1000)
	start := time.Now()
	var got int
	for {
		n, err := r.Read(buf)
		got += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	elapsed := time.Since(start)
	if got != 5000 {
		t.Fatalf("got %d bytes", got)
	}
	// 5000 B at 2000 B/s with an initially empty bucket ≈ 2.5s of token wait.
	if elapsed < 2*time.Second || elapsed > 4*time.Second {
		t.Fatalf("throttle timing off: %v (want ~2.5s)", elapsed)
	}
	SetRate(0)
	if SetRate(-5); Rate() != 0 {
		t.Fatalf("negative rate must clamp to 0, got %d", Rate())
	}
	start = time.Now()
	if _, err := io.ReadAll(newThrottle(context.Background(), bytes.NewReader(bytes.Repeat([]byte("y"), 1<<20)))); err != nil {
		t.Fatalf("unlimited read: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("unlimited throttle added %v", d)
	}
}

// TestRateLimitAppliesToResponses proves the Transport ReadLimiter wiring:
// the same client with no cap downloads 30 KiB near-instantly, while a
// 60 KB/s cap needs at least ~0.4s of token waits for the body.
func TestRateLimitAppliesToResponses(t *testing.T) {
	t.Cleanup(func() { SetRate(0); httpxRateOffCheck() })
	payload := bytes.Repeat([]byte{7}, 30720)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	cl := NewClient()
	Get := func() (int64, time.Duration) {
		st := time.Now()
		resp, err := cl.Get(srv.URL)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			t.Fatalf("copy: %v", err)
		}
		return n, time.Since(st)
	}

	SetRate(0)
	ResetRateBucket()
	if n, d := Get(); n != int64(len(payload)) || d > 300*time.Millisecond {
		t.Fatalf("unlimited run wrong: n=%d dur=%v", n, d)
	}

	SetRate(60000) // 60 KB/s -> 30 KB needs ~0.4s+
	ResetRateBucket()
	n, d := Get()
	if n != int64(len(payload)) {
		t.Fatalf("limited run wrong size: %d", n)
	}
	if d < 350*time.Millisecond || d > 3*time.Second {
		t.Fatalf("rate limit not effective or grossly wrong: %v for %d B", d, n)
	}
}

func httpxRateOffCheck() {
	if Rate() != 0 {
		SetRate(0)
	}
}

// Ensure the probe path is not throttled into the timing of small reads:
// one probe-sized body must pass instantly even under a low cap.
func TestProbeSizedReadUnderCap(t *testing.T) {
	t.Cleanup(func() { SetRate(0) })
	SetRate(2000)
	ResetRateBucket()
	start := time.Now()
	if _, err := io.ReadAll(newThrottle(context.Background(), bytes.NewReader([]byte("x")))); err != nil {
		t.Fatalf("tiny read: %v", err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("probe-sized read blocked %v", d)
	}
}

// TestStopInterruptsRateLimitedReads proves the fix for the pause-under-rate
// hang: token-bucket sleeps are bound to the request context, so cancelling
// a download (what Manager.Stop does) cuts through them instead of waiting
// every leg out (measured pre-fix: 5-8s to settle, probe saw worse).
func TestStopInterruptsRateLimitedReads(t *testing.T) {
	skipLong(t, "server throttles the body to ~1 KiB/s on purpose")
	t.Cleanup(func() { SetRate(0); ResetRateBucket() })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2097152")
		w.Header().Set("Accept-Ranges", "bytes")
		chunk := make([]byte, 4096)
		for i := 0; i < 512; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer srv.Close()
	SetRate(32 << 10)
	ResetRateBucket()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: &limitRT{base: tr}}

	// the cancelled leg: this is what pause does
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	go func() { time.Sleep(300 * time.Millisecond); cancel() }()
	io.Copy(io.Discard, resp.Body) // ends with context canceled, expected
	resp.Body.Close()
	cancelled := time.Since(started)
	if cancelled > 2500*time.Millisecond {
		t.Fatalf("cancelled read took %v to stop under 32 KiB/s (want <2.5s: bucket sleeps must be interruptible)", cancelled)
	}

	// control: the same leg without cancel is clearly slower — proves the
	// server is actually rate-bound, not just fast
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	req2, _ := http.NewRequestWithContext(ctx2, http.MethodGet, srv.URL, nil)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("control request: %v", err)
	}
	started2 := time.Now()
	io.CopyN(io.Discard, resp2.Body, 256<<10)
	resp2.Body.Close()
	full := time.Since(started2)
	if full <= cancelled {
		t.Fatalf("control leg finished in %v (cancelled was %v) — server too fast to prove cancellation saved time", full, cancelled)
	}
}
