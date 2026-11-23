package httpx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"

// NewClient builds a tuned http.Client: keep-alive, generous idle pool so N
// concurrent chunk workers reuse connections. TLS-fingerprint (utls) is a
// documented M2+ drop-in at the transport dialer.
func NewClient() *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 128,
		MaxConnsPerHost:     256,
		IdleConnTimeout:     90 * time.Second,
		// A pooled conn whose peer went half-dead (mid-download cancel +
		// RST) would otherwise strand a fresh attempt until the 30s idle
		// guard; 10s response headers caps that stall. IDM-grade resume.
		ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	// no global Timeout: large downloads are long-lived. The transport is
	// wrapped so every response body passes the shared token bucket
	// (IDM-style global speed cap, SetRate); unlimited rate adds no delay.
	return &http.Client{Transport: &limitRT{base: tr}}
}

// limitRT wraps response bodies for the global rate cap.
type limitRT struct{ base http.RoundTripper }

func (l *limitRT) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := l.base.RoundTrip(req)
	if err == nil && resp.Body != nil && Rate() > 0 {
		// bind the throttle to the request context: cancelling a download
		// (pause/stop) must cut through bucket sleeps, not wait them out.
		resp.Body = newThrottle(req.Context(), resp.Body)
	}
	return resp, err
}

// applyHeader clones req and applies the request's replay headers.
func applyHeader(req *http.Request, r model.Request, ua string) {
	if ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	if r.Referer != "" {
		req.Header.Set("Referer", r.Referer)
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
}

// Probe resolves size + Range support without downloading the body.
// Strategy: GET with "Range: bytes=0-0". A 206 + Content-Range => supportRange.
// A 200 => no Range support (full body; close immediately). Falls back to HEAD.
func Probe(ctx context.Context, c *http.Client, r model.Request, ua string) (*model.ResolveResult, error) {
	name := filenameFrom(r)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	applyHeader(req, r, ua)
	req.Header.Set("Range", "bytes=0-0")

	resp, err := c.Do(req)
	if err != nil {
		// Network/URL failure — surface it (fixes GoPeed's silent-failure class).
		return nil, fmt.Errorf("probe: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
		resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("probe: HTTP %d %s", resp.StatusCode, resp.Status)
	}

	res := &model.ResolveResult{Name: firstNonEmpty(name, filenameFromResp(resp, r.URL)), Accepts: true}

	switch resp.StatusCode {
	case http.StatusPartialContent: // 206
		res.SupportRange = true
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if total, ok := parseContentRangeTotal(cr); ok {
				res.Size = total
			}
		}
	default: // 200 — server ignored the probe Range
		res.Size = resp.ContentLength // -1 if unknown
		res.SupportRange = strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")
		// Many servers (CDN speed endpoints) honor Range without advertising
		// Accept-Ranges. Confirm with a 2nd probe so we can use N-conn parallel
		// (this is the reliability fix vs GoPeed's fixed-size splitting).
		if !res.SupportRange && res.Size > 0 {
			res.SupportRange = confirmRange(ctx, c, r, ua)
		}
	}
	if res.Size == 0 {
		res.Size = -1
	}
	// M6 page resolver entry: an HTML page masquerading as a download.
	// Small text/html bodies count (a 200 full body we never read is bounded
	// by ContentLength; a real binary is usually far larger) — and so do
	// unknown-size chunked text/html responses (dynamic share pages: douyin,
	// bilibili … no Content-Length at all, which used to defeat the old
	// `0<=size<=64KiB` test and downloaded the HTML shell as a file).
	res.IsPage = strings.Contains(resp.Header.Get("Content-Type"), "text/html") &&
		(res.Size < 0 || res.Size <= 64<<10)
	return res, nil
}

// confirmRange sends "Range: bytes=2-3"; a 206 response proves Range support.
func confirmRange(ctx context.Context, c *http.Client, r model.Request, ua string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return false
	}
	applyHeader(req, r, ua)
	req.Header.Set("Range", "bytes=2-3")
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4))
		resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusPartialContent {
		return true
	}
	// Fallback: a 200 whose body is tiny (==2 bytes) implies the range was honored.
	if resp.StatusCode == http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 3))
		return len(b) <= 2
	}
	return false
}

// parseContentRangeTotal parses "bytes 0-0/12345" -> 12345.
func parseContentRangeTotal(v string) (int64, bool) {
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return 0, false
	}
	tail := strings.TrimSpace(v[i+1:])
	if tail == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(tail, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// filenameFromResp extracts a filename from Content-Disposition, else URL base.
func filenameFromResp(resp *http.Response, rawurl string) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if fn := parseDispositionFilename(cd); fn != "" {
			return fn
		}
	}
	if u, err := url.Parse(rawurl); err == nil {
		if b := path.Base(u.Path); b != "" && b != "/" && b != "." {
			return b
		}
	}
	return "download.bin"
}

// parseDispositionFilename pulls filename= / filename*= out of a
// Content-Disposition header value (basic form; RFC5987 * form decoded best-effort).
func parseDispositionFilename(cd string) string {
	for _, part := range strings.Split(cd, ";") {
		part = strings.TrimSpace(part)
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, "filename*=") {
			v := part[len("filename*="):]
			// charset'lang'encoded
			if idx := strings.Index(v, "''"); idx >= 0 {
				if dec, err := url.QueryUnescape(v[idx+2:]); err == nil {
					return dec
				}
			}
		}
		if strings.HasPrefix(lower, "filename=") {
			v := strings.Trim(part[len("filename="):], "\" ")
			return v
		}
	}
	return ""
}

func filenameFrom(r model.Request) string {
	if r.Filename != "" {
		return r.Filename
	}
	return ""
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
