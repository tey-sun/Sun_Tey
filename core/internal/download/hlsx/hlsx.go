// Package hlsx downloads HLS (m3u8) video streams: master/media playlist
// parsing, parallel segment fetch with bounded retries + resume (completed
// segments are files on disk, rebuilt from the filesystem after a crash),
// AES-128-CBC decryption, concat merge and MP4 remux via ffmpeg when
// available (lossless -c copy).
package hlsx

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
)

// RunOptions carries the callbacks the manager provides (mirrors httpx).
type RunOptions struct {
	Emit    func(model.Event)
	Persist func(downloaded int64)
	Save    func() error // persist task mutations (plan, parts dir, renamed final path)
}

// Downloader performs parallel HLS segment downloads with resume.
type Downloader struct {
	Client            *http.Client
	UA                string
	MaxAttemptsPerSeg int           // lifetime attempts per segment per run
	RetryBase         time.Duration // first backoff step
	RetryMax          time.Duration // backoff ceiling
	AttemptTimeout    time.Duration // per-attempt wall clock limit
	BufferKB          int           // disk write buffer (KB)
}

func NewDownloader(c *http.Client, ua string) *Downloader {
	return &Downloader{
		Client:            c,
		UA:                ua,
		MaxAttemptsPerSeg: 8,
		RetryBase:         500 * time.Millisecond,
		RetryMax:          6 * time.Second,
		AttemptTimeout:    30 * time.Second,
		BufferKB:          128,
	}
}

func (d *Downloader) bufSize() int {
	if d.BufferKB > 0 {
		return d.BufferKB * 1024
	}
	return 128 * 1024
}

// fatalErr: retrying cannot help. transientErr: network / 5xx worth retrying.
type fatalErr struct{ msg string }

func (e *fatalErr) Error() string { return e.msg }

type transientErr struct{ err error }

func (e *transientErr) Error() string { return e.err.Error() }

// IsHLSURL reports whether the URL path names an m3u8 playlist.
func IsHLSURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.ToLower(u.Path), ".m3u8")
}

// ---------------------------------------------------------------- playlists

type variant struct {
	bw  int
	uri string
}

type parsed struct {
	masterMedia string // non-empty for master playlists: best variant URI (resolved)
	init        string
	key         string // resolved KEY URI ("" = none)
	iv          []byte // explicit IV (nil = per-segment sequence IV)
	seq         uint64
	segs        []string // resolved segment URIs
}

func attrStr(line, key string) string {
	i := strings.Index(line, key+`="`)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key)+2:]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// attrRaw reads an attribute that may be quoted (URI="..") or bare (IV=0x..).
func attrRaw(line, key string) string {
	i := strings.Index(line, key+"=")
	if i < 0 {
		return ""
	}
	rest := line[i+len(key)+1:]
	if len(rest) > 0 && rest[0] == '"' {
		if j := strings.IndexByte(rest[1:], '"'); j >= 0 {
			return rest[1 : 1+j]
		}
		return rest[1:]
	}
	if j := strings.IndexAny(rest, ", "); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func attrInt(line, key string) int {
	i := strings.Index(line, key+"=")
	if i < 0 {
		return -1
	}
	rest := line[i+len(key)+1:]
	if j := strings.IndexAny(rest, ",\r\n\t "); j >= 0 {
		rest = rest[:j]
	}
	n, _ := strconv.Atoi(strings.TrimSpace(rest))
	return n
}

// normalizeRef turns one playlist URI reference into a fetchable absolute URL.
//
// Real-world playlists and pages are much messier than RFC 8216 suggests, and
// this is the difference between "IDM 能下我们下不了":
//   - "//host/path"            protocol-relative (video src, some CDNs)
//   - "host.tld/a/b.m3u8"      bare host, no scheme, no leading slashes —
//     exactly what the *.playcvld.com / *.s1q2105.com
//     master playlists emit for their variant streams
//   - "/a/b.ts", "a/b.ts"      plain relative references
//   - "http://host/ a.ts"      spaces, quotes, control characters
//
// A bare host must be detected before url.Parse, because url.Parse happily
// returns a scheme-less PATH reference for "host.tld/x" and base resolution
// then appends a hostname to the base directory, yielding a 404 that looks like
// an expired link.
func normalizeRef(base *url.URL, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	ref = strings.Trim(ref, `"'<>`)
	if ref == "" {
		return "", nil
	}
	// Percent-encode characters that break http.NewRequestWithContext.
	ref = strings.ReplaceAll(ref, " ", "%20")
	ref = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, ref)

	lower := strings.ToLower(ref)
	switch {
	case strings.HasPrefix(lower, "http://"), strings.HasPrefix(lower, "https://"):
		return ref, nil
	case strings.HasPrefix(ref, "//"):
		scheme := "https"
		if base != nil && base.Scheme != "" {
			scheme = base.Scheme
		}
		return scheme + ":" + ref, nil
	case strings.HasPrefix(lower, "data:"):
		return "", &fatalErr{"播放列表使用了 data: 内嵌数据，暂不支持"}
	case strings.HasPrefix(lower, "file:"), strings.HasPrefix(lower, "javascript:"):
		return "", &fatalErr{"播放列表包含非法地址: " + brief(ref)}
	}

	if !looksLikeBareHost(ref) {
		if base == nil {
			return ref, nil
		}
		ru, err := url.Parse(ref)
		if err != nil {
			return "", &fatalErr{fmt.Sprintf("无效地址 %s: %v", brief(ref), err)}
		}
		return base.ResolveReference(ru).String(), nil
	}
	// Bare "host.tld/path": the resource lives on that host. Prefer HTTPS (the
	// page it came from is HTTPS, and plain HTTP is usually blocked/redirected
	// by these CDNs); fetchCandidates retries the other scheme.
	scheme := "https"
	if base != nil && base.Scheme == "http" {
		scheme = "http"
	}
	return scheme + "://" + ref, nil
}

// looksLikeBareHost reports whether s is "host.tld/path..." with no scheme and
// no leading slash — the form these CDN master playlists emit for variant
// streams. Browsers/IDM resolve everything else against the base URL, so the
// rules are deliberately strict:
//   - a slash is REQUIRED ("seg0.ts", "video.mp4" are relative files, never
//     hosts — the no-slash form must resolve against the base);
//   - labels may only contain DNS-legal characters — "_" marks a filename
//     ("media_720/seg.ts", "index_1920x1080.mp4/playlist.m3u8");
//   - the final label must be an alphabetic TLD >= 2 chars ("mp4"/"m3u8"/"ts"
//     are extensions, ".com"/".cn" are TLDs), unless the host is a dotted-quad
//     IPv4 (LAN/P2P streams use bare "127.0.0.1:8080/path").
func looksLikeBareHost(s string) bool {
	i := strings.IndexAny(s, "/?#")
	head := s
	if i >= 0 {
		head = s[:i]
	}
	if i < 0 || s[i] != '/' {
		return false // no path part: relative file, not a host
	}
	if head == "" || strings.Contains(head, "%") {
		return false
	}
	if strings.Count(head, ":") > 1 {
		return false // "scheme://host" leftovers, IPv6, junk
	}
	host, port := head, ""
	if j := strings.LastIndex(head, ":"); j >= 0 {
		host, port = head[:j], head[j+1:]
		if port == "" {
			return false
		}
		for _, r := range port {
			if r < '0' || r > '9' {
				return false // "user@host:..." or junk — not host:port
			}
		}
	}
	if !strings.Contains(host, ".") {
		return false // plain relative directory
	}
	labels := strings.Split(host, ".")
	ipv4 := len(labels) == 4
	for k, lbl := range labels {
		if lbl == "" {
			return false
		}
		for _, r := range lbl {
			ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
				(r >= '0' && r <= '9') || r == '-'
			if !ok {
				return false // "index_1920x1080" — filenames use "_" etc.
			}
			if ipv4 && (r < '0' || r > '9') {
				ipv4 = false
			}
		}
		if k == len(labels)-1 && !ipv4 {
			// final label = TLD: alphabetic, >= 2 chars. "mp4", "m3u8", "ts"
			// contain digits -> filename extensions, not hosts.
			if len(lbl) < 2 {
				return false
			}
			for _, r := range lbl {
				if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
					return false
				}
			}
		}
	}
	return true
}

func brief(s string) string {
	r := []rune(s)
	if len(r) > 160 {
		return string(r[:160]) + "…"
	}
	return s
}

func resolveURL(base *url.URL, ref string) string {
	s, err := normalizeRef(base, ref)
	if err != nil {
		return ""
	}
	return s
}

// fetchCandidates are the URLs to try, in order, for one target.
//
// The CDNs in the wild fail in three ways that IDM silently papers over: the
// page is HTTPS but the playlist names an HTTP host that is blocked; the video
// URL is really the "…/index_1920x1080.mp4" prefix of a Wowza-style layout
// whose real manifest is "…/index_1920x1080.mp4/playlist.m3u8"; and some hosts
// serve the manifest only over the other scheme. The shapes are tried
// same-scheme first, then flipped, each with the Wowza suffix variant — bounded
// (max 4 GETs), so the fast-fail rules below still apply.
func fetchCandidates(u string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	addWith := func(s string) {
		add(s)
		// Wowza layout: "<name>.mp4" is the directory of
		// "<name>.mp4/playlist.m3u8".
		if !strings.HasSuffix(strings.ToLower(s), ".m3u8") {
			add(strings.TrimRight(s, "/") + "/playlist.m3u8")
		}
	}
	addWith(u)
	switch {
	case strings.HasPrefix(u, "https://"):
		addWith("http://" + u[len("https://"):])
	case strings.HasPrefix(u, "http://"):
		addWith("https://" + u[len("http://"):])
	}
	return out
}

// isPermanentURLFailure classifies transport errors that no amount of retrying
// can fix. The old code retried all of them 20 times with 8s backoff, which is
// the "下载卡在 0%，一直提示中断重试" symptom: an address with no scheme, a
// blocked cleartext transport, a DNS failure or a TLS/proxy problem is a
// configuration/link fact, not a transient hiccup.
func isPermanentURLFailure(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, pat := range []string{
		"unsupported protocol scheme",
		`unsupported protocol ""`,
		"invalid url",
		"missing port",
		"plain http is not allowed",
		"no such host",
		"nodata",
		"certificate",
		"tls:",
		"x509",
		"proxy connect",
		"illegal character",
		"invalid character",
		"net/http: nil context",
	} {
		if strings.Contains(s, pat) {
			return true
		}
	}
	return false
}

func humanizeTransportErr(rawurl string, err error) string {
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "unsupported protocol scheme"):
		return fmt.Sprintf("播放列表地址缺少协议前缀（不是 http/https 开头）：%s。内核已尝试自动补全协议仍失败，通常是扩展/复制到的地址不完整 — 请粘贴以 https:// 开头的完整 m3u8 地址", brief(rawurl))
	case strings.Contains(s, "no such host"), strings.Contains(s, "nodata"):
		return fmt.Sprintf("域名解析失败（%s）：本机 DNS 拦不掉该 CDN，请检查网络/代理/加速节点", brief(rawurl))
	case strings.Contains(s, "plain http is not allowed"):
		return fmt.Sprintf("该地址是明文 http，被系统拒绝（%s）— 请改用 https 地址", brief(rawurl))
	case strings.Contains(s, "certificate"), strings.Contains(s, "x509"), strings.Contains(s, "tls:"):
		return fmt.Sprintf("HTTPS 证书校验失败（%s）：多为 CDN 证书链/时间/代理问题", brief(rawurl))
	case strings.Contains(s, "proxy connect"):
		return fmt.Sprintf("代理连接失败（%s）：本机代理不支持该请求", brief(rawurl))
	}
	return err.Error()
}

func parsePlaylist(text string, base *url.URL) (*parsed, error) {
	if !strings.HasPrefix(strings.TrimSpace(text), "#EXTM3U") {
		return nil, &fatalErr{"目标不是 m3u8 播放列表（缺 #EXTM3U，可能链接已过期或被防盗链拦截）"}
	}
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	p := &parsed{}
	master := false
	var variants []variant
	var keyURI string
	var keyIV []byte
	keySeen := false
	endList := false
	for i := 0; i < len(lines); i++ {
		ln := strings.TrimSpace(lines[i])
		switch {
		case ln == "" || ln == "#EXTM3U":
		case strings.HasPrefix(ln, "#EXT-X-STREAM-INF"):
			master = true
			bw := attrInt(ln, "BANDWIDTH")
			for j := i + 1; j < len(lines); j++ {
				u := strings.TrimSpace(lines[j])
				if u != "" && !strings.HasPrefix(u, "#") {
					variants = append(variants, variant{bw, u})
					i = j
					break
				}
			}
		case strings.HasPrefix(ln, "#EXT-X-MAP"):
			p.init = attrStr(ln, "URI")
		case strings.HasPrefix(ln, "#EXT-X-KEY"):
			if strings.Contains(ln, "METHOD=NONE") {
				keyURI, keyIV = "", nil
				continue
			}
			if !strings.Contains(ln, "METHOD=AES-128") {
				return nil, &fatalErr{"不支持的 HLS 加密方式（仅支持 AES-128）"}
			}
			ku := attrStr(ln, "URI")
			ivs := strings.ToLower(attrRaw(ln, "IV"))
			if ivs != "" {
				b, err := hex.DecodeString(strings.TrimPrefix(ivs, "0x"))
				if err == nil && len(b) == 16 {
					keyIV = b
				}
			} else {
				keyIV = nil
			}
			if keySeen && ku != keyURI {
				return nil, &fatalErr{"HLS 加密 key 在播放列表中途变更，暂不支持"}
			}
			keyURI, keySeen = ku, true
		case strings.HasPrefix(ln, "#EXT-X-MEDIA-SEQUENCE"):
			j := strings.IndexByte(ln, ':')
			if j >= 0 {
				n, _ := strconv.ParseUint(strings.TrimSpace(ln[j+1:]), 10, 64)
				p.seq = n
			}
		case strings.HasPrefix(ln, "#EXT-X-ENDLIST"):
			endList = true
		case strings.HasPrefix(ln, "#"):
		default:
			if master {
				continue
			}
			p.segs = append(p.segs, ln)
		}
	}
	if master {
		if len(variants) == 0 {
			return nil, &fatalErr{"master 播放列表里没有可用的变体流"}
		}
		sort.SliceStable(variants, func(a, b int) bool { return variants[a].bw > variants[b].bw })
		p.masterMedia = resolveURL(base, variants[0].uri)
		return p, nil
	}
	if len(p.segs) == 0 {
		return nil, &fatalErr{"m3u8 播放列表里没有分片"}
	}
	if !endList {
		return nil, &fatalErr{"暂不支持直播流（无 #EXT-X-ENDLIST），请下载点播/回放视频"}
	}
	p.init = resolveURL(base, p.init)
	p.key = resolveURL(base, keyURI)
	for i := range p.segs {
		p.segs[i] = resolveURL(base, p.segs[i])
	}
	p.iv = keyIV
	return p, nil
}

// ---------------------------------------------------------------- fetching

func (d *Downloader) headers(t *model.Task) map[string]string {
	h := map[string]string{}
	for k, v := range t.Req.Header {
		h[k] = v
	}
	if d.UA != "" && h["User-Agent"] == "" {
		h["User-Agent"] = d.UA
	}
	return h
}

func readAllN(r io.Reader, n int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, n))
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	r := []rune(s)
	if len(r) > 120 {
		r = r[:120]
	}
	return ": " + string(r)
}

// getOnce performs one GET; transient failures wrapped as transientErr,
// unfixable ones as fatalErr (see isPermanentURLFailure).
func (d *Downloader) getOnce(ctx context.Context, rawurl string, hdr map[string]string, maxBody int64) ([]byte, error) {
	u := rawurl
	if !strings.HasPrefix(strings.ToLower(u), "http://") && !strings.HasPrefix(strings.ToLower(u), "https://") {
		// Defensive: task/playlist input may be scheme-less ("host/x.m3u8") or
		// protocol-relative ("//host/x.m3u8"). Fetching such a string fails with
		// `unsupported protocol scheme ""`, which the retry loop used to spin on.
		if fixed, err := normalizeRef(nil, u); err != nil {
			return nil, err
		} else if fixed != "" {
			u = fixed
		}
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, &fatalErr{fmt.Sprintf("无效地址 %s: %v", brief(u), err)}
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if isPermanentURLFailure(err) {
			return nil, &fatalErr{humanizeTransportErr(u, err)}
		}
		return nil, &transientErr{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readAllN(resp.Body, 4096)
		switch {
		case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500:
			return nil, &transientErr{fmt.Errorf("HTTP %d", resp.StatusCode)}
		default:
			return nil, &fatalErr{fmt.Sprintf("HTTP %d（链接可能已过期或被防盗链拦截）%s", resp.StatusCode, snippet(body))}
		}
	}
	body, err := readAllN(resp.Body, maxBody)
	if err != nil {
		return nil, &transientErr{err}
	}
	return body, nil
}

// fetchRetry wraps getOnce with bounded backoff (same anti-loop philosophy as
// the httpx chunk retry: bounded attempts, give-up with a clear reason).
//
// Each attempt walks fetchCandidates (scheme flip + the Wowza
// "<name>.mp4/playlist.m3u8" layout), and a fatal classification stops
// immediately: DNS failure, missing scheme, certificate and proxy problems are
// facts about the link, and the old 20x8s loop on those is exactly what looked
// like "下载一直循环、永远不动".
func (d *Downloader) fetchRetry(ctx context.Context, rawurl string, hdr map[string]string, label string, taskID string, emit func(model.Event)) ([]byte, error) {
	b, _, err := d.fetchRetryURL(ctx, rawurl, hdr, label, taskID, emit)
	return b, err
}

func (d *Downloader) fetchRetryURL(ctx context.Context, rawurl string, hdr map[string]string, label string, taskID string, emit func(model.Event)) ([]byte, string, error) {
	delay := d.RetryBase
	var last error
	for att := 1; att <= d.MaxAttemptsPerSeg; att++ {
		var attemptErr error
		var lastFatal *fatalErr
		for _, cand := range fetchCandidates(rawurl) {
			actx, cancel := context.WithTimeout(ctx, d.AttemptTimeout)
			body, err := d.getOnce(actx, cand, hdr, 512<<20)
			cancel()
			if err == nil {
				return body, cand, nil
			}
			if ctx.Err() != nil {
				return nil, "", ctx.Err()
			}
			// A fatal on ONE candidate shape (e.g. a 404 on the bare
			// "<name>.mp4") must not skip the remaining shapes ("<name>.mp4/
			// playlist.m3u8"): IDM probes them all. Only when every shape
			// failed do we surface the fatal — it is the strongest signal.
			if fe, ok := err.(*fatalErr); ok {
				lastFatal = fe
				continue
			}
			attemptErr = err
		}
		if lastFatal != nil {
			return nil, "", lastFatal
		}
		last = attemptErr
		if att < d.MaxAttemptsPerSeg && emit != nil {
			level := "info"
			if att >= 5 {
				level = "error"
			} else if att >= 3 {
				level = "warn"
			}
			emit(model.Event{Type: "retry", TaskID: taskID, Attempt: att, Level: level,
				Err: fmt.Sprintf("%s 中断（第 %d/%d 次，%v 后重试）：%v",
					label, att, d.MaxAttemptsPerSeg, delay.Round(10*time.Millisecond), last),
				TS: time.Now().UnixMilli()})
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > d.RetryMax {
			delay = d.RetryMax
		}
	}
	return nil, "", fmt.Errorf("%s 下载失败（已重试 %d 次仍中断）：%v", label, d.MaxAttemptsPerSeg-1, last)
}

// fetchPlaylist fetches a manifest and reports which URL finally served it, so
// the caller can persist the working address (scheme flip / playlist.m3u8
// discovery) instead of re-deriving it on every resume.
func (d *Downloader) fetchPlaylist(ctx context.Context, u string, hdr map[string]string, taskID string, emit func(model.Event)) (string, string, error) {
	b, used, err := d.fetchRetryURL(ctx, u, hdr, "播放列表", taskID, emit)
	if err != nil {
		return "", used, err
	}
	return string(b), used, nil
}

// ---------------------------------------------------------------- crypto

func normalizeKey(raw []byte) ([]byte, error) {
	if len(raw) == 16 {
		return raw, nil
	}
	s := strings.TrimSpace(string(raw))
	if len(s) == 32 {
		if b, err := hex.DecodeString(s); err == nil && len(b) == 16 {
			return b, nil
		}
	}
	return nil, &fatalErr{"HLS 加密 key 格式异常（应为 16 字节）"}
}

func ivFor(st *model.HLSState, seq uint64) []byte {
	if len(st.IV) == 16 {
		return st.IV
	}
	iv := make([]byte, 16)
	binary.BigEndian.PutUint64(iv[8:], seq)
	return iv
}

func decryptAES128CBC(ct, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(ct) == 0 || len(ct)%block.BlockSize() != 0 {
		return nil, &fatalErr{"加密分片长度异常（不是 16 的整数倍）"}
	}
	out := make([]byte, len(ct))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, ct)
	pad := int(out[len(out)-1])
	if pad >= 1 && pad <= 16 && pad <= len(out) {
		out = out[:len(out)-pad]
	}
	return out, nil
}

// ---------------------------------------------------------------- run

func segName(i int) string { return fmt.Sprintf("seg-%08d", i) }

// Run resolves the playlist if needed, downloads pending segments (skipping
// files already on disk = crash-safe resume), merges and produces the final
// media file at t.SavePath (MP4 via ffmpeg when available).
func (d *Downloader) Run(ctx context.Context, t *model.Task, opts *RunOptions) error {
	if opts == nil {
		opts = &RunOptions{}
	}
	emit, persist, save := opts.Emit, opts.Persist, opts.Save
	st := t.HLS
	if st == nil {
		return &fatalErr{"任务缺少 HLS 状态"}
	}
	if d.Client == nil {
		d.Client = http.DefaultClient
	}
	hdr := d.headers(t)

	// --- plan phase: resolve playlists + key once, then persist ---
	if len(st.SegURLs) == 0 {
		// Fetch FIRST, then resolve references against the URL that actually
		// served the manifest: fetchCandidates may have flipped http<->https,
		// discovered the Wowza "<name>.mp4/playlist.m3u8" layout, or completed
		// a bare-host address. Persisting `used` means a resume starts from
		// the working address instead of re-deriving (and re-looping on) it.
		text, used, err := d.fetchPlaylist(ctx, st.MediaURL, hdr, t.ID, emit)
		if err != nil {
			return err
		}
		if used == "" {
			used = st.MediaURL
		}
		st.MediaURL = used
		base, err := url.Parse(used)
		if err != nil {
			return &fatalErr{fmt.Sprintf("无效播放列表地址: %v", err)}
		}
		p, err := parsePlaylist(text, base)
		if err != nil {
			return err
		}
		if p.masterMedia != "" { // master -> best variant media playlist
			text2, used2, err := d.fetchPlaylist(ctx, p.masterMedia, hdr, t.ID, emit)
			if err != nil {
				return err
			}
			if used2 == "" {
				used2 = p.masterMedia
			}
			base2, err := url.Parse(used2)
			if err != nil {
				return &fatalErr{fmt.Sprintf("无效变体地址: %v", err)}
			}
			p2, err := parsePlaylist(text2, base2)
			if err != nil {
				return err
			}
			if p2.masterMedia != "" {
				return &fatalErr{"m3u8 变体嵌套过深"}
			}
			p, st.MediaURL = p2, used2
		}
		st.InitURL, st.KeyURL, st.SegURLs, st.SeqStart, st.IV = p.init, p.key, p.segs, p.seq, p.iv
		if st.KeyURL != "" {
			kb, err := d.fetchRetry(ctx, st.KeyURL, hdr, "加密 key", t.ID, emit)
			if err != nil {
				return err
			}
			key, err := normalizeKey(kb)
			if err != nil {
				return err
			}
			st.Key = key
		}
		st.PartsDir = t.SavePath + ".parts"
		if err := os.MkdirAll(st.PartsDir, 0o755); err != nil {
			return fmt.Errorf("创建分片目录失败: %w", err)
		}
		if save != nil {
			if err := save(); err != nil {
				return fmt.Errorf("保存播放列表计划失败: %w", err)
			}
		}
	}
	if st.PartsDir == "" {
		st.PartsDir = t.SavePath + ".parts"
		if err := os.MkdirAll(st.PartsDir, 0o755); err != nil {
			return err
		}
	}
	if len(st.Key) == 0 && st.KeyURL != "" {
		kb, err := d.fetchRetry(ctx, st.KeyURL, hdr, "加密 key", t.ID, emit)
		if err != nil {
			return err
		}
		key, err := normalizeKey(kb)
		if err != nil {
			return err
		}
		st.Key = key
	}

	initPath := filepath.Join(st.PartsDir, "init")
	segPath := func(i int) string { return filepath.Join(st.PartsDir, segName(i)) }

	// --- resume: completed segments are whatever files survived ---
	done := make([]bool, len(st.SegURLs))
	var doneMu sync.Mutex
	var downloaded atomic.Int64
	if st.InitURL != "" {
		if sz, err := fileSize(initPath); err == nil {
			downloaded.Add(sz)
		}
	}
	pending := make([]int, 0, len(st.SegURLs))
	for i := range st.SegURLs {
		if sz, err := fileSize(segPath(i)); err == nil {
			done[i] = true
			downloaded.Add(sz)
		} else {
			pending = append(pending, i)
		}
	}
	// initial UI mirror (copy; live updates flow through the ticker)
	st.Done = append([]bool{}, done...)

	nconn := t.Conn
	if nconn < 1 {
		nconn = 8
	}
	if nconn > 16 {
		nconn = 16
	}

	// --- parallel fetch with bounded retries ---
	subCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	var errMu sync.Mutex
	var firstErr error
	setErr := func(e error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}

	work := make(chan int)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for i := range work {
			label := fmt.Sprintf("分片 %d/%d", i+1, len(st.SegURLs))
			body, err := d.fetchRetry(subCtx, st.SegURLs[i], hdr, label, t.ID, emit)
			if err != nil {
				if subCtx.Err() == nil && !errorsIsContext(err) {
					setErr(err)
					cancelAll()
				}
				return
			}
			if len(st.Key) > 0 {
				body, err = decryptAES128CBC(body, st.Key, ivFor(st, st.SeqStart+uint64(i)))
				if err != nil {
					setErr(err)
					cancelAll()
					return
				}
			}
			tmp := segPath(i) + ".tmp"
			if err := writeAll(tmp, body, d.bufSize()); err != nil {
				setErr(fmt.Errorf("写入分片失败: %w", err))
				cancelAll()
				return
			}
			if err := os.Rename(tmp, segPath(i)); err != nil { // rename = atomic "segment done"
				setErr(fmt.Errorf("分片落盘失败: %w", err))
				cancelAll()
				return
			}
			doneMu.Lock()
			done[i] = true
			doneMu.Unlock()
			downloaded.Add(int64(len(body)))
		}
	}
	for w := 0; w < nconn; w++ {
		wg.Add(1)
		go worker()
	}
	go func() {
		defer close(work)
		for _, i := range pending {
			select {
			case work <- i:
			case <-subCtx.Done():
				return
			}
		}
	}()

	// init segment (fMP4) downloaded alongside, order fixed at merge time
	if st.InitURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fileSize(initPath); err == nil {
				return
			}
			body, err := d.fetchRetry(subCtx, st.InitURL, hdr, "初始化分片", t.ID, emit)
			if err != nil {
				if subCtx.Err() == nil && !errorsIsContext(err) {
					setErr(err)
					cancelAll()
				}
				return
			}
			if err := writeAll(initPath+".tmp", body, d.bufSize()); err != nil {
				setErr(fmt.Errorf("写入初始化分片失败: %w", err))
				cancelAll()
				return
			}
			if err := os.Rename(initPath+".tmp", initPath); err != nil {
				setErr(fmt.Errorf("初始化分片落盘失败: %w", err))
				cancelAll()
				return
			}
			downloaded.Add(int64(len(body)))
		}()
	}

	// progress ticker: speed + mid-run persist (survives hard kill)
	tickStop := make(chan struct{})
	go func() {
		var last int64
		lastT := time.Now()
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-tickStop:
				return
			case <-tk.C:
				doneMu.Lock()
				st.Done = append([]bool{}, done...)
				doneMu.Unlock()
				cur := downloaded.Load()
				nt := time.Now()
				dur := nt.Sub(lastT).Seconds()
				var speed float64
				if dur > 0 {
					speed = float64(cur-last) / dur
				}
				last, lastT = cur, nt
				if emit != nil {
					emit(model.Event{Type: "progress", TaskID: t.ID, Downloaded: cur, Speed: speed, TS: nt.UnixMilli()})
				}
				if persist != nil {
					persist(cur)
				}
			}
		}
	}()
	wg.Wait()
	close(tickStop)

	if e := firstErr; e != nil {
		return e
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// --- merge ---
	merged := t.SavePath + ".merging"
	f, err := os.Create(merged)
	if err != nil {
		return fmt.Errorf("创建合并文件失败: %w", err)
	}
	w := bufio.NewWriterSize(f, d.bufSize())
	cpbuf := make([]byte, d.bufSize())
	fmp4 := st.InitURL != ""
	if fmp4 {
		if err := appendTo(w, initPath, cpbuf); err != nil {
			f.Close()
			return err
		}
	}
	for i := range st.SegURLs {
		if err := appendTo(w, segPath(i), cpbuf); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("合并写入失败: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("合并写入失败: %w", err)
	}
	_ = os.RemoveAll(st.PartsDir)
	st.PartsDir = ""
	st.SegURLs = nil
	st.Key = nil

	t.Downloaded = downloaded.Load()

	// --- MP4 output: ffmpeg remux (lossless) when available ---
	ff := findFFmpeg()
	if ff != "" {
		if err := remuxToMP4(ff, merged, t.SavePath); err == nil {
			_ = os.Remove(merged)
			if emit != nil {
				emit(model.Event{Type: "info", TaskID: t.ID, Err: "已无损转换为 MP4", TS: time.Now().UnixMilli()})
			}
			if save != nil {
				_ = save()
			}
			return nil
		} else if emit != nil {
			emit(model.Event{Type: "info", TaskID: t.ID, Err: fmt.Sprintf("ffmpeg 转 MP4 失败，保留原始容器：%v", err), TS: time.Now().UnixMilli()})
		}
	}
	if fmp4 {
		// fragmented MP4 concat is itself a playable .mp4
		if err := os.Rename(merged, t.SavePath); err != nil {
			return fmt.Errorf("保存 MP4 失败: %w", err)
		}
		if save != nil {
			_ = save()
		}
		return nil
	}
	// TS container without ffmpeg: keep honest extension instead of lying .mp4
	ext := filepath.Ext(t.SavePath)
	newSave := strings.TrimSuffix(t.SavePath, ext) + ".ts"
	if err := os.Rename(merged, newSave); err != nil {
		return fmt.Errorf("保存 TS 失败: %w", err)
	}
	t.SavePath = newSave
	t.Name = filepath.Base(newSave)
	if save != nil {
		_ = save()
	}
	if emit != nil {
		emit(model.Event{Type: "info", TaskID: t.ID, Err: "未检测到 ffmpeg，已合并为 .ts；安装 ffmpeg（或放 ffmpeg.exe 到程序目录）后新视频将自动转 MP4", TS: time.Now().UnixMilli()})
	}
	return nil
}

// ---------------------------------------------------------------- helpers

func errorsIsContext(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded ||
		strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "context deadline exceeded")
}

func fileSize(p string) (int64, error) {
	fi, err := os.Stat(p)
	if err != nil || fi.IsDir() {
		return 0, fmt.Errorf("missing")
	}
	return fi.Size(), nil
}

func writeAll(p string, b []byte, bufSize int) error {
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, bufSize)
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.Flush()
}

func appendTo(w *bufio.Writer, p string, buf []byte) error {
	f, err := os.Open(p)
	if err != nil {
		return fmt.Errorf("缺少分片 %s: %w", filepath.Base(p), err)
	}
	defer f.Close()
	_, err = w.ReadFrom(f)
	return err
}

func findFFmpeg() string {
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		for _, c := range []string{filepath.Join(dir, "ffmpeg.exe"), filepath.Join(dir, "binaries", "ffmpeg.exe")} {
			if _, err := os.Stat(c); err == nil {
				return c
			}
		}
	}
	for _, name := range []string{"ffmpeg", "ffmpeg.exe"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

func remuxToMP4(ffmpeg, merged, final string) error {
	cmd := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
		"-i", merged, "-c", "copy", "-movflags", "+faststart", final)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%v %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

// BaseNameFromURL derives a sane video filename from the playlist URL.
func BaseNameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "video.mp4"
	}
	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		return "video.mp4"
	}
	if strings.HasSuffix(strings.ToLower(base), ".m3u8") {
		base = base[:len(base)-len(".m3u8")]
	}
	return base + ".mp4"
}
