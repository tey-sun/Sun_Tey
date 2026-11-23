package httpx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"golang.org/x/net/html"

	"github.com/tey-sun/suntey/core/internal/model"
)

// PageExts are the file extensions that turn an HTML page URL into a real
// download candidate (the M6 resolver: paste-a-webpage → pick the link).
var PageExts = map[string]bool{
	".zip": true, ".rar": true, ".7z": true, ".gz": true, ".bz2": true, ".xz": true, ".tar": true,
	".iso": true, ".exe": true, ".msi": true, ".apk": true, ".dmg": true, ".img": true, ".msix": true,
	".mp4": true, ".mkv": true, ".avi": true, ".mov": true, ".webm": true, ".wmv": true, ".flv": true, ".ts": true,
	".m3u8": true, ".m4v": true,
	".mp3": true, ".m4a": true, ".flac": true, ".wav": true, ".ogg": true,
	".pdf": true, ".epub": true, ".doc": true, ".docx": true, ".xls": true, ".xlsx": true, ".ppt": true, ".pptx": true,
}

func IsDownloadExt(name string) bool {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	// "name.zip?v=2" style: strip query/fragment before extension check.
	if j := strings.IndexByte(name, '?'); j >= 0 {
		name = name[:j]
	}
	if j := strings.IndexByte(name, '#'); j >= 0 {
		name = name[:j]
	}
	return PageExts[strings.ToLower(knownExt(name))]
}

// PageCandidate is one downloadable link discovered inside an HTML page.
type PageCandidate struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Size int64  `json:"size,omitempty"` // probed size, 0 = not probed
}

// maxPageProbes caps how many candidates get a live HEAD probe (page parsing
// must stay under ~a second even on slow linkservers).
const maxPageProbes = 4

// ParsePage fetchs an HTML page and returns downloadable links, document
// order, deduped, extension-filtered. The caller decides what to do with 0/1/N
// candidates (the M6 resolver flow).
func ParsePage(ctx context.Context, c *http.Client, r model.Request, ua string) ([]PageCandidate, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	applyHeader(req, r, ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("page: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("page: HTTP %d %s", resp.StatusCode, resp.Status)
	}
	base := resp.Request.URL // after redirects
	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	if rerr != nil {
		return nil, fmt.Errorf("page: read: %w", rerr)
	}
	doc, perr := html.Parse(bytes.NewReader(raw))
	if perr != nil {
		return nil, fmt.Errorf("page: parse: %w", perr)
	}
	var out []PageCandidate
	seen := map[string]bool{}
	addCand := func(candURL string, fromDOM bool) {
		ref, e := url.Parse(candURL)
		if e != nil {
			return
		}
		if ref.Scheme == "" && fromDOM {
			abs := base.ResolveReference(ref)
			if abs.Scheme != "http" && abs.Scheme != "https" {
				return
			}
			ref = abs
		}
		if ref.Scheme != "http" && ref.Scheme != "https" || !IsDownloadExt(ref.Path) {
			return
		}
		key := strings.Join([]string{base.Host, ref.Host, ref.Path, ref.RawQuery}, "\x00")
		if seen[key] {
			return
		}
		seen[key] = true
		name := strings.TrimSuffix(baseNameFromURL(ref.String()), knownExt(ref.Path))
		if name == "" {
			name = "download"
		}
		out = append(out, PageCandidate{Name: name, URL: ref.String()})
	}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "a", "source", "video", "audio":
				for _, a := range n.Attr {
					if a.Val == "" || (a.Key != "href" && a.Key != "src") {
						continue
					}
					addCand(a.Val, true)
				}
			case "meta":
				// Open Graph media (og:video / og:audio and their :url /
				// :secure_url variants): the standard way pages advertise
				// their media file to downloaders.
				var prop, cont string
				for _, a := range n.Attr {
					switch strings.ToLower(a.Key) {
					case "property", "itemprop", "name":
						prop = a.Val
					case "content":
						cont = a.Val
					}
				}
				if strings.HasPrefix(prop, "og:") && (strings.Contains(prop, "video") || strings.Contains(prop, "audio")) && cont != "" {
					addCand(cont, false)
				}
			}
		}
		for ch := n.FirstChild; ch != nil; ch = ch.NextSibling {
			walk(ch)
		}
	}
	walk(doc)
	// Embedded links (SPA shells: douyin / bilibili keep the real media URL
	// in JS data, not anchors). Scan the raw bytes after normalising the
	// JSON `\/` escape; DOM hits keep priority (document order first).
	for _, u := range embeddedMediaURLs(raw) {
		addCand(u, false)
	}
	return out, nil
}

// embeddedExtRE matches absolute media/archive URLs inside page scripts and
// JSON payloads. Deliberately excludes quotes, angle brackets, parens and
// backslashes so JSON string boundaries terminate the match.
var embeddedExtRE = regexp.MustCompile(`(?i)https?://[A-Za-z0-9._~:/?#@!$&'*+,;=%-]+\.(?:mp4|m4v|mkv|webm|mov|avi|flv|m3u8|ts|mp3|m4a|zip|exe|iso|pdf|apk|dmg)(?:[?#][^\s"'<>\\]*)?`)

func embeddedMediaURLs(raw []byte) []string {
	s := strings.ReplaceAll(string(raw), `\/`, `/`)
	return embeddedExtRE.FindAllString(s, 12)
}

// probed pairs a page candidate with its successful probe result.
type probed struct {
	cand PageCandidate
	res  *model.ResolveResult
}

// ResolvePage picks the best candidate for an HTML page the user pasted
// directly (迅雷 batch-download lite): probe up to maxPageProbes candidates,
// prefer the first one whose name matches the URL tail or whose filename
// contains the page slug; otherwise take the first probed candidate.
func ResolvePage(ctx context.Context, c *http.Client, r model.Request, ua string, probe func(context.Context, *http.Client, model.Request, string) (*model.ResolveResult, error)) (*model.ResolveResult, error) {
	cands, err := ParsePage(ctx, c, r, ua)
	if err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("解析页面未找到可下载链接")
	}
	pageSlug := strings.TrimSuffix(knownExt(baseNameFromURL(r.URL)), ".html")
	var okList []probed
	for i := range cands {
		if len(okList) >= maxPageProbes {
			break
		}
		pr, e := probe(ctx, c, model.Request{URL: cands[i].URL}, ua)
		if e == nil && pr != nil {
			okList = append(okList, probed{cands[i], pr})
		}
	}
	if len(okList) == 0 {
		return nil, fmt.Errorf("解析页面失败：候选链接均无法访问")
	}
	best := okList[0]
	for _, p := range okList {
		n := strings.ToLower(p.cand.Name)
		if strings.Contains(n, strings.ToLower(pageSlug)) || n == strings.ToLower(strings.TrimSuffix(baseNameFromURL(r.URL), knownExt(r.URL))) {
			best = p
			break
		}
	}
	best.res.Name = firstNonEmpty(best.res.Name, best.cand.Name)
	best.res.ProbedURL = best.cand.URL // the real file link behind the page
	best.res.Candidates = pageCandidates(okList)
	// The result was parsed out of an HTML page: keep the page flag so the UI
	// picker still knows to offer the candidate list (the zip probe alone
	// would say "plain file" and hide the M6.5 modal).
	best.res.IsPage = true
	return best.res, nil
}

// pageCandidates converts a probed list into UI-facing candidates
// (迅雷-style picker; the core itself already chose `best`).
func pageCandidates(okList []probed) []model.Candidate {
	out := make([]model.Candidate, 0, len(okList))
	for _, p := range okList {
		out = append(out, model.Candidate{
			URL:  p.cand.URL,
			Name: firstNonEmpty(p.res.Name, p.cand.Name),
			Size: p.res.Size,
		})
	}
	return out
}

// knownExt returns the lowercased extension including the dot (".zip"), or "".
func knownExt(name string) string {
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		return strings.ToLower(name[i:])
	}
	return ""
}

// baseNameFromURL is the path segment name of a URL ("." and "/" become "").
func baseNameFromURL(rawurl string) string {
	name := ""
	if u, err := url.Parse(rawurl); err == nil {
		name = path.Base(u.Path)
	} else {
		name = path.Base(rawurl)
	}
	if name == "." || name == "/" {
		return ""
	}
	return name
}

// ResolvePageHeaders is the anti-leech retry of ResolvePage: candidate probes
// carry the browser-forwarded headers (cookies/referer from the page visit),
// which beat a bare derived-Referer probe on hosts that 403 fresh visitors.
func ResolvePageHeaders(ctx context.Context, c *http.Client, r model.Request, ua string,
	probe func(context.Context, *http.Client, model.Request, string) (*model.ResolveResult, error),
	headers map[string]string) (*model.ResolveResult, error) {
	cands, err := ParsePage(ctx, c, r, ua)
	if err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("解析页面未找到可下载链接")
	}
	pageSlug := strings.ToLower(strings.TrimSuffix(knownExt(baseNameFromURL(r.URL)), ".html"))
	var okList []probed
	for i := range cands {
		if len(okList) >= maxPageProbes {
			break
		}
		cr := model.Request{URL: cands[i].URL, Header: headers}
		if pr, e := probe(ctx, c, cr, ua); e == nil && pr != nil {
			okList = append(okList, probed{cands[i], pr})
		}
	}
	if len(okList) == 0 {
		return nil, fmt.Errorf("解析页面失败：候选链接均无法访问")
	}
	best := okList[0]
	for _, p := range okList {
		if strings.Contains(strings.ToLower(p.cand.Name), pageSlug) {
			best = p
			break
		}
	}
	best.res.Name = firstNonEmpty(best.res.Name, best.cand.Name)
	best.res.ProbedURL = best.cand.URL
	best.res.Candidates = pageCandidates(okList)
	return best.res, nil
}
