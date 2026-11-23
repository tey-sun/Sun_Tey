package watcher

import (
	"context"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/atotto/clipboard"

	"github.com/tey-sun/suntey/core/internal/download"
	"github.com/tey-sun/suntey/core/internal/model"
)

// Clipboard mirrors IDM/迅雷's clipboard monitor: when a downloadable link
// lands on the clipboard, a task is created and started automatically.
// Intentionally heuristic — plain web-page URLs without a recognizable
// extension are ignored so everyday copy/paste traffic does not spam the
// task list, while magnet links always count.
//
// It reads the clipboard on a timer (no Windows message hook), so the cost
// is a few microseconds per poll; when the switch is off it does nothing at
// all beyond the ticker.
func Clipboard(ctx context.Context, mgr *download.Manager) {
	// Treat whatever is already on the clipboard as seen: Sun_Tey should not
	// resurrect a link copied long before it started.
	last, _ := clipboard.ReadAll()

	tick := time.NewTicker(1500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if !mgr.Cfg.ClipboardEnabled() {
				continue
			}
			s, err := clipboard.ReadAll()
			if err != nil {
				continue // headless/no-display hosts: stay quiet
			}
			s = strings.TrimSpace(s)
			if s == "" || len(s) > 4096 || s == last {
				if s != last {
					last = s
				}
				continue
			}
			last = s
			if !LooksDownloadable(s) {
				continue
			}
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			t, err := mgr.Create(c, model.Request{URL: s})
			cancel()
			if err == nil && t != nil {
				_ = mgr.Start(t.ID) // failure here is surfaced by SSE/task state
			}
			// Create errors (offline resolve, duplicate name races) stay quiet:
			// the user did not ask for this file explicitly.
		}
	}
}

// LooksDownloadable is the clipboard heuristic: magnets pass; http(s) links
// pass only when the path looks like a file we would actually fetch.
func LooksDownloadable(s string) bool {
	if strings.HasPrefix(strings.ToLower(s), "magnet:") {
		return strings.Contains(s, "xt=")
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return false
	}
	ext := strings.ToLower(path.Ext(u.Path))
	if ext == "" {
		return false
	}
	return downloadableExt[ext]
}

var downloadableExt = map[string]bool{
	".zip": true, ".rar": true, ".7z": true, ".tar": true, ".gz": true, ".bz2": true, ".xz": true,
	".exe": true, ".msi": true, ".apk": true, ".iso": true, ".img": true, ".dmg": true,
	".mp4": true, ".mkv": true, ".mov": true, ".avi": true, ".webm": true, ".ts": true, ".m3u8": true,
	".mp3": true, ".m4a": true, ".flac": true, ".wav": true, ".epub": true, ".pdf": true,
	".docx": true, ".xlsx": true, ".pptx": true, ".torrent": true,
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true,
}
