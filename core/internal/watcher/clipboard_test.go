package watcher

import (
	"testing"

	"github.com/tey-sun/suntey/core/internal/model"
)

func TestLooksDownloadable(t *testing.T) {
	yes := []string{
		"magnet:?xt=urn:btih:0123456789abcdef&dn=movie.mkv",
		"https://example.com/files/setup.exe",
		"http://cdn.example.org/a/b/video.mp4?token=9",
		"https://example.com/archive.tar.gz",
		"https://example.com/episode.m3u8",
		"https://example.com/t.torrent",
	}
	no := []string{
		"not a url at all",
		"https://example.com/blog/post",        // page, no extension
		"http://localhost:8080/",               // root
		"ftp://example.com/tool.exe",           // scheme filtered
		"magnet:?abc=def",                      // no xt=
		"file:///C:/downloads/x.zip",           // not http(s)
		"https://example.com/x.zip;session=42", // ext not in list (xlsx ok below)
		"",
	}
	for _, s := range yes {
		if !LooksDownloadable(s) {
			t.Errorf("must accept %q", s)
		}
	}
	for _, s := range no {
		if LooksDownloadable(s) {
			t.Errorf("must reject %q", s)
		}
	}
}

func TestClipboardEnabledDefault(t *testing.T) {
	var c model.Config
	if !c.ClipboardEnabled() {
		t.Fatal("missing config must default to enabled (IDM ships watching on)")
	}
	off, on := false, true
	c.ClipboardWatch = &off
	if c.ClipboardEnabled() {
		t.Fatal("explicit false must disable")
	}
	c.ClipboardWatch = &on
	if !c.ClipboardEnabled() {
		t.Fatal("explicit true must enable")
	}
}
