package model

import "testing"

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"s501740.s1q2105.com/hls/abc/index_1920x1080.m3u8", "https://s501740.s1q2105.com/hls/abc/index_1920x1080.m3u8"}, // user paste, no scheme
		{"//cdn.example.com/a.zip", "https://cdn.example.com/a.zip"},                                                     // protocol-relative
		{"https://example.com/a.zip", "https://example.com/a.zip"},
		{"http://localhost:8080/x", "http://localhost:8080/x"},
		{" localhost:8080/x ", "https://localhost:8080/x"}, // bare host:port paste gets prefixed
		{"magnet:?xt=urn:btih:abc&dn=x", "magnet:?xt=urn:btih:abc&dn=x"},
		{"ed2k://|file|a.iso|123|HASH|/", "ed2k://|file|a.iso|123|HASH|/"},
		{"file:///C:/d/x.torrent", "file:///C:/d/x.torrent"},
		{"ftp://host/a", "ftp://host/a"},
		{"", ""},
		{"1.2.3.4:8080/x.zip", "https://1.2.3.4:8080/x.zip"}, // numeric scheme head must NOT be treated as scheme
		{"E:\\Temp\\x.torrent", "E:\\Temp\\x.torrent"},       // windows absolute path paste (a local .torrent)
		{"C:/Downloads/a.bin", "C:/Downloads/a.bin"},         // forward-slash drive path
		{"\\\\srv\\share\\x.mp4", "\\\\srv\\share\\x.mp4"},   // UNC path
		{"./rel/x.torrent", "./rel/x.torrent"},               // explicit relative
	}
	for _, c := range cases {
		if got := NormalizeURL(c.in); got != c.want {
			t.Errorf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
