package model

import "strings"

// NormalizeURL makes pasted or sniffed urls robust the way IDM/迅雷 accept
// them: a bare host ("example.com/a.zip") gets https://, and a
// protocol-relative CDN url ("//cdn.example.com/a.zip") gets the scheme back.
// Magnet / ed2k / file and any proper scheme pass through untouched.
func NormalizeURL(u string) string {
	t := strings.TrimSpace(u)
	if t == "" {
		return t
	}
	low := strings.ToLower(t)
	if strings.HasPrefix(low, "magnet:") || strings.HasPrefix(low, "ed2k:") {
		return t
	}
	if strings.HasPrefix(t, "//") {
		return "https:" + t
	}
	if looksLikeLocalPath(t) {
		return t // Windows/POSIX file paths (a pasted .torrent) must survive
		// unprefixed: prefixing "https://E:\x.torrent" parses as host "E"
		// with an invalid port and kills the BT task at add time.
	}
	if i := strings.Index(t, "://"); i > 0 && hasSchemePrefix(t[:i]) {
		return t // proper scheme already (http(s), ftp, file, torrent…)
	}
	return "https://" + t
}

// looksLikeLocalPath keeps filesystem paths out of the https:// fallback:
// anything with a backslash, a leading slash or dot-slash, or a drive-letter
// colon ("E:...", "c:/...") is a path, not a bare host:port URL.
func looksLikeLocalPath(t string) bool {
	if strings.ContainsRune(t, '\\') {
		return true
	}
	if strings.HasPrefix(t, "/") || strings.HasPrefix(t, "./") || strings.HasPrefix(t, "../") {
		return true
	}
	if len(t) >= 2 && isAlpha(t[0]) && t[1] == ':' {
		return true
	}
	return false
}

// hasSchemePrefix reports whether s is a valid RFC 3986 scheme head
// (alpha then alpha/digit/+-.) — guards against "1.2.3.4:8080/x" style hosts
// where the colon sits before a dot-led number.
func hasSchemePrefix(s string) bool {
	if s == "" || !isAlpha(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if isAlpha(c) || isDigit(c) || c == '+' || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
