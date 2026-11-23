package version

// Version is the core version. Bump at release.
//
// 0.2.0 ships the pieces users kept asking for: BitTorrent/magnet downloads,
// HLS (m3u8) video downloads with MP4 remux, mid-run crash-safe resume,
// progress-preserving connection-count changes and bounded, non-looping retry.
const Version = "0.2.0"

// Build is stamped at build time via -ldflags "-X .../version.Build=..." so the
// UI can prove which binary is actually running (users could not tell an old
// build from a new one, which made "我打开 exe 没变化" undiagnosable).
var Build = "dev"

// Features lists the engine capabilities for the About/Settings panel.
var Features = []string{
	"HTTP/HTTPS 多线程分段下载 + 断点续传",
	"BT / 磁力链接下载（DHT、Tracker、PEX、做种）",
	"HLS(m3u8) 视频下载 + MP4 无损转封装",
	"浏览器扩展：右键下载、视频/音频/图片嗅探、m3u8 抓取",
}
