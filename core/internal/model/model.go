package model

import "time"

// TaskStatus is the lifecycle state of a download task.
type TaskStatus int

const (
	StatusUnknown TaskStatus = iota
	StatusResolving
	StatusReady // resolved, not started
	StatusRunning
	StatusPaused
	StatusComplete
	StatusError
)

func (s TaskStatus) String() string {
	switch s {
	case StatusResolving:
		return "resolving"
	case StatusReady:
		return "ready"
	case StatusRunning:
		return "running"
	case StatusPaused:
		return "paused"
	case StatusComplete:
		return "complete"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

// Request is a single download request: a URL plus the headers that must be
// replayed on every chunk request (anti-leech / auth: Referer, Cookie, UA...).
type Request struct {
	URL      string            `json:"url"`
	Header   map[string]string `json:"header,omitempty"`
	Filename string            `json:"filename,omitempty"`
	// Conn overrides the configured connections per task for THIS task
	// (0 = use the setting). Throttling anti-leech CDNs punish many parallel
	// connections, so the advice shown on a throttling failure ("在设置中减少连
	// 接数") needs a per-task escape hatch too, e.g. {"conn": 2} from the
	// extension, the CLI or a retry after a slow-start failure.
	Conn int `json:"conn,omitempty"`
	// Referer rides as its own field (anti-leech): when the M6 page resolver
	// swaps a webpage URL for a direct file link, the page stays as the
	// Referer on every chunk request and every restart probe.
	Referer string `json:"referer,omitempty"`
}

// FileEntry is one file inside a (possibly multi-file) resource.
type FileEntry struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// ResolveResult is the outcome of probing a resource: name, size, chunkability.
type ResolveResult struct {
	Name         string `json:"name"`
	Size         int64  `json:"size"`         // -1 if unknown
	SupportRange bool   `json:"supportRange"` // server honors HTTP Range
	Accepts      bool   `json:"accepts"`
	// IsPage: the URL is an HTML page, not a file (small text/html body).
	// The M6 resolver parses <a>/<source> links and resolves the best one;
	// ProbedURL then records the real file link that was chosen.
	IsPage    bool   `json:"isPage,omitempty"`
	ProbedURL string `json:"probedUrl,omitempty"`
	// Candidates: every probed page link (M6.5) — the core already picked the
	// best one; the UI may offer the user a picker and re-create with another.
	Candidates []Candidate `json:"candidates,omitempty"`
	Files      []FileEntry `json:"files,omitempty"`
}

// Candidate is one probed page link for the UI candidate picker.
type Candidate struct {
	URL  string `json:"url"`
	Name string `json:"name"`
	Size int64  `json:"size"` // -1 if unknown
}

// ChunkState is the persisted progress of one byte range (for resume).
type ChunkState struct {
	Index      int   `json:"index"`
	Start      int64 `json:"start"`
	End        int64 `json:"end"` // inclusive
	Downloaded int64 `json:"downloaded"`
}

// HLSState is the persisted state of an HLS (m3u8) video task.
// Resume model: every finished segment is a file in PartsDir; after a hard
// kill the completed set is rebuilt from the filesystem, no bitmap needed.
type HLSState struct {
	MediaURL string   `json:"mediaUrl"`
	InitURL  string   `json:"initUrl,omitempty"` // fMP4 EXT-X-MAP init segment
	KeyURL   string   `json:"keyUrl,omitempty"`  // AES-128 key URI
	Key      []byte   `json:"key,omitempty"`     // fetched 16-byte key (base64 in JSON)
	IV       []byte   `json:"iv,omitempty"`      // explicit IV (nil = sequence IV)
	SegURLs  []string `json:"segUrls,omitempty"`
	SeqStart uint64   `json:"seqStart,omitempty"`
	PartsDir string   `json:"partsDir,omitempty"`
	Done     []bool   `json:"done,omitempty"` // UI progress mirror
}

// Task is a managed download. Persisted in bbolt.
type Task struct {
	ID           string       `json:"id"`
	Req          Request      `json:"req"`
	Status       TaskStatus   `json:"status"`
	Name         string       `json:"name"`
	SavePath     string       `json:"savePath"`
	Size         int64        `json:"size"`
	SupportRange bool         `json:"supportRange"`
	Conn         int          `json:"conn"`            // parallel connections
	Files        []FileEntry  `json:"files,omitempty"` // BT: file list inside the torrent
	Downloaded   int64        `json:"downloaded"`
	Speed        float64      `json:"speed"` // bytes/sec, rolling
	Chunks       []ChunkState `json:"chunks"`
	HLS          *HLSState    `json:"hls,omitempty"`
	Err          string       `json:"err,omitempty"`
	// ProbedURL: when a pasted webpage was resolved to a direct file link,
	// this records which link was chosen (Req.URL already points at it;
	// restarts re-probe the file, never re-parse the page).
	ProbedURL string `json:"probedUrl,omitempty"`
	// BTSel is the bttx file-selection ("0,2,3" = selected file indices into
	// Files); empty means download every file. BitTorrent tasks only.
	BTSel     string    `json:"btSel,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Event is emitted on the progress bus (streamed to the UI / CLI).
type Event struct {
	Type       string  `json:"type"` // progress | status | done | error | retry | stalled | downgrade
	TaskID     string  `json:"taskId"`
	Status     string  `json:"status,omitempty"`
	Downloaded int64   `json:"downloaded,omitempty"`
	Size       int64   `json:"size,omitempty"`
	Speed      float64 `json:"speed,omitempty"`
	Err        string  `json:"err,omitempty"`
	// Attempt + Level drive the UI's retry log: a 1st/2nd quick retry of a
	// single chunk is normal noise on the public internet (Level "info" ->
	// grey), a repeated failure is worth attention (Level "warn" -> yellow),
	// and the final give-up is Level "error" -> red. Without Level the panel
	// used to paint every transient retry red, which read as "下载一直循环".
	Attempt int    `json:"attempt,omitempty"`
	Level   string `json:"level,omitempty"` // info | warn | error
	// BT fields (bttx engine): peer/seed visibility and upload share.
	Peers    int    `json:"peers,omitempty"`
	Seeds    int    `json:"seeds,omitempty"`
	Uploaded int64  `json:"uploaded,omitempty"`
	Name     string `json:"name,omitempty"`
	TS       int64  `json:"ts"`
}

// Config is the persisted downloader configuration.
type Config struct {
	DownloadDir   string `json:"downloadDir"`
	MaxRunning    int    `json:"maxRunning"`
	DefaultConn   int    `json:"defaultConn"`
	WriteBufferKB int    `json:"writeBufferKB,omitempty"` // per-connection disk write buffer
	UserAgent     string `json:"userAgent"`
	// DefaultUA is the shipped browser-like UA (GET /config fills it; the UI
	// prefills the settings field so an empty UA box shows a visible value).
	DefaultUA string `json:"defaultUserAgent,omitempty"`
	// RateLimitKBps is the IDM-style global download cap (all HTTP/HLS legs
	// share one token bucket). nil = unchanged on PUT; 0 = unlimited.
	RateLimitKBps *int64 `json:"rateLimitKBps,omitempty"`
	// ClipboardWatch toggles the clipboard monitor (magnet/file links land
	// as tasks). nil = enabled for existing configs.
	ClipboardWatch *bool `json:"clipboardWatch,omitempty"`
	// BitTorrent (bttx engine). Zero values fall back to sane defaults.
	BTPort      int      `json:"btPort,omitempty"`      // listen port (0 = random)
	BTSeedAfter *bool    `json:"btSeedAfter,omitempty"` // keep seeding when done (default true)
	BTNoUpload  *bool    `json:"btNoUpload,omitempty"`  // pure downloader mode
	BTTrackers  []string `json:"btTrackers,omitempty"`  // extra trackers for every task
	BTPeers     []string `json:"btPeers,omitempty"`     // pinned "host:port" peers
	// APIToken is the stable loopback token (generated once, persisted),
	// so browser extensions keep working across restarts.
	APIToken string `json:"apiToken"`
}

// ClipboardEnabled reports whether the clipboard monitor should run.
// Missing config means enabled (IDM ships with clipboard watching on).
func (c Config) ClipboardEnabled() bool {
	return c.ClipboardWatch == nil || *c.ClipboardWatch
}
