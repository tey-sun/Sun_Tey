// Package bttx implements BitTorrent downloads (magnet + .torrent) on top of
// anacrolix/torrent, in the same Run(ctx, task, opts) shape as the httpx/hlsx
// engines, so the Manager dispatches every task type uniformly.
//
// Design:
//   - ONE client for the whole app: a single listen port, one DHT node, PEX,
//     tracker redundancy shared by every BT task.
//   - Resume: data is written in its final layout (library part-files off) and
//     every piece is SHA1-verified when the torrent is re-added, so a killed
//     process resumes by requesting only the missing pieces.
//   - Magnet: metadata arrives from peers over ut_metadata; name/size/file list
//     are filled in then and persisted via opts. A swarm with no metadata is
//     reported as an actionable error after a bounded timeout — not left
//     spinning in 下载中.
package bttx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/generics"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	torrenttypes "github.com/anacrolix/torrent/types"

	"github.com/tey-sun/suntey/core/internal/model"
)

// Config tunes the shared BitTorrent client.
type Config struct {
	ListenPort      int      // 0 = random free port
	MaxConns        int      // established connections per task
	SeedAfter       bool     // keep sharing after completion
	BaseDir         string   // storage root for torrents with no registered dir
	Trackers        []string // appended to every task (public bootstrap list)
	ExtraPeers      []string // "host:port" pinned peers (private swarms, LAN, tests)
	NoDHT           bool
	DisablePEX      bool
	NoUpload        bool
	Debug           bool          // pass through to anacrolix cfg.Debug (enables "updating requests" debug logs etc.)
	MetadataTimeout time.Duration // magnet metadata phase bound
}

// RunOptions mirrors the other engines' callbacks.
type RunOptions struct {
	Emit    func(model.Event)
	Persist func(downloaded int64)
	// OnMeta fires once metadata is known (immediately for .torrent, after the
	// ut_metadata exchange for magnets) and must persist the task's
	// name/size/files/save path before returning nil.
	OnMeta func(name string, size int64, files []model.FileEntry, savePath string) error
	// OnLive fires right after the live registry entry exists (so a test or
	// the manager can be sure SetFileSelection will find the torrent) and
	// before metadata waits begin. Optional.
	OnLive func()
}

// Engine owns the process-wide BitTorrent client.
type Engine struct {
	cfg   Config
	mu    sync.Mutex
	cl    *torrent.Client
	st    storage.ClientImplCloser
	dirs  map[string]string           // infohash hex -> destination dir
	tasks map[string]*torrent.Torrent // task id -> live torrent (file selection)
	sels  map[string]*liveSelection   // task id -> 运行期勾选（改选后完成目标必须跟着变）
	port  int
}

// New builds the engine. The client starts on first use (lazy port bind).
func New(cfg Config) *Engine {
	if cfg.MetadataTimeout <= 0 {
		cfg.MetadataTimeout = 4 * time.Minute
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 32
	}
	return &Engine{cfg: cfg, dirs: map[string]string{}, tasks: map[string]*torrent.Torrent{}, sels: map[string]*liveSelection{}}
}

// --- file selection (迅雷/IDM "下载详情" 勾选) -----------------------------

// SelectionKey is the task.Extra key holding the selected file indices as
// "0,2,3". Empty string means "everything" (the default after DownloadAll).
const SelectionKey = "btSel"

// ParseSelection decodes a SelectionKey value; nil means "all files".
func ParseSelection(s string) map[int]bool {
	if s == "" {
		return nil
	}
	out := map[int]bool{}
	for _, p := range strings.Split(s, ",") {
		var i int
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%d", &i); err == nil {
			out[i] = true
		}
	}
	return out
}

// JoinSelection encodes indices; an empty slice returns "" meaning "all".
func JoinSelection(keep []int) string {
	cp := make([]int, 0, len(keep))
	for _, v := range keep {
		if v >= 0 {
			cp = append(cp, v)
		}
	}
	if len(cp) == 0 {
		return ""
	}
	sort.Ints(cp)
	var b strings.Builder
	prev := -1
	for _, v := range cp {
		if v == prev {
			continue
		}
		if prev >= 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", v)
		prev = v
	}
	return b.String()
}

// SelectedSize sums the lengths of the selected files among files.
func SelectedSize(files []model.FileEntry, sel map[int]bool) int64 {
	if sel == nil {
		var total int64
		for _, f := range files {
			total += f.Size
		}
		return total
	}
	var total int64
	for i := range files {
		if sel[i] {
			total += files[i].Size
		}
	}
	return total
}

// applySelection maps a file selection onto piece interest. v1.61 semantics
// (piece.go purePriority): a piece is wanted iff max(file.prio, piece.prio,
// reader hints) > None. Two knobs matter: file priorities only ever raise
// (DownloadAll-style raises stick), while CancelPieces/DownloadPieces lower
// and raise the piece-level priority, and the pending set is recomputed from
// effectivePriority — a piece re-lifted after CancelPieces becomes wanted
// again. So: files start unraised (never DownloadAll), selected files lift
// their pieces, deselected pieces drop. Shared boundary pieces stay wanted
// if any wanted file covers them.
func applySelection(t *torrent.Torrent, sel map[int]bool) {
	fs := t.Files()
	for i := range fs {
		if sel == nil || sel[i] {
			fs[i].SetPriority(torrenttypes.PiecePriorityNormal)
		} else {
			fs[i].SetPriority(torrenttypes.PiecePriorityNone)
		}
	}
}

// selectedProgress returns completed/total bytes restricted to the selection
// (so a partial download can actually reach "complete").
func selectedProgress(t *torrent.Torrent, sel map[int]bool) (comp, total int64) {
	fs := t.Files()
	if sel == nil {
		return t.BytesCompleted(), t.Length()
	}
	for i := range fs {
		if sel[i] {
			comp += fs[i].BytesCompleted()
			total += fs[i].Length()
		}
	}
	return comp, total
}

// liveSelection is the live file selection of a running task. SetFileSelection
// mutates it and Run re-reads it every tick, so changing the checked files
// mid-download moves the finish line instead of staying frozen at whatever the
// task read when it started (a re-selection that shrinks the set must be able
// to complete right away, and one that grows it must not complete early).
type liveSelection struct {
	mu  sync.Mutex
	sel map[int]bool // nil means "all files"
	ver int64
}

func (l *liveSelection) set(sel map[int]bool) {
	l.mu.Lock()
	l.sel, l.ver = sel, l.ver+1
	l.mu.Unlock()
}

func (l *liveSelection) get() (map[int]bool, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sel, l.ver
}

// registerTask/clearTask keep the task id -> torrent registry consistent even
// if a later run re-registers the same id (restart resume). Registering also
// publishes the live selection that SetFileSelection will rewrite.
func (e *Engine) registerTask(id string, t *torrent.Torrent, sel map[int]bool) *liveSelection {
	live := &liveSelection{sel: sel}
	e.mu.Lock()
	e.tasks[id] = t
	e.sels[id] = live
	e.mu.Unlock()
	return live
}

func (e *Engine) clearTask(id string, t *torrent.Torrent) {
	e.mu.Lock()
	if e.tasks[id] == t {
		delete(e.tasks, id)
		delete(e.sels, id)
	}
	e.mu.Unlock()
}

// ErrBTNotRunning is returned when a selection request targets a task whose
// torrent is not live (not started / finished); the caller still persists the
// selection and it applies on next run.
var ErrBTNotRunning = errors.New("BT 任务当前未运行，选择已保存，下次开始生效")

// ErrBTNoMeta is returned for a live torrent whose metadata has not arrived.
var ErrBTNoMeta = errors.New("BT 元数据未就绪，无法按文件勾选")

// ErrBTEmptySelection is returned when a live selection request carries no
// files at all (start-time empty BTSel still means "everything" and is never
// routed here).
var ErrBTEmptySelection = errors.New("BT 至少勾选一个文件")

// SetFileSelection applies a file selection to the live torrent if there is
// one; persistence of the selection into task.Extra is the caller's job so a
// restart resumes with the same set.
func (e *Engine) SetFileSelection(taskID string, keep []int) error {
	e.mu.Lock()
	t, live := e.tasks[taskID], e.sels[taskID]
	e.mu.Unlock()
	if t == nil {
		return ErrBTNotRunning
	}
	if t.Info() == nil {
		return ErrBTNoMeta
	}
	if len(keep) == 0 {
		return ErrBTEmptySelection
	}
	// v1.61 want pipeline (piece.go purePriority): a piece is wanted iff
	// max(file.prio, piece.prio, reader hints) > None. CancelPieces lowers the
	// piece-level priority and drops pending interest — file priorities alone
	// can never re-add those pieces, so the live path must re-lift them with
	// DownloadPieces (raises piece priority to Normal) on re-selection.
	// File priorities stay authoritative for the initial run: files start at
	// PiecePriorityNone (initFiles) and we only ever raise the selected ones,
	// so no DownloadAll-style raise is left to stick.
	sel := ParseSelection(JoinSelection(keep))
	applySelection(t, sel)
	fs := t.Files()
	for i := range fs {
		if sel == nil || sel[i] {
			t.DownloadPieces(fs[i].BeginPieceIndex(), fs[i].EndPieceIndex())
		} else {
			t.CancelPieces(fs[i].BeginPieceIndex(), fs[i].EndPieceIndex())
		}
	}
	// Publish last: Run must never observe the new finish line before the
	// pieces backing it were re-lifted.
	if live != nil {
		live.set(sel)
	}
	return nil
}

// FileList returns the live (or metainfo-level) file list with the selection
// applied, for UI detail dialogs.
type FileItem struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Selected bool   `json:"selected"`
}

func FileItems(files []model.FileEntry, sel map[int]bool) []FileItem {
	out := make([]FileItem, 0, len(files))
	for i, f := range files {
		out = append(out, FileItem{Index: i, Name: f.Name, Size: f.Size,
			Selected: sel == nil || sel[i]})
	}
	return out
}

// RegisterDir tells the engine where the files of infohash `hex` belong.
func (e *Engine) RegisterDir(hexHash, dir string) {
	e.mu.Lock()
	e.dirs[hexHash] = dir
	e.mu.Unlock()
}

func (e *Engine) dirFor(hexHash string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dirs[hexHash]
}

// DefaultTrackers is the public bootstrap list appended to every task so a
// tracker-only magnet gets a fighting chance (deduped per tier).
func DefaultTrackers() []string {
	return []string{
		"udp://tracker.opentrackr.org:1337/announce",
		"udp://open.stealth.si:80/announce",
		"udp://tracker.torrent.eu.org:451/announce",
		"udp://exodus.desync.com:6969/announce",
		"udp://tracker.tiny-vps.com:6969/announce",
		"udp://tracker.darkness.services:6969/announce",
		"udp://tracker.dler.org:6969/announce",
		"udp://open.demonii.com:1337/announce",
	}
}

// partFilesOff disables the library's ".part file" staging so bytes go straight
// into the final layout (what users see in Explorer mid-download).
func partFilesOff() generics.Option[bool] {
	var o generics.Option[bool]
	o.Set(false)
	return o
}

// client lazily starts the shared client (binds the listen port once).
func (e *Engine) client() (*torrent.Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cl != nil {
		return e.cl, nil
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.ListenPort = e.cfg.ListenPort
	cfg.NoDHT = e.cfg.NoDHT
	cfg.DisablePEX = e.cfg.DisablePEX
	cfg.NoUpload = e.cfg.NoUpload
	cfg.Seed = e.cfg.SeedAfter
	cfg.DisableIPv6 = true
	cfg.NoDefaultPortForwarding = true // no UPnP noise from a desktop app
	cfg.DisableWebtorrent = true
	// HTTP/FTP webseeds (url-list in official .torrent files) are permanent
	// CDN seed sources: an Ubuntu/Debian ISO downloads at full speed even when its
	// public swarm is cold. Webtorrent needs a browser, so stays off.
	cfg.DisableWebseeds = false
	cfg.AcceptPeerConnections = true
	cfg.DisableUTP = true
	cfg.Debug = e.cfg.Debug
	// Completed/seeding torrents reject incoming conns by default ("don't want
	// conns right now"), which starves fresh leechers dialing us. Always accept;
	// MaxConns bounds the cost.
	cfg.AlwaysWantConns = true // uTP is unreliable on Windows (loopback handshakes EOF); TCP only
	cfg.EstablishedConnsPerTorrent = e.cfg.MaxConns
	cfg.DataDir = e.cfg.BaseDir
	e.st = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir: e.cfg.BaseDir,
		FilePathMaker: func(o storage.FilePathMakerOpts) string {
			return filepath.Join(o.File.BestPath()...)
		},
		TorrentDirMaker: func(baseDir string, info *metainfo.Info, hash metainfo.Hash) string {
			d := filepath.Join(baseDir, info.BestName())
			if registered := e.dirs[hash.HexString()]; registered != "" {
				d = registered
			}
			if len(info.Files) == 0 {
				// Single-file torrents: anacrolix treats TorrentDir as the
				// whole-file path (FilePathMaker returns ""), so the name must
				// stay attached. Returning only the directory opens the folder
				// itself — Windows then fails with CreateFileMapping/read
				// "Incorrect function" on every piece.
				return filepath.Join(d, info.BestName())
			}
			return d
		},
		UsePartFiles:    partFilesOff(),
		PieceCompletion: storage.NewMapPieceCompletion(), // verify from disk on add
	})
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		e.st.Close()
		e.st = nil
		return nil, fmt.Errorf("BT 客户端启动失败: %w", err)
	}
	e.cl = cl
	e.port = cl.LocalPort()
	return cl, nil
}

// ListenPort is the port the shared client listens on (0 before start).
func (e *Engine) ListenPort() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.port
}

// Close shuts the client and storage down (app exit).
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var msgs []string
	if e.cl != nil {
		for _, er := range e.cl.Close() {
			if er != nil {
				msgs = append(msgs, er.Error())
			}
		}
		e.cl = nil
	}
	if e.st != nil {
		if err := e.st.Close(); err != nil {
			msgs = append(msgs, err.Error())
		}
		e.st = nil
	}
	if len(msgs) > 0 {
		return fmt.Errorf("bt close: %s", strings.Join(msgs, "; "))
	}
	return nil
}

// Stats is a snapshot of the shared client (settings panel / diagnostics).
type Stats struct {
	ListenPort int  `json:"listenPort"`
	Torrents   int  `json:"torrents"`
	Peers      int  `json:"peers"`
	Seeds      int  `json:"seeds"`
	Uploaded   int  `json:"uploaded"`
	Downloaded int  `json:"downloaded"`
	Running    bool `json:"running"`
}

// Stats returns the current client state.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	cl, port := e.cl, e.port
	e.mu.Unlock()
	s := Stats{ListenPort: port, Running: cl != nil}
	if cl == nil {
		return s
	}
	for _, t := range cl.Torrents() {
		st := t.Stats()
		s.Torrents++
		s.Peers += st.TotalPeers
		s.Seeds += st.ConnectedSeeders
		s.Downloaded += int(st.PeerConns.BytesReadData.Int64())
		s.Uploaded += int(st.PeerConns.BytesWrittenData.Int64())
	}
	return s
}

// IsMagnetURL reports whether raw is a magnet link carrying a v1 infohash.
func IsMagnetURL(raw string) bool {
	u := strings.ToLower(strings.TrimSpace(raw))
	return strings.HasPrefix(u, "magnet:?") && strings.Contains(u, "btih:")
}

// IsTorrentSource reports whether raw is a .torrent (local path or http(s) URL).
func IsTorrentSource(raw string) bool {
	u := strings.ToLower(strings.TrimSpace(raw))
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return strings.Contains(u, ".torrent")
	}
	return strings.HasSuffix(u, ".torrent")
}

// InfoHashFromMagnet extracts the 40-char hex infohash from a magnet URI.
func InfoHashFromMagnet(raw string) (string, error) {
	spec, err := torrent.TorrentSpecFromMagnetUri(raw)
	if err != nil {
		return "", fmt.Errorf("磁力链接解析失败: %w", err)
	}
	hex := spec.InfoHash.HexString()
	if hex == "" || strings.Trim(hex, "0") == "" {
		return "", fmt.Errorf("磁力链接缺少 btih 信息哈希")
	}
	return hex, nil
}

// TorrentMeta is the parsed view of a .torrent file the Manager needs when
// creating a task (before anything touches the network).
type TorrentMeta struct {
	InfoHash  string            `json:"infoHash"`
	Name      string            `json:"name"`
	Size      int64             `json:"size"`
	Files     []model.FileEntry `json:"files"`
	Trackers  [][]string        `json:"trackers"`
	Announce  string            `json:"announce"`
	InfoBytes []byte            `json:"-"`
}

// ReadTorrentFile parses a .torrent file from disk.
func ReadTorrentFile(p string) (*TorrentMeta, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readTorrent(f)
}

// ReadTorrentBytes parses raw .torrent bytes (the UI uploads the picked file).
func ReadTorrentBytes(b []byte) (*TorrentMeta, error) {
	return readTorrent(bytes.NewReader(b))
}

func readTorrent(r io.Reader) (*TorrentMeta, error) {
	mi, err := metainfo.Load(r)
	if err != nil {
		return nil, fmt.Errorf("种子文件解析失败: %w", err)
	}
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, fmt.Errorf("种子文件 info 段损坏: %w", err)
	}
	if info.NumPieces() == 0 || info.TotalLength() <= 0 {
		return nil, fmt.Errorf("种子文件为空或分片信息缺失")
	}
	name := info.BestName()
	if name == "" || name == metainfo.NoName {
		name = mi.HashInfoBytes().HexString()[:12]
	}
	return &TorrentMeta{
		InfoHash:  mi.HashInfoBytes().HexString(),
		Name:      name,
		Size:      info.TotalLength(),
		Files:     fileEntries(&info),
		Trackers:  mi.UpvertedAnnounceList(),
		Announce:  mi.Announce,
		InfoBytes: mi.InfoBytes,
	}, nil
}

func fileEntries(info *metainfo.Info) []model.FileEntry {
	fs := info.UpvertedFiles()
	out := make([]model.FileEntry, 0, len(fs))
	for _, f := range fs {
		name := path.Join(f.Path...)
		if name == "" || name == "." {
			name = info.BestName()
		}
		out = append(out, model.FileEntry{Name: name, Size: f.Length})
	}
	return out
}

// SingleFile reports a torrent payload that is exactly one file (so it can land
// straight in the download dir instead of a wrapper folder).
func SingleFile(files []model.FileEntry) bool {
	return len(files) == 1 && !strings.Contains(files[0].Name, "/")
}

// SavePathFor computes the task destination: single-file torrents land directly
// in baseDir, multi-file torrents keep their top-level folder.
func SavePathFor(baseDir string, m *TorrentMeta) string {
	if SingleFile(m.Files) {
		return filepath.Join(baseDir, filepath.FromSlash(m.Files[0].Name))
	}
	return filepath.Join(baseDir, filepath.FromSlash(m.Name))
}

// mergeTiers appends extra trackers into the last tier, skipping duplicates.
func mergeTiers(tiers [][]string, extra []string) [][]string {
	if len(extra) == 0 {
		return tiers
	}
	seen := map[string]bool{}
	for _, tier := range tiers {
		for _, tr := range tier {
			seen[tr] = true
		}
	}
	if len(tiers) == 0 {
		tiers = [][]string{nil}
	}
	last := len(tiers) - 1
	for _, tr := range extra {
		if tr == "" || seen[tr] {
			continue
		}
		tiers[last] = append(tiers[last], tr)
	}
	return tiers
}

func sortStrings(s []string) { sort.Strings(s) }

// fetchTorrentBytes loads a .torrent from a local path or an http(s) URL.
func fetchTorrentBytes(ctx context.Context, raw string) ([]byte, error) {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("下载种子文件失败: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("种子文件地址 HTTP %d", resp.StatusCode)
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	if strings.HasPrefix(raw, "file://") {
		raw = filepath.FromSlash(strings.TrimPrefix(raw, "file://")) // file:///C:/x -> C:/x
	}
	return os.ReadFile(raw)
}

// StallNoPeers is the "no peers/seeds" condition: reported as an error after a
// bounded silence, so a dead swarm never spins in 下载中 (the anti-loop rule the
// HTTP engine already follows).
var StallNoPeers = errors.New("无可用节点/种子")

// Run drives one BT task: add torrent -> (magnet) fetch metadata -> download
// every piece -> verify -> report. Cancellation stops the task; on-disk
// progress is real bytes, so a later Start resumes with piece verification.
func (e *Engine) Run(ctx context.Context, task *model.Task, opts *RunOptions) error {
	if opts == nil {
		opts = &RunOptions{}
	}
	raw := strings.TrimSpace(task.Req.URL)
	if raw == "" {
		return fmt.Errorf("BT 任务缺少磁力链接/种子地址")
	}
	cl, err := e.client()
	if err != nil {
		return err
	}

	spec, isMagnet, err := buildSpec(ctx, raw, e.cfg)
	if err != nil {
		return err
	}
	spec.Storage = e.st
	hex := spec.InfoHash.HexString()
	if hex == "" || strings.Trim(hex, "0") == "" {
		return fmt.Errorf("无法确定任务的信息哈希")
	}
	// Route this torrent's files into the task's own directory, keyed by
	// infohash (known up-front for magnets too). Multi-file torrents get their
	// top-level folder from FilePathMaker, so nothing bleeds into the root.
	dir := filepath.Dir(task.SavePath)
	if task.SavePath == "" {
		dir = e.cfg.BaseDir
	}
	e.RegisterDir(hex, dir)

	t, _, err := cl.AddTorrentSpec(spec)
	if err != nil {
		return fmt.Errorf("加入 BT 任务失败: %w", err)
	}
	live := e.registerTask(task.ID, t, ParseSelection(task.BTSel))
	defer e.clearTask(task.ID, t)
	if opts.OnLive != nil {
		opts.OnLive()
	}
	// Pinned peers travel inside the spec (spec.PeerAddrs: magnet x.pe, config
	// ExtraPeers), which is the library's supported path for private swarms,
	// LAN seeding and tests.

	// Magnet: metadata (name, size, file list) must arrive from peers before the
	// task can show anything meaningful. Bound the wait: a swarm nobody seeds
	// must report 无节点/种子, not spin.
	if isMagnet && t.Info() == nil {
		select {
		case <-t.GotInfo():
		case <-time.After(e.cfg.MetadataTimeout):
			e.dropIfUnused(cl, t)
			return fmt.Errorf("磁力链接在 %v 内没有拿到元数据（节点/种子为 0 或被墙）：换一条磁力、添加节点（x.pe）或挂梯子后重试: %w",
				e.cfg.MetadataTimeout, StallNoPeers)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	info := t.Info()
	if info == nil {
		e.dropIfUnused(cl, t)
		return fmt.Errorf("种子元数据不可用")
	}
	files := fileEntries(info)
	size := t.Length()
	// The live handle, not task.BTSel: a selection that landed while metadata
	// was still arriving is already in there.
	sel, selVer := live.get()
	metaSize := SelectedSize(files, sel) // full length when no selection
	name := t.Name()
	if name == "" {
		name = info.BestName()
	}
	savePath := SavePathFor(dir, &TorrentMeta{Name: name, Files: files})
	if opts.OnMeta != nil {
		if err := opts.OnMeta(name, metaSize, files, savePath); err != nil {
			return err
		}
	}
	task.Name, task.Size, task.Files, task.SavePath = name, metaSize, files, savePath

	applySelection(t, sel)

	var (
		lastComp  int64 = -1
		lastTS          = time.Now()
		lastProg        = time.Now()
		persistAt       = time.Now()
		tick            = time.NewTicker(500 * time.Millisecond)
		peerTick        = time.NewTicker(5 * time.Second)
	)
	defer tick.Stop()
	defer peerTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-peerTick.C:
			// anacrolix dials spec.PeerAddrs once at add; a failed handshake
			// (busy seeder, LAN, private swarms, tests) would never be retried
			// without this nudge. AddPeers re-hands-off a dial for known peers.
			ps := make([]torrent.PeerInfo, 0, len(spec.PeerAddrs))
			for _, p := range spec.PeerAddrs {
				if ap, perr := netip.ParseAddrPort(p); perr == nil {
					ps = append(ps, torrent.PeerInfo{Addr: ap})
				}
			}
			t.AddPeers(ps)
		}
		// 完成目标跟随运行期改选（迅雷/IDM 勾选变化后总量立刻重算）：
		// SetFileSelection 只改 piece 优先级，判定用的 sel 必须在这里重新取。
		if cur, ver := live.get(); ver != selVer {
			sel, selVer = cur, ver
		}
		comp, total := selectedProgress(t, sel)
		now := time.Now()
		var speed float64
		if lastComp >= 0 {
			if dt := now.Sub(lastTS).Seconds(); dt > 0 {
				speed = float64(comp-lastComp) / dt
			}
		}
		st := t.Stats()
		if comp > lastComp {
			lastProg = now
		}
		lastComp, lastTS = comp, now

		if opts.Persist != nil && now.Sub(persistAt) >= 2*time.Second {
			persistAt = now
			opts.Persist(comp)
		}
		opts.Emit(model.Event{Type: "progress", TaskID: task.ID, Status: "running",
			Downloaded: comp, Size: total, Speed: speed,
			Peers: st.TotalPeers, Seeds: st.ConnectedSeeders,
			Uploaded: st.PeerConns.BytesWrittenData.Int64(),
			TS:       now.UnixMilli()})

		if total > 0 && comp >= total {
			if opts.Persist != nil {
				opts.Persist(total)
			}
			if !e.cfg.SeedAfter {
				// Downloader semantics: a finished task must stop occupying
				// upload bandwidth and sockets (迅雷/IDM do the same).
				e.dropIfUnused(cl, t)
			}
			return nil
		}
		// Dead swarm: bounded silence with zero peers and zero progress.
		if st.TotalPeers == 0 && comp < size && now.Sub(lastProg) >= NoPeerGrace {
			return fmt.Errorf("BT 任务停滞：连续 %v 没有任何节点（种子为 0 或全部掉线），已保留 %d/%d 字节。可稍后点「开始」续传，或在设置里增加 tracker/节点",
				NoPeerGrace, comp, size)
		}
	}
}

// dropIfUnused releases a torrent the app no longer needs.
func (e *Engine) dropIfUnused(cl *torrent.Client, t *torrent.Torrent) {
	if e.cfg.SeedAfter {
		return
	}
	t.DisallowDataDownload()
	t.Drop()
}

// NoPeerGrace bounds the "no peers at all" phase before a task reports 停滞.
const NoPeerGrace = 3 * time.Minute

// buildSpec turns the task source (magnet URI / .torrent path / .torrent URL)
// into a torrent spec, with trackers and pinned peers merged in.
func buildSpec(ctx context.Context, raw string, cfg Config) (*torrent.TorrentSpec, bool, error) {
	if IsMagnetURL(raw) {
		spec, err := torrent.TorrentSpecFromMagnetUri(raw)
		if err != nil {
			return nil, true, fmt.Errorf("磁力链接解析失败: %w", err)
		}
		spec.Trackers = mergeTiers(spec.Trackers, append(cfg.Trackers, DefaultTrackers()...))
		spec.PeerAddrs = append(spec.PeerAddrs, cfg.ExtraPeers...)
		return spec, true, nil
	}
	b, err := fetchTorrentBytes(ctx, raw)
	if err != nil {
		return nil, false, err
	}
	mi, err := metainfo.Load(bytes.NewReader(b))
	if err != nil {
		return nil, false, fmt.Errorf("种子文件解析失败: %w", err)
	}
	spec, err := torrent.TorrentSpecFromMetaInfoErr(mi)
	if err != nil {
		return nil, false, fmt.Errorf("种子文件信息不完整: %w", err)
	}
	spec.Trackers = mergeTiers(spec.Trackers, append(cfg.Trackers, DefaultTrackers()...))
	spec.PeerAddrs = append(spec.PeerAddrs, cfg.ExtraPeers...)
	return spec, false, nil
}

// IsBT reports whether a task is a BitTorrent task (magnet link, .torrent file
// path or .torrent URL). The Manager uses this to pick the bttx engine instead
// of probing HTTP — the old code probed every URL, which failed instantly with
// "unsupported scheme magnet:" for magnets (the "BT 完全不能用" symptom).
func IsBT(t *model.Task) bool {
	if t == nil {
		return false
	}
	raw := strings.TrimSpace(t.Req.URL)
	return IsMagnetURL(raw) || IsTorrentSource(raw)
}

// InfoHashOf resolves the task's infohash up-front when possible (magnet link,
// local .torrent file). Returns "" when it can only be known after the network
// step (an http .torrent URL).
func InfoHashOf(raw string) string {
	raw = strings.TrimSpace(raw)
	if IsMagnetURL(raw) {
		if hex, err := InfoHashFromMagnet(raw); err == nil {
			return hex
		}
		return ""
	}
	if IsTorrentSource(raw) && !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		if m, err := ReadTorrentFile(raw); err == nil {
			return m.InfoHash
		}
	}
	return ""
}

// Drop removes a torrent from the shared client (task deleted by the user).
// Data on disk is left alone; only the swarm membership and sockets go away.
func (e *Engine) Drop(infoHashHex string) bool {
	e.mu.Lock()
	cl := e.cl
	e.mu.Unlock()
	if cl == nil || infoHashHex == "" {
		return false
	}
	var ih metainfo.Hash
	if err := ih.FromHexString(infoHashHex); err != nil {
		return false
	}
	var t *torrent.Torrent
	for _, cand := range cl.Torrents() {
		if cand.InfoHash().HexString() == ih.HexString() {
			t = cand
			break
		}
	}
	if t == nil {
		return false
	}
	t.DisallowDataDownload()
	t.DisallowDataUpload()
	t.Drop()
	e.mu.Lock()
	delete(e.dirs, infoHashHex)
	e.mu.Unlock()
	return true
}

// DropTask removes a task's torrent, resolving its infohash from the URL.
func (e *Engine) DropTask(t *model.Task) bool {
	return e.Drop(InfoHashOf(t.Req.URL))
}

// SeedAfter reports the current post-completion seeding policy.
func (e *Engine) SeedAfter() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg.SeedAfter
}

// SetPolicy applies 做种/禁止上传/额外 tracker/节点 live. The listen port cannot
// change while the client is bound; the caller restarts the engine for that.
func (e *Engine) SetPolicy(seedAfter, noUpload bool, trackers, peers []string) {
	e.mu.Lock()
	e.cfg.SeedAfter = seedAfter
	e.cfg.NoUpload = noUpload
	e.cfg.Trackers = trackers
	e.cfg.ExtraPeers = peers
	cl := e.cl
	e.mu.Unlock()
	if cl == nil {
		return
	}
	for _, t := range cl.Torrents() {
		if noUpload {
			t.DisallowDataUpload()
		} else if seedAfter {
			t.AllowDataUpload()
		}
	}
}
