package download

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/bttx"
	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/download/hlsx"
	"github.com/tey-sun/suntey/core/internal/download/httpx"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/store"
)

// runHandle identifies one run() goroutine generation. Auto-relaunch (range
// downgrade) registers a new generation under the same task id; cleanup must
// only ever remove its own generation.
type runHandle struct {
	cancel context.CancelFunc
	gen    uint64
}

// Manager owns task lifecycle: resolve -> create -> run -> persist, streaming
// events. It is the single in-process owner the dispatcher (and later the
// Tauri front-end) drives, mirroring GoPeed's in-process Dispatch design.
type Manager struct {
	Store  *store.Store
	Bus    *event.Bus
	Client *http.Client
	Cfg    model.Config

	mu      sync.Mutex
	running map[string]runHandle
	// stopReasons 记录「谁让这个任务停的」：用户点的（永不自动续传）还是
	// 关机/降级（下次启动自动续传）。run() 收尾时取走并写进任务。
	stopReasons map[string]model.PauseReason
	runSeq      uint64
	dl          *httpx.Downloader
	hls         *hlsx.Downloader
	BT          *bttx.Engine
}

func NewManager(st *store.Store, bus *event.Bus, cfg model.Config) *Manager {
	cl := httpx.NewClient()
	m := &Manager{
		Store:       st,
		Bus:         bus,
		Client:      cl,
		Cfg:         cfg,
		running:     map[string]runHandle{},
		stopReasons: map[string]model.PauseReason{},
		dl:          httpx.NewDownloader(cl, cfg.UserAgent),
		hls:         hlsx.NewDownloader(cl, cfg.UserAgent),
	}
	if cfg.WriteBufferKB > 0 {
		m.dl.BufferKB = cfg.WriteBufferKB
		m.hls.BufferKB = cfg.WriteBufferKB
	}
	if cfg.RateLimitKBps != nil && *cfg.RateLimitKBps > 0 {
		httpx.SetRate(*cfg.RateLimitKBps * 1024) // persisted cap survives restarts
	}
	m.BT = bttx.New(bttxConfig(cfg))
	return m
}

// bttxConfig maps the persisted Config onto the BT engine's knobs, applying the
// documented defaults (做种 = true, 允许上传 = true) for unset pointers.
func bttxConfig(cfg model.Config) bttx.Config {
	seedAfter, noUpload := true, false
	if cfg.BTSeedAfter != nil {
		seedAfter = *cfg.BTSeedAfter
	}
	if cfg.BTNoUpload != nil {
		noUpload = *cfg.BTNoUpload
	}
	return bttx.Config{
		BaseDir:    cfg.DownloadDir,
		ListenPort: cfg.BTPort,
		SeedAfter:  seedAfter,
		NoUpload:   noUpload,
		Trackers:   cfg.BTTrackers,
		ExtraPeers: cfg.BTPeers,
	}
}

// BTStats exposes the shared BitTorrent client state (listen port, peers,
// seeds, upload share) for the settings panel.
func (m *Manager) BTStats() bttx.Stats { return m.BT.Stats() }

// SetFileSelection narrows a BT task to the given file indices (迅雷/IDM
// "下载详情" 勾选). The selection persists on the task (BTSel) and Size is
// recomputed from it; while the torrent is live the priorities apply at once,
// otherwise the next run applies them. Returns the refreshed task.
func (m *Manager) SetFileSelection(id string, keep []int) (*model.Task, error) {
	t, err := m.Store.GetTask(id)
	if err != nil {
		return nil, err
	}
	if !bttx.IsBT(t) {
		return t, errors.New("非 BT 任务，不支持按文件勾选")
	}
	if len(keep) == 0 {
		return t, errors.New("至少勾选一个文件")
	}
	sel := bttx.ParseSelection(bttx.JoinSelection(keep))
	if len(t.Files) > 0 {
		clamped := map[int]bool{}
		for i := range t.Files {
			if sel[i] {
				clamped[i] = true
			}
		}
		if len(clamped) == 0 {
			return t, errors.New("勾选的文件索引不在种子文件列表内")
		}
		sel = clamped
		t.Size = bttx.SelectedSize(t.Files, sel)
	}
	t.BTSel = bttx.JoinSelection(keep)
	if t.Status == model.StatusComplete {
		// Finished whole-torrent before this feature existed, or a widened
		// selection on a done task: nothing left to fetch, just record it.
		t.UpdatedAt = time.Now()
		if err := m.Store.PutTask(t); err != nil {
			return t, err
		}
		m.emitStatus(t)
		return t, nil
	}
	if err := m.Store.PutTask(t); err != nil {
		return t, err
	}
	// Live torrents pick the change up instantly; not-running/no-meta cases
	// keep the persisted selection and apply it on the next run.
	if err := m.BT.SetFileSelection(id, keep); err != nil &&
		!errors.Is(err, bttx.ErrBTNotRunning) && !errors.Is(err, bttx.ErrBTNoMeta) {
		return t, err
	}
	m.emitStatus(t)
	return t, nil
}

// BTFiles lists a BT task's files with the current selection flags.
func (m *Manager) BTFiles(id string) ([]bttx.FileItem, error) {
	t, err := m.Store.GetTask(id)
	if err != nil {
		return nil, err
	}
	if !bttx.IsBT(t) || len(t.Files) == 0 {
		return nil, errors.New("BT 元数据未就绪（任务未开始或磁力还没拿到文件列表）")
	}
	return bttx.FileItems(t.Files, bttx.ParseSelection(t.BTSel)), nil
}

func (m *Manager) Resolve(ctx context.Context, req model.Request) (*model.ResolveResult, error) {
	req.URL = model.NormalizeURL(req.URL) // bare host / protocol-relative paste
	res, err := httpx.Probe(ctx, m.Client, req, m.Cfg.UserAgent)
	if err != nil {
		return nil, err
	}
	// M6 page resolver: the pasted URL is an HTML page, not a file — parse its
	// links and resolve the best one (ProbedURL records the real file link;
	// Create then swaps the task onto it with the page as Referer).
	if res.IsPage {
		c, err := httpx.ResolvePage(ctx, m.Client, req, m.Cfg.UserAgent, httpx.Probe)
		if err != nil {
			// Anti-leech hosts 403 the bare link probe even though the
			// extension forwarded the page's own cookies (Header replay
			// beats the derived Referer in the request builder) — retry the
			// best candidate with those headers so a resolvable page never
			// fails with "候选链接均无法访问" on a normal software-downloads page.
			if len(req.Header) > 0 {
				return httpx.ResolvePageHeaders(ctx, m.Client, req, m.Cfg.UserAgent, httpx.Probe, req.Header)
			}
			return nil, err
		}
		return c, nil
	}
	return res, nil
}

// UpdateConfig merges non-zero settings, applies them live and persists.
// APIToken is never changed through this path (rotation stays out of scope).
func (m *Manager) UpdateConfig(in model.Config) (model.Config, error) {
	if in.DownloadDir != "" {
		if err := os.MkdirAll(in.DownloadDir, 0o755); err != nil {
			return m.Cfg, fmt.Errorf("downloadDir: %w", err)
		}
		m.Cfg.DownloadDir = in.DownloadDir
	}
	if in.DefaultConn > 0 {
		m.Cfg.DefaultConn = in.DefaultConn
	}
	if in.MaxRunning > 0 {
		m.Cfg.MaxRunning = in.MaxRunning
	}
	if in.UserAgent != "" {
		m.Cfg.UserAgent = in.UserAgent
		m.dl.UA = in.UserAgent
		m.hls.UA = in.UserAgent
	}
	if in.WriteBufferKB > 0 {
		m.Cfg.WriteBufferKB = in.WriteBufferKB
		m.dl.BufferKB = in.WriteBufferKB
		m.hls.BufferKB = in.WriteBufferKB
	}
	if in.RateLimitKBps != nil {
		m.Cfg.RateLimitKBps = in.RateLimitKBps
		httpx.SetRate(*in.RateLimitKBps * 1024) // 0 clears the cap
		httpx.ResetRateBucket()                 // an old rate must not lend a burst
	}
	if in.ClipboardWatch != nil {
		m.Cfg.ClipboardWatch = in.ClipboardWatch
	}
	if in.AutoResumeOnLaunch != nil {
		m.Cfg.AutoResumeOnLaunch = in.AutoResumeOnLaunch
	}
	if btChanged := m.applyBTConfig(in); btChanged != nil {
		// The policy itself is already saved; only the listen-port rebinding
		// had to wait, so surface that as a hint rather than failing the save.
		_ = btChanged
	}
	if err := m.Store.SaveConfig(m.Cfg); err != nil {
		return m.Cfg, err
	}
	return m.Cfg, nil
}

// applyBTConfig merges the BT settings. 做种/禁止上传/trackers/节点 apply live;
// the listen port needs the client to rebind, which is only done when no BT
// task is in flight (dropping sockets mid-download would look like corruption).
func (m *Manager) applyBTConfig(in model.Config) error {
	portChanged := in.BTPort > 0 && in.BTPort != m.Cfg.BTPort
	policyChanged := in.BTPort > 0 || in.BTSeedAfter != nil || in.BTNoUpload != nil ||
		len(in.BTTrackers) > 0 || len(in.BTPeers) > 0
	if !policyChanged {
		return nil
	}
	if in.BTPort > 0 {
		m.Cfg.BTPort = in.BTPort
	}
	if in.BTSeedAfter != nil {
		m.Cfg.BTSeedAfter = in.BTSeedAfter
	}
	if in.BTNoUpload != nil {
		m.Cfg.BTNoUpload = in.BTNoUpload
	}
	if len(in.BTTrackers) > 0 {
		m.Cfg.BTTrackers = in.BTTrackers
	}
	if len(in.BTPeers) > 0 {
		m.Cfg.BTPeers = in.BTPeers
	}
	seedAfter, noUpload := true, false
	if m.Cfg.BTSeedAfter != nil {
		seedAfter = *m.Cfg.BTSeedAfter
	}
	if m.Cfg.BTNoUpload != nil {
		noUpload = *m.Cfg.BTNoUpload
	}
	m.BT.SetPolicy(seedAfter, noUpload, m.Cfg.BTTrackers, m.Cfg.BTPeers)
	if !portChanged {
		return nil
	}
	if m.anyBTRunning() {
		return fmt.Errorf("BT 监听端口已保存，但需要暂停所有 BT 任务后才生效")
	}
	_ = m.BT.Close()
	m.BT = bttx.New(bttxConfig(m.Cfg))
	return nil
}

// anyBTRunning reports whether a BitTorrent task is currently in flight.
func (m *Manager) anyBTRunning() bool {
	m.mu.Lock()
	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		if t, err := m.Store.GetTask(id); err == nil && bttx.IsBT(t) {
			return true
		}
	}
	return false
}

// Create resolves and registers a task (status Ready), persisted to the store.
// Re-sending a URL that already has an unfinished task refreshes its replay
// headers (fresh anti-leech cookies/referer pushed from the browser extension)
// and returns the existing task instead of starting a duplicate from zero —
// both the duplicate-task fix and the recovery path for expired links.
func (m *Manager) Create(ctx context.Context, req model.Request) (*model.Task, error) {
	req.URL = model.NormalizeURL(req.URL)
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(req.URL)), "ed2k://") {
		return nil, fmt.Errorf("ed2k 链接暂不支持下载（首版识别）")
	}
	if ex := m.findActiveByURL(req.URL); ex != nil {
		if len(req.Header) > 0 {
			ex.Req.Header = req.Header
			ex.UpdatedAt = time.Now()
			_ = m.Store.PutTask(ex)
		}
		return ex, nil
	}
	conn := m.Cfg.DefaultConn
	if conn < 1 {
		conn = 8
	}
	if req.Conn > 0 { // per-task override (see model.Request.Conn)
		conn = req.Conn
	}
	// BitTorrent (magnet link / .torrent path / .torrent URL): the swarm is the
	// source, so there is nothing to HTTP-probe. Before this branch existed,
	// every magnet went through the HTTP resolver and failed instantly with
	// "unsupported scheme" — the "BT 功能完全不能用" symptom.
	if bttx.IsMagnetURL(req.URL) || bttx.IsTorrentSource(req.URL) {
		return m.createBT(req)
	}
	// HLS (m3u8 video): no size probe; segments drive progress, output is
	// merged/remuxed by hlsx at completion.
	if hlsx.IsHLSURL(req.URL) {
		name := req.Filename
		if name == "" {
			name = hlsx.BaseNameFromURL(req.URL)
		}
		savePath, err := m.uniquePath(name)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		t := &model.Task{
			ID:           newID(),
			Req:          req,
			Status:       model.StatusReady,
			Name:         name,
			SavePath:     savePath,
			Size:         -1,
			SupportRange: false,
			Conn:         conn,
			HLS:          &model.HLSState{MediaURL: req.URL},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := m.Store.PutTask(t); err != nil {
			return nil, err
		}
		return t, nil
	}
	res, err := m.Resolve(ctx, req)
	if err != nil {
		return nil, err
	}
	// Page resolved to a direct file link: the task downloads the file, and
	// the page becomes the Referer (anti-leech CDNs 403 without it). Restarts
	// re-probe t.Req.URL — already the file, so the page is parsed once.
	if res.ProbedURL != "" && res.ProbedURL != req.URL {
		pageURL := req.URL
		req.URL = res.ProbedURL
		req.Referer = pageURL
	}
	name := res.Name
	if req.Filename != "" {
		name = req.Filename
	}
	savePath, err := m.uniquePath(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	t := &model.Task{
		ID:       newID(),
		Req:      req,
		Status:   model.StatusReady,
		Name:     name,
		SavePath: savePath,
		// 下载写中间文件，完成后原子改名：半截文件永远不会顶着最终名，
		// 也永远不会覆盖用户已有的同名文件（§6.1 约束 4）。
		TempPath:     savePath + model.PartialSuffix,
		Size:         res.Size,
		SupportRange: res.SupportRange,
		Conn:         conn,
		ProbedURL:    res.ProbedURL,
		// 源文件身份在建任务时就登记一次，续传时才有对照物。
		Identity:    model.FromProbe(res.Size, res.LastModified, res.ETag),
		SnapVersion: model.SnapshotVersion,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := m.Store.PutTask(t); err != nil {
		return nil, err
	}
	return t, nil
}

// Start begins (or resumes) a task in the background; returns once launched.
func (m *Manager) Start(id string) error {
	if _, err := m.Store.GetTask(id); err != nil {
		return err
	}
	m.mu.Lock()
	if _, ok := m.running[id]; ok {
		m.mu.Unlock()
		return nil // idempotent: already running (UI/extension double-start is safe)
	}
	if m.Cfg.MaxRunning > 0 && len(m.running) >= m.Cfg.MaxRunning {
		m.mu.Unlock()
		return fmt.Errorf("已达最大并发数 %d（可在设置中调整）", m.Cfg.MaxRunning)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.runSeq++
	gen := m.runSeq
	m.running[id] = runHandle{cancel: cancel, gen: gen}
	m.mu.Unlock()

	go m.run(ctx, id, cancel, gen)
	return nil
}

// Stop cancels a running task (the run's deferred handler then persists the
// final Paused state and releases the slot).
//
// When NOTHING is running for that id, the row state is settled here. A task can
// be stranded at Status=Running by a killed process, by a rejected auto-relaunch
// or by a crash between "set Running" and "set final". Before this fix, pressing
// 暂停 on such a row did nothing, so the UI sat at "下载中 · 0 B/s" and the resume
// button (only rendered for Paused) never appeared — the "任务卡在下载中、点暂停
// 没反应、只能删除重下（进度全丢）" symptom. Marking it Paused restores 开始/续传.
func (m *Manager) Stop(id string) { m.stopWith(id, model.PauseUser) }

// stopWith 停止任务并记下「为什么停」。自动续传（M1.5 场景 B）只捞非用户暂停，
// 所以这个原因是「重启后要不要替你继续下」的唯一依据。
func (m *Manager) stopWith(id string, reason model.PauseReason) {
	m.mu.Lock()
	if m.stopReasons == nil {
		m.stopReasons = map[string]model.PauseReason{}
	}
	m.stopReasons[id] = reason
	h, running := m.running[id]
	m.mu.Unlock()
	if running {
		h.cancel()
		return
	}
	t, err := m.Store.GetTask(id)
	if err != nil {
		return
	}
	if t.Status == model.StatusRunning || t.Status == model.StatusResolving {
		t.Status = model.StatusPaused
		t.PauseReason = reason
		t.Speed = 0
		t.UpdatedAt = time.Now()
		_ = m.Store.PutTask(t)
		m.emitStatus(t)
	}
}

// GracefulShutdown stops every running task so run() persists pause snapshots
// (chunk offsets included) for resume on next launch. Returns the count.
// Used by the Tauri shell via POST /shutdown before the window closes.
func (m *Manager) GracefulShutdown() int {
	ts, err := m.Store.ListTasks()
	if err != nil {
		return 0
	}
	n := 0
	for _, t := range ts {
		if t != nil && t.Status == model.StatusRunning {
			// 原因写 shutdown：下次启动自动续传（用户没点暂停，是我们替他停的）。
			m.stopWith(t.ID, model.PauseShutdown)
			n++
		}
	}
	return n
}

func (m *Manager) run(ctx context.Context, id string, cancel context.CancelFunc, gen uint64) {
	release := func() {
		m.mu.Lock()
		if h, ok := m.running[id]; ok && h.gen == gen {
			delete(m.running, id)
		}
		m.mu.Unlock()
	}
	downgradeRestart := false
	defer func() {
		release()
		cancel()
	}()

	t, err := m.Store.GetTask(id)
	if err != nil {
		return
	}
	t.Status = model.StatusRunning
	t.Err = ""
	t.PauseReason = model.PauseNone
	t.SnapVersion = model.SnapshotVersion
	_ = m.Store.PutTask(t)
	m.emitStatus(t)

	// 续传前置校验（R11 / M1.5）：**先证伪再续传**。BT 的真相在 piece 位图、
	// HLS 的真相在已完成的分片文件里，各由自己的引擎负责，不在这里重复。
	if !bttx.IsBT(t) && t.HLS == nil {
		note, err := m.verifyResume(ctx, t)
		if err != nil {
			reason := err.Error()
			t.Status = model.StatusError
			t.Err = reason
			t.ResumeNote = reason
			t.PauseReason = model.PauseInvalid
			t.Speed = 0
			t.UpdatedAt = time.Now()
			_ = m.Store.PutTask(t)
			m.Bus.Publish(model.Event{Type: "error", TaskID: id, Status: t.Status.String(),
				Err: reason, Level: "error", TS: time.Now().UnixMilli()})
			return
		}
		if note != "" {
			_ = m.Store.PutTask(t)
			m.Bus.Publish(model.Event{Type: "resume", TaskID: id, Status: t.Status.String(),
				Downloaded: t.Downloaded, Size: t.Size, Err: note, Level: "warn",
				TS: time.Now().UnixMilli()})
		}
	}

	var runErr error
	if bttx.IsBT(t) {
		runErr = m.BT.Run(ctx, t, &bttx.RunOptions{
			Emit: func(e model.Event) { m.Bus.Publish(e) },
			// Completed bytes are the engine's ground truth (piece verification),
			// so mid-run persistence is just this counter: a hard kill resumes
			// with the library re-hashing the files on disk.
			Persist: func(downloaded int64) {
				t.Downloaded = downloaded
				t2, err := m.Store.GetTask(id)
				if err != nil || t2.Status != model.StatusRunning {
					return
				}
				t2.Downloaded = downloaded
				t2.UpdatedAt = time.Now()
				_ = m.Store.PutTask(t2)
			},
			// Metadata (magnet) / file list: fill in the row so the user sees a
			// real name, size and destination instead of a bare infohash.
			OnMeta: func(name string, size int64, files []model.FileEntry, savePath string) error {
				t.Name, t.Size, t.Files, t.SavePath = name, size, files, savePath
				t2, err := m.Store.GetTask(id)
				if err != nil {
					return err
				}
				t2.Name, t2.Size, t2.Files, t2.SavePath = name, size, files, savePath
				t2.UpdatedAt = time.Now()
				if err := m.Store.PutTask(t2); err != nil {
					return err
				}
				m.emitStatus(t2)
				return nil
			},
		})
	} else if t.HLS != nil {
		runErr = m.hls.Run(ctx, t, &hlsx.RunOptions{
			Emit: func(e model.Event) { m.Bus.Publish(e) },
			// Mid-run byte totals only: segment completion is filesystem truth
			// (atomic renames), so a hard kill resumes with zero lost segments.
			Persist: func(total int64) {
				t2, err := m.Store.GetTask(id)
				if err != nil || t2.Status != model.StatusRunning {
					return
				}
				t2.Downloaded = total
				t2.HLS = t.HLS
				t2.UpdatedAt = time.Now()
				_ = m.Store.PutTask(t2)
			},
			Save: func() error {
				t2, err := m.Store.GetTask(id)
				if err != nil {
					return err
				}
				t2.HLS = t.HLS
				t2.Name = t.Name
				t2.SavePath = t.SavePath
				t2.UpdatedAt = time.Now()
				return m.Store.PutTask(t2)
			},
		})
	} else {
		runErr = m.dl.Run(ctx, t, &httpx.RunOptions{
			Emit: func(e model.Event) { m.Bus.Publish(e) },
			// Persist progress mid-run (atomic chunk counters, ~every 2s): a hard
			// kill or crash now loses at most one interval instead of everything —
			// this is what makes resume survive process death.
			Persist: func(total int64, chunks []model.ChunkState) {
				t2, err := m.Store.GetTask(id)
				if err != nil || t2.Status != model.StatusRunning {
					return
				}
				// The planner owns the layout (it may re-split on a connection-count
				// change while keeping every verified byte), so the snapshot stores the
				// LIVE layout, not just counters: what is on disk must always be
				// describable by the persisted chunk list (R11).
				if len(chunks) > 0 {
					t2.Chunks = chunks
					var s int64
					for _, c := range chunks {
						s += c.Downloaded
					}
					t2.Downloaded = s
				} else {
					t2.Downloaded = total
				}
				t2.UpdatedAt = time.Now()
				_ = m.Store.PutTask(t2)
			},
		})
	}

	// Chunked tasks: progress lives in Chunks. Sequential tasks (Chunks==nil)
	// keep the authoritative task.Downloaded maintained by Run — do not
	// overwrite it with sum(nil)==0 or single-conn resume would restart at 0.
	if len(t.Chunks) > 0 {
		t.Downloaded = sumDownloaded(t.Chunks)
	}
	t.UpdatedAt = time.Now()
	isNotRange := runErr != nil && errors.Is(runErr, httpx.ErrServerNotRange)
	switch {
	case isNotRange && ctx.Err() == nil:
		// Probe confirmed Range, production ignores it: downgrade to a single
		// sequential stream and pause so the caller relaunches (CLI loop / UI).
		t.SupportRange = false
		t.Conn = 1
		t.Chunks = nil
		t.Downloaded = 0
		t.Status = model.StatusPaused
		t.PauseReason = model.PauseDowngrade
		downgradeRestart = true
		m.Bus.Publish(model.Event{Type: "downgrade", TaskID: id, Status: t.Status.String(), Size: t.Size, Err: "服务器不支持分块 Range，已自动切换单线程续传", TS: time.Now().UnixMilli()})
	case runErr != nil && ctx.Err() != nil: // user pause/stop
		t.Status = model.StatusPaused
		t.PauseReason = m.takeReason(id)
	case runErr != nil:
		t.Status = model.StatusError
		t.Err = runErr.Error()
	default:
		t.Status = model.StatusComplete
	}
	if len(t.Chunks) > 0 {
		t.Downloaded = sumDownloaded(t.Chunks)
	}
	if isNotRange && ctx.Err() == nil {
		// downgrade: multi-conn partial bytes are unreliable on a lying
		// server — restart clean as a single stream.
		t.Downloaded = 0
	}
	if t.Status == model.StatusComplete && t.Downloaded == 0 && t.Size > 0 {
		t.Downloaded = t.Size
	}
	if t.Status == model.StatusComplete {
		// 中间文件 → 最终文件（原子改名，目标被占用就加序号，绝不覆盖）。
		// 改名失败（杀毒正在扫刚写完的文件很常见）重试三次；再失败就保留
		// 中间文件并把任务标错——字节一个不丢，点「开始」会立刻重试改名。
		var rnErr error
		for i, wait := range []time.Duration{0, 400 * time.Millisecond, 1200 * time.Millisecond} {
			if i > 0 {
				if !sleepCtx(ctx, wait) {
					break
				}
			}
			if rnErr = m.finishWorkFile(t); rnErr == nil {
				break
			}
		}
		if rnErr != nil {
			t.Status = model.StatusError
			t.Err = rnErr.Error()
			t.PauseReason = model.PauseInvalid
		} else {
			t.ResumeNote = ""
			t.PauseReason = model.PauseNone
		}
	}
	t.SnapVersion = model.SnapshotVersion
	t.UpdatedAt = time.Now()
	_ = m.Store.PutTask(t)
	if runErr != nil && ctx.Err() == nil && !isNotRange {
		m.Bus.Publish(model.Event{Type: "error", TaskID: id, Status: t.Status.String(), Err: runErr.Error(), TS: time.Now().UnixMilli()})
	} else {
		m.emitStatus(t)
	}

	// Auto-relaunch (deferred to the very end so the downgraded state above is
	// fully persisted first): run once as a single sequential stream — the
	// task no longer claims Range, so the guard cannot re-trigger -> no loop.
	if downgradeRestart {
		// Free this generation's slot before Start (the outer defer runs only
		// after this function returns and would make Start reject "already
		// running"); the gen check keeps a relaunch's newer slot untouched.
		release()
		if err := m.Start(id); err != nil {
			// The relaunch was refused (concurrency cap / store hiccup). Without
			// this the row stays at "下载中" while nothing runs: the user sees a
			// frozen task with no resume button. Settle it as Paused so 开始 works.
			t.Status = model.StatusPaused
			t.Speed = 0
			t.UpdatedAt = time.Now()
			_ = m.Store.PutTask(t)
			m.Bus.Publish(model.Event{Type: "status", TaskID: id, Status: t.Status.String(),
				Downloaded: t.Downloaded, Size: t.Size,
				Err: "已切换单线程续传，但此刻并发已满：" + err.Error() + " — 点「开始」即可继续",
				TS:  time.Now().UnixMilli()})
		}
	}
}

// createBT registers a BitTorrent task (magnet link, local .torrent path or
// .torrent URL). A local .torrent is parsed here, so name/size/file list show
// immediately; a magnet starts with the infohash as its name and the engine
// fills in the real metadata (name, size, files, save path) once peers hand
// over the info dict — that step is OnMeta below.
func (m *Manager) createBT(req model.Request) (*model.Task, error) {
	raw := strings.TrimSpace(req.URL)
	name := req.Filename
	var size int64 = -1 // unknown until metadata is known
	var files []model.FileEntry

	if bttx.IsTorrentSource(raw) && !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		meta, err := bttx.ReadTorrentFile(raw)
		if err != nil {
			return nil, err
		}
		name, size, files = meta.Name, meta.Size, meta.Files
	}
	if name == "" {
		if hex, err := bttx.InfoHashFromMagnet(raw); err == nil {
			if len(hex) >= 12 {
				hex = hex[:12]
			}
			name = "BT " + hex // renamed to the real torrent name when metadata lands
		} else {
			name = "BT 任务（等待元数据）"
		}
	}
	savePath, err := m.uniquePath(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	t := &model.Task{
		ID:           newID(),
		Req:          req,
		Status:       model.StatusReady,
		Name:         name,
		SavePath:     savePath,
		Size:         size,
		Files:        files,
		SupportRange: false,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.Store.PutTask(t); err != nil {
		return nil, err
	}
	return t, nil
}

// CreateTorrentBytes registers a task from raw .torrent bytes (the webview and
// the browser extension upload the picked file; only the desktop dialog knows
// local paths). The bytes are spooled to the store directory under a stable
// name so the task keeps a readable source across restarts: the engine re-reads
// the .torrent on resume, and deleting the task can clean the spool entry.
func (m *Manager) CreateTorrentBytes(ctx context.Context, raw []byte, filename string) (*model.Task, error) {
	if _, err := bttx.ReadTorrentBytes(raw); err != nil {
		return nil, err
	}
	dir := m.torrentSpoolDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	base := filepath.Base(filepath.FromSlash(filename))
	if base == "." || base == "/" || base == "\\" {
		base = "upload.torrent"
	}
	if !strings.HasSuffix(strings.ToLower(base), ".torrent") {
		base += ".torrent"
	}
	path := filepath.Join(dir, base)
	// Same torrent name re-added: overwrite (the file is content-addressed by
	// its own infohash, so the newest upload is the one the user wants).
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return nil, err
	}
	return m.Create(ctx, model.Request{URL: path})
}

// torrentSpoolDir is where uploaded .torrent files are kept: next to the store
// database, so a portable install keeps everything inside its own folder.
func (m *Manager) torrentSpoolDir() string {
	if p := m.Store.Path(); p != "" {
		return filepath.Join(filepath.Dir(p), "torrents")
	}
	return filepath.Join(m.Cfg.DownloadDir, ".suntey-torrents")
}

// DeleteTask removes a task: stop any in-flight run, release its BitTorrent
// swarm membership, drop the record. Data files are left on disk (the user
// decides what to do with them, same rule as IDM/迅雷).
func (m *Manager) DeleteTask(id string) error {
	m.Stop(id)
	if t, err := m.Store.GetTask(id); err == nil && bttx.IsBT(t) {
		m.BT.DropTask(t)
	}
	return m.Store.DeleteTask(id)
}

func (m *Manager) emitStatus(t *model.Task) {
	typ := "status"
	if t.Status == model.StatusComplete {
		typ = "done"
	}
	m.Bus.Publish(model.Event{Type: typ, TaskID: t.ID, Status: t.Status.String(), Downloaded: t.Downloaded, Size: t.Size, TS: time.Now().UnixMilli()})
}

// uniquePath returns <DownloadDir>/<name>, appending " (n)" if the file exists.
func (m *Manager) uniquePath(name string) (string, error) {
	dir := m.Cfg.DownloadDir
	if dir == "" {
		return "", fmt.Errorf("no download dir configured")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	base := filepath.Base(name)
	if base == "." || base == "/" || base == "\\" {
		base = "download.bin"
	}
	candidate := filepath.Join(dir, base)
	ext := filepath.Ext(base)
	stem := base[:len(base)-len(ext)]
	for i := 1; ; i++ {
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate, nil
		}
		candidate = filepath.Join(dir, fmt.Sprintf("%s (%d)%s", stem, i, ext))
	}
}

// findActiveByURL returns the most recently updated unfinished task with the
// same URL (Ready/Running/Paused/Error), or nil. Completed/error-free re-adds
// intentionally create a fresh task instead.
func (m *Manager) findActiveByURL(url string) *model.Task {
	ts, err := m.Store.ListTasks()
	if err != nil {
		return nil
	}
	var best *model.Task
	for _, t := range ts {
		if t == nil || t.Req.URL != url {
			continue
		}
		switch t.Status {
		case model.StatusReady, model.StatusRunning, model.StatusPaused, model.StatusError:
			if best == nil || t.UpdatedAt.After(best.UpdatedAt) {
				best = t
			}
		}
	}
	return best
}

func sumDownloaded(chunks []model.ChunkState) int64 {
	var n int64
	for _, c := range chunks {
		n += c.Downloaded
	}
	return n
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
