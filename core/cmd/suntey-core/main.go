package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"

	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tey-sun/suntey/core/internal/dispatcher"
	"github.com/tey-sun/suntey/core/internal/download"
	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/download/httpx"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/server"
	"github.com/tey-sun/suntey/core/internal/store"
	"github.com/tey-sun/suntey/core/internal/version"
	"github.com/tey-sun/suntey/core/internal/watcher"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "-v", "--version":
		fmt.Println("suntey core " + version.Version)
	case "resolve":
		cmdResolve(os.Args[2:])
	case "start":
		cmdStart(os.Args[2:])
	case "resume":
		cmdResume(os.Args[2:])
	case "tasks":
		cmdTasks(os.Args[2:])
	case "config":
		cmdConfig(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `suntey core `+version.Version+`
usage:
  core resolve <url> [--conn N]            probe size/Range, print ResolveResult JSON
  core start   <url> [--conn N] [--dir D]  create + download, stream progress JSONL
  core resume  <id>                        resume a paused task by id, stream progress
  core tasks                               list tasks
  core config  [--dir D] [--conn N]        show/set config`)
}

// ---- shared wiring --------------------------------------------------------

type app struct {
	st   *store.Store
	bus  *event.Bus
	mgr  *download.Manager
	disp *dispatcher.Dispatcher
	cfg  model.Config
}

func openApp(storePath, dir string, conn int) *app {
	st, err := store.Open(storePath)
	fatal(err)
	cfg, err := st.LoadConfig()
	fatal(err)
	if cfg.DownloadDir == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "."
		}
		cfg.DownloadDir = filepath.Join(home, "Downloads")
	}
	if cfg.DefaultConn <= 0 {
		cfg.DefaultConn = 8
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = httpx.DefaultUserAgent // single source (client.go)
	}
	if dir != "" {
		cfg.DownloadDir = dir
	}
	if conn > 0 {
		cfg.DefaultConn = conn
	}
	_ = st.SaveConfig(cfg)

	bus := event.NewBus()
	mgr := download.NewManager(st, bus, cfg)
	disp := buildDispatcher(mgr)
	return &app{st: st, bus: bus, mgr: mgr, disp: disp, cfg: cfg}
}

func buildDispatcher(mgr *download.Manager) *dispatcher.Dispatcher {
	d := dispatcher.New()
	d.Register("POST", "/resolve", func(body []byte) (any, error) {
		var req model.Request
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
		return mgr.Resolve(context.Background(), req)
	})
	d.Register("POST", "/tasks", func(body []byte) (any, error) {
		var req model.Request
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
		return mgr.Create(context.Background(), req)
	})
	d.Register("POST", "/tasks/start", func(body []byte) (any, error) {
		var p struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(body, &p)
		return nil, mgr.Start(p.ID)
	})
	d.Register("POST", "/tasks/stop", func(body []byte) (any, error) {
		var p struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(body, &p)
		mgr.Stop(p.ID)
		return nil, nil
	})
	d.Register("GET", "/tasks", func([]byte) (any, error) {
		return mgr.Store.ListTasks()
	})
	d.Register("DELETE", "/tasks", func(body []byte) (any, error) {
		var p struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		// Manager.DeleteTask (not Store.DeleteTask): stops the run AND drops the
		// BitTorrent swarm. Going straight to the store left deleted BT tasks
		// joined to the swarm, seeding and holding sockets for a task the user
		// can no longer see or stop.
		return nil, mgr.DeleteTask(p.ID)
	})
	d.Register("GET", "/version", func([]byte) (any, error) {
		// Version + feature list: the UI shows this in Settings so a user can
		// confirm at a glance which build is running (the shipped exe and a
		// stale one are otherwise visually identical).
		return map[string]any{
			"version":  version.Version,
			"build":    version.Build,
			"features": version.Features,
			"routes":   d.Routes(),
		}, nil
	})
	d.Register("GET", "/bt/stats", func([]byte) (any, error) {
		// Shared client state: listen port (NAT diagnosis), peer/seed counts and
		// the upload share, shown in Settings so "BT 没速度" is answerable.
		return mgr.BTStats(), nil
	})
	d.Register("POST", "/bt/torrent", func(body []byte) (any, error) {
		// Upload a .torrent picked by the webview/extension: the file dialog can
		// hand over a path only on the desktop shell, so the bytes come base64'd.
		var p struct {
			Name     string `json:"name"`
			Filename string `json:"filename"`
			Data     string `json:"data"` // base64
		}
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		raw, err := base64.StdEncoding.DecodeString(p.Data)
		if err != nil {
			return nil, fmt.Errorf("种子数据 base64 解析失败: %w", err)
		}
		name := p.Filename
		if name == "" {
			name = "upload.torrent"
		}
		return mgr.CreateTorrentBytes(context.Background(), raw, name)
	})
	d.Register("GET", "/bt/files", func(body []byte) (any, error) {
		// Per-file list of a BT task with selection flags (下载详情弹窗).
		var p struct {
			ID string `json:"id"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &p); err != nil {
				return nil, err
			}
		}
		return mgr.BTFiles(p.ID)
	})
	d.Register("POST", "/bt/selection", func(body []byte) (any, error) {
		// Narrow a BT task to the listed file indices; live torrents react
		// at once, stopped ones apply the persisted selection on next start.
		var p struct {
			ID    string `json:"id"`
			Files []int  `json:"files"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		return mgr.SetFileSelection(p.ID, p.Files)
	})
	d.Register("GET", "/config", func([]byte) (any, error) {
		cfg := mgr.Cfg
		if cfg.DefaultUA == "" {
			cfg.DefaultUA = httpx.DefaultUserAgent // shipped default, for the UI's UA box
		}
		return cfg, nil
	})
	d.Register("PUT", "/config", func(body []byte) (any, error) {
		var c model.Config
		if err := json.Unmarshal(body, &c); err != nil {
			return nil, err
		}
		return mgr.UpdateConfig(c)
	})
	d.Register("POST", "/shutdown", func([]byte) (any, error) {
		// Graceful window-close: pause all running tasks (run() persists
		// Paused + chunk offsets for resume), then exit once snapshots land.
		n := mgr.GracefulShutdown()
		go func() {
			time.Sleep(1200 * time.Millisecond)
			os.Exit(0)
		}()
		return map[string]any{"stopped": n}, nil
	})
	return d
}

func defaultStorePath() string {
	base, _ := os.UserConfigDir()
	if base == "" {
		base = "."
	}
	return filepath.Join(base, "suntey", "core.db")
}

// ---- commands -------------------------------------------------------------

func cmdResolve(args []string) {
	fs := flag.NewFlagSet("resolve", flag.ExitOnError)
	conn := fs.Int("conn", 0, "connections")
	dir := fs.String("dir", "", "download dir")
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	a := openApp(*storePath, *dir, *conn)
	defer a.st.Close()

	var res model.ResolveResult
	if err := a.disp.CallJSON("POST", "/resolve", model.Request{URL: fs.Arg(0)}, &res); err != nil {
		fatal(err)
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
}

func cmdStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	conn := fs.Int("conn", 0, "connections")
	dir := fs.String("dir", "", "download dir")
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	a := openApp(*storePath, *dir, *conn)
	defer a.st.Close()

	var t model.Task
	if err := a.disp.CallJSON("POST", "/tasks", model.Request{URL: fs.Arg(0)}, &t); err != nil {
		fatal(err)
	}
	emit(map[string]any{"type": "task", "id": t.ID, "name": t.Name, "size": t.Size, "range": t.SupportRange, "savePath": t.SavePath})
	launchAndStream(a, t.ID)
}

func cmdResume(args []string) {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	a := openApp(*storePath, "", 0)
	defer a.st.Close()
	if t, err := a.st.GetTask(fs.Arg(0)); err == nil {
		emit(map[string]any{"type": "task", "id": t.ID, "name": t.Name, "size": t.Size, "downloaded": t.Downloaded, "status": t.Status.String(), "savePath": t.SavePath})
	}
	launchAndStream(a, fs.Arg(0))
}

// launchAndStream starts a task and prints JSONL events until a terminal event.
// A manager downgrade (Range ignored -> sequential) surfaces as a non-user
// "paused"; relaunch once so the sequential stream completes the task.
func launchAndStream(a *app, id string) {
	for {
		stopped, paused := runOnce(a, id)
		if !paused || stopped {
			return
		}
		t, err := a.st.GetTask(id)
		if err != nil || t.SupportRange || t.Status != model.StatusPaused {
			return // user pause, not a downgrade
		}
		emit(map[string]any{"type": "downgrade", "id": id, "note": "server ignores Range; switching to single connection"})
	}
}

func runOnce(a *app, id string) (stopped, paused bool) {
	ch, cancel := a.bus.Subscribe(256)
	defer cancel()

	if err := a.mgr.Start(id); err != nil {
		fatal(err)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		emit(map[string]any{"type": "stopping", "id": id})
		a.mgr.Stop(id)
	}()

	for e := range ch {
		out, _ := json.Marshal(e)
		fmt.Println(string(out))
		switch {
		case e.Type == "stopping":
			stopped = true
		case e.Type == "done", e.Type == "error":
			return stopped, false
		case e.Type == "status" && e.Status == "paused":
			return stopped, true
		}
	}
	return stopped, false
}

func cmdTasks(args []string) {
	fs := flag.NewFlagSet("tasks", flag.ExitOnError)
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)
	a := openApp(*storePath, "", 0)
	defer a.st.Close()
	ts, err := a.st.ListTasks()
	fatal(err)
	out, _ := json.MarshalIndent(ts, "", "  ")
	fmt.Println(string(out))
}

func cmdConfig(args []string) {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	conn := fs.Int("conn", 0, "default connections")
	dir := fs.String("dir", "", "download dir")
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)
	a := openApp(*storePath, *dir, *conn)
	defer a.st.Close()
	out, _ := json.MarshalIndent(a.cfg, "", "  ")
	fmt.Println(string(out))
}

// ---- serve (loopback sidecar for Tauri shell / browser extension) ----------

// cmdServe runs the loopback HTTP API on 127.0.0.1. With --port 0 a random
// free port is chosen; a token is generated when not supplied. The first
// stdout line is {"type":"listening","port":N,"token":"..."} for the parent.
func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.Int("port", 17332, "loopback port (busy → random free port)")
	token := fs.String("token", "", "API token (empty = auto-generate)")
	conn := fs.Int("conn", 0, "default connections")
	dir := fs.String("dir", "", "download dir")
	storePath := fs.String("store", defaultStorePath(), "bbolt path")
	_ = fs.Parse(args)

	a := openApp(*storePath, *dir, *conn)
	defer a.st.Close()

	// Startup repair pass: a server that died mid-download leaves Running
	// rows (mark them Paused so the UI can resume), and tasks persisted
	// before url normalisation existed carry bare/protocol-relative urls
	// that http.Get cannot fetch (scheme lost) — prefix https once, here.
	if ts, err := a.st.ListTasks(); err == nil {
		for i := range ts {
			dirty := false
			if u := model.NormalizeURL(ts[i].Req.URL); u != ts[i].Req.URL {
				ts[i].Req.URL = u
				dirty = true
			}
			if ts[i].Status == model.StatusRunning {
				// 上一进程被强杀/断电/重启系统留下的「下载中」：落到暂停并标成崩溃，
				// 这样下面的 AutoResume 才知道该捞它（M1.5 场景 B）。
				ts[i].Status = model.StatusPaused
				ts[i].PauseReason = model.PauseCrash
				ts[i].UpdatedAt = time.Now()
				dirty = true
			}
			if dirty {
				_ = a.st.PutTask(ts[i])
			}
		}
	}

	// 快照迁移说明（M1.5）：旧版本任务库被提升时，逐条告诉用户改了什么。
	for _, mg := range a.st.Migrations() {
		emit(map[string]any{"type": "migration", "taskId": mg.TaskID, "name": mg.Name, "note": mg.Note})
	}

	// M1.5 场景 B：重启系统 / 进程被强杀后自动接着下（用户手动暂停的绝不自动开跑）。
	if a.cfg.AutoResumeEnabled() {
		if n := a.mgr.AutoResume(context.Background()); n > 0 {
			emit(map[string]any{"type": "resumed", "count": n})
		}
	}

	tk := *token
	if tk == "" {
		tk = a.cfg.APIToken
		if tk == "" {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				fatal(err)
			}
			tk = hex.EncodeToString(b[:])
			a.cfg.APIToken = tk
			_ = a.st.SaveConfig(a.cfg)
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		// fixed port busy → fall back to a random free port
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatal(err)
		}
	}
	srv := &http.Server{Handler: server.New(a.disp, a.bus, tk)}
	go srv.Serve(ln)

	// IDM-style clipboard monitor for the whole life of the server.
	clipCtx, clipCancel := context.WithCancel(context.Background())
	defer clipCancel()
	go watcher.Clipboard(clipCtx, a.mgr)

	tp := ln.Addr().(*net.TCPAddr)
	emit(map[string]any{"type": "listening", "port": tp.Port, "token": tk})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	emit(map[string]any{"type": "stopping"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// ---- helpers --------------------------------------------------------------

func emit(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	_ = time.Now
}
