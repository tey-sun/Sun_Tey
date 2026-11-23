package bttx

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"github.com/anacrolix/torrent/bencode"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"

	"github.com/tey-sun/suntey/core/internal/model"
)

// makeTorrent writes a single-file .torrent (pieceLen pieces) plus its data
// file into dir. Returns the .torrent path and the display name. No network.
func makeTorrent(t *testing.T, dir string, data []byte, pieceLen int64) (metaPath string, name string) {
	t.Helper()
	name = "sample.bin"
	pieces, err := metainfo.GeneratePieces(bytes.NewReader(data), pieceLen, nil)
	if err != nil {
		t.Fatalf("GeneratePieces: %v", err)
	}
	info := metainfo.Info{
		PieceLength: pieceLen,
		Pieces:      pieces,
		Name:        name,
		Length:      int64(len(data)),
	}
	mi := metainfo.MetaInfo{InfoBytes: infoBytes(t, info), Announce: ""}
	metaPath = filepath.Join(dir, name+".torrent")
	f, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("create meta: %v", err)
	}
	if err := mi.Write(f); err != nil {
		f.Close()
		t.Fatalf("write meta: %v", err)
	}
	f.Close()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatalf("write data: %v", err)
	}
	return metaPath, name
}

func encodeBencode(v interface{}) ([]byte, error) { return bencode.Marshal(v) }

func infoBytes(t *testing.T, info metainfo.Info) []byte {
	t.Helper()
	b, err := encodeBencode(&info)
	if err != nil {
		t.Fatalf("encode info: %v", err)
	}
	return b
}

// seedClient starts a pure-anacrolix seeder holding every piece of the torrent
// stored under seedDir, listening on a random port. Returns client + "ip:port".
func seedClient(t *testing.T, seedDir, metaPath string) (*torrent.Client, string) {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisablePEX = true
	cfg.NoDefaultPortForwarding = true
	cfg.DisableIPv6 = true
	cfg.DisableWebtorrent = true
	cfg.DisableWebseeds = true
	cfg.AcceptPeerConnections = true
	cfg.Seed = true            // complete seeder must upload pieces; uploadAllowed gates on seeding()
	cfg.DisableUTP = true      // match production: uTP EOFs on Windows loopback
	cfg.AlwaysWantConns = true // completed seeder must still accept the engine's incoming dial
	cfg.EstablishedConnsPerTorrent = 4
	cfg.DataDir = seedDir
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatalf("seed client: %v", err)
	}
	mi, err := readMeta(metaPath)
	if err != nil {
		cl.Close()
		t.Fatalf("read meta: %v", err)
	}
	spec, err := torrent.TorrentSpecFromMetaInfoErr(mi)
	if err != nil {
		cl.Close()
		t.Fatalf("spec: %v", err)
	}
	// Same file-mode storage flavor the engine uses (NewFileOpts avoids the
	// mmap path that fails CreateFileMapping on this host).
	spec.Storage = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir: seedDir,
		FilePathMaker: func(o storage.FilePathMakerOpts) string {
			if p := o.File.BestPath(); len(p) > 0 {
				return filepath.Join(p...)
			}
			return "" // single-file: the name is already carried by TorrentDirMaker
		},
		TorrentDirMaker: func(baseDir string, info *metainfo.Info, hash metainfo.Hash) string {
			// v1.61 single-file semantics: this return value IS the file path
			// (the library appends name only for multi-file sub-paths).
			if len(info.Files) == 0 {
				return filepath.Join(baseDir, info.BestName())
			}
			return baseDir
		},
		PieceCompletion: storage.NewMapPieceCompletion(),
	})
	tt, _, err := cl.AddTorrentSpec(spec)
	if err != nil {
		cl.Close()
		t.Fatalf("seed add: %v", err)
	}
	select {
	case <-tt.GotInfo():
	case <-time.After(10 * time.Second):
		cl.Close()
		t.Fatal("seed: no info")
	}
	tt.DownloadAll() // seeder reads every piece, then serves them
	return cl, "127.0.0.1:" + strconv.Itoa(cl.LocalPort())
}

func readMeta(p string) (*metainfo.MetaInfo, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return metainfo.Load(f)
}

func testEngine(t *testing.T, peers []string, baseDir string) *Engine {
	t.Helper()
	return New(Config{
		ListenPort:      0,
		MaxConns:        2,
		BaseDir:         baseDir,
		ExtraPeers:      peers,
		NoDHT:           true,
		DisablePEX:      true,
		Debug:           true, // anacrolix "updating requests" debug trail
		MetadataTimeout: 10 * time.Second,
	})
}

func TestBTDeliversTorrentFile(t *testing.T) {
	tmp := scratchTree(t)
	data := make([]byte, 300*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	metaPath, name := makeTorrent(t, tmp, data, 16384)
	seedDir := filepath.Join(tmp, "seed")
	if err := os.Mkdir(seedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(tmp, name), filepath.Join(seedDir, name)); err != nil {
		t.Fatalf("move data into seedDir: %v", err)
	}

	seed, addr := seedClient(t, seedDir, metaPath)
	defer seed.Close()
	if c, err := net.DialTimeout("tcp", addr, 2*time.Second); err == nil {
		c.Close()
	} else {
		t.Fatalf("seeder addr %s unreachable: %v", addr, err)
	}
	t.Logf("seeder at %s", addr)

	// The seeder must verify its own pieces (become complete) before it can
	// serve them; if it doesn't, the engine starves with haven-empty peer.
	seedT := seed.Torrents()[0]
	seedDeadline := time.Now().Add(15 * time.Second)
	for !seedT.Complete().Bool() && time.Now().Before(seedDeadline) {
		time.Sleep(500 * time.Millisecond)
	}
	sst := seedT.Stats()
	t.Logf("seeder complete=%v bytes=%d/%d piecesComplete=%d",
		seedT.Complete().Bool(), seedT.BytesCompleted(), seedT.Length(), sst.PiecesComplete)
	if !seedT.Complete().Bool() {
		t.Errorf("seeder never became complete (its file/storage/mapping is broken)")
	}

	dest := filepath.Join(tmp, "dest")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEngine(t, []string{addr}, dest)
	defer e.Close()

	// Windows mmap fileIo cannot map a brand-new zero-length file, which is
	// exactly what anacrolix opens on first download. Pre-create the target at
	// full length so the mapping succeeds (production fix mirrors this).
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	pre, perr := os.OpenFile(filepath.Join(dest, "sample.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	if perr != nil {
		t.Fatal(perr)
	}
	if terr := pre.Truncate(int64(len(data))); terr != nil {
		t.Logf("pre-truncate: %v", terr)
	}
	pre.Close()

	task := &model.Task{ID: "t-bt", Req: model.Request{URL: metaPath}}
	metaCalled := false
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		for {
			select {
			case <-stopWatch:
				return
			case <-time.After(5 * time.Second):
				t.Logf("stats: %+v", e.Stats())
			}
		}
	}()
	err := e.Run(ctx, task, &RunOptions{
		Emit:    func(model.Event) {},
		Persist: func(int64) {},
		OnMeta: func(nm string, size int64, files []model.FileEntry, savePath string) error {
			metaCalled = true
			if nm != name || size != int64(len(data)) {
				t.Errorf("meta mismatch: name=%q size=%d", nm, size)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !metaCalled {
		t.Fatal("OnMeta never fired")
	}
	got, err := os.ReadFile(task.SavePath)
	if err != nil {
		t.Fatalf("read result: %v (savePath=%q)", err, task.SavePath)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes mismatch: got %d want %d sha %x want %x",
			len(got), len(data), sha1.Sum(got), sha1.Sum(data))
	}
}

func TestBTResumeAfterCancel(t *testing.T) {
	tmp := scratchTree(t)
	data := make([]byte, 400*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	metaPath, name := makeTorrent(t, tmp, data, 16384)
	seedDir := filepath.Join(tmp, "seed")
	if err := os.Mkdir(seedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(tmp, name), filepath.Join(seedDir, name)); err != nil {
		t.Fatalf("move data into seedDir: %v", err)
	}

	seed, addr := seedClient(t, seedDir, metaPath)
	defer seed.Close()

	dest := filepath.Join(tmp, "dest")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEngine(t, []string{addr}, dest)
	defer e.Close()

	task := &model.Task{ID: "t-resume", Req: model.Request{URL: metaPath}}

	// phase 1: run until >100KB, then cancel (simulated pause)
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		first <- e.Run(ctx, task, &RunOptions{
			Emit: func(model.Event) {},
			Persist: func(dl int64) {
				task.Downloaded = dl
				if dl > 100*1024 {
					cancel()
				}
			},
			OnMeta: func(nm string, size int64, files []model.FileEntry, savePath string) error {
				task.SavePath = savePath
				return nil
			},
		})
	}()
	select {
	case <-first:
	case <-time.After(30 * time.Second):
		cancel()
		<-first
		t.Fatal("phase1 stuck")
	}
	cancel()
	partial := task.Downloaded
	if partial == 0 {
		t.Skip("no partial progress captured (too fast), resume untested")
	}

	// phase 2: rerun must finish with exact bytes (verify-from-disk resume)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel2()
	if err := e.Run(ctx2, task, &RunOptions{
		Emit:    func(model.Event) {},
		Persist: func(dl int64) { task.Downloaded = dl },
		OnMeta: func(nm string, size int64, files []model.FileEntry, savePath string) error {
			task.SavePath = savePath
			return nil
		},
	}); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	got, err := os.ReadFile(task.SavePath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("resumed bytes mismatch (partial was %d)", partial)
	}
}

func TestMagnetParseHelpers(t *testing.T) {
	m := "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=demo&tr=udp%3A%2F%2Ftracker"
	if !IsMagnetURL(m) {
		t.Fatal("magnet should be detected")
	}
	hexh, err := InfoHashFromMagnet(m)
	if err != nil || hexh != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("infohash parse: %q %v", hexh, err)
	}
	if IsMagnetURL("http://x/y.m3u8") {
		t.Fatal("m3u8 is not a magnet")
	}
	if !IsTorrentSource("C:\\x\\y.torrent") || !IsTorrentSource("https://x/y.torrent") {
		t.Fatal("torrent source detection broken")
	}
	if !IsBT(&model.Task{Req: model.Request{URL: m}}) {
		t.Fatal("IsBT must route magnets to bttx")
	}
	if IsBT(&model.Task{Req: model.Request{URL: "http://x/a.zip"}}) {
		t.Fatal("http task must not be BT")
	}
}

// fileTreeTorrent writes a multi-file torrent (16 KiB pieces, files named
// a.bin/b.bin/...) plus its populated seed tree, and returns the .torrent path,
// the seeder DataDir and the random payload of every file. Every per-file
// selection test asserts against this swarm; sizes are explicit because the
// pause/resume case needs files big enough to interrupt mid-transfer.
func fileTreeTorrent(t *testing.T, tmp, name string, sizes ...int64) (metaPath, seedDir string, payloads [][]byte) {
	t.Helper()
	names := []string{"a.bin", "b.bin", "c.bin", "d.bin"}
	if len(sizes) == 0 || len(sizes) > len(names) {
		t.Fatalf("unsupported file count %d", len(sizes))
	}
	var total int64
	payloads = make([][]byte, len(sizes))
	joined := make([]byte, 0)
	seedDir = filepath.Join(tmp, "seed")
	if err := os.Mkdir(seedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := make([]metainfo.FileInfo, 0, len(sizes))
	for i, sz := range sizes {
		payloads[i] = make([]byte, sz)
		if _, err := rand.Read(payloads[i]); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(seedDir, names[i]), payloads[i], 0o644); err != nil {
			t.Fatal(err)
		}
		files = append(files, metainfo.FileInfo{Path: []string{names[i]}, Length: sz})
		joined = append(joined, payloads[i]...)
		total += sz
	}
	pieces, err := metainfo.GeneratePieces(bytes.NewReader(joined), 16384, nil)
	if err != nil {
		t.Fatalf("GeneratePieces: %v", err)
	}
	info := metainfo.Info{
		Name:        name,
		PieceLength: 16384,
		Pieces:      pieces,
		Length:      total,
		Files:       files,
	}
	mi := metainfo.MetaInfo{InfoBytes: infoBytes(t, info), Announce: ""}
	metaPath = filepath.Join(tmp, name+".torrent")
	mf, err := os.Create(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := mi.Write(mf); err != nil {
		mf.Close()
		t.Fatal(err)
	}
	mf.Close()
	return metaPath, seedDir, payloads
}

// twoFileTorrent is the shared per-file-selection fixture: a.bin + b.bin,
// 96 KiB each — small enough that a whole run is seconds, big enough that the
// live-path (non-paused) switches are unambiguous.
func twoFileTorrent(t *testing.T, tmp, name string) (metaPath, seedDir string, a, b []byte) {
	t.Helper()
	metaPath, seedDir, payloads := fileTreeTorrent(t, tmp, name, 96*1024, 96*1024)
	return metaPath, seedDir, payloads[0], payloads[1]
}

// awaitSeedComplete fails fast when the seeder never loads every piece: a
// half-seeded swarm would make every selection assertion meaningless.
func awaitSeedComplete(t *testing.T, seed *torrent.Client) {
	t.Helper()
	seedT := seed.Torrents()[0]
	deadline := time.Now().Add(15 * time.Second)
	for !seedT.Complete().Bool() && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
	}
	if !seedT.Complete().Bool() {
		t.Fatal("seeder never became complete for the two-file fixture")
	}
}

// TestFileSelectionPartial proves the 迅雷-style per-file selection: a two-file
// torrent (a.bin + b.bin, 16KiB pieces) where only index 1 (b.bin) is selected.
// The engine must finish the task with b.bin complete and a.bin never fetched.
func TestFileSelectionPartial(t *testing.T) {
	tmp := scratchTree(t)
	metaPath, seedDir, _, b := twoFileTorrent(t, tmp, "multi")

	seed, addr := seedClient(t, seedDir, metaPath)
	defer seed.Close()
	awaitSeedComplete(t, seed)

	dest := filepath.Join(tmp, "dest")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEngine(t, []string{addr}, dest)
	defer e.Close()

	task := &model.Task{ID: "t-sel", Req: model.Request{URL: metaPath}, BTSel: "1"}
	metaSize := int64(-1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := e.Run(ctx, task, &RunOptions{
		Emit:    func(model.Event) {},
		Persist: func(int64) {},
		OnMeta: func(nm string, size int64, files []model.FileEntry, savePath string) error {
			metaSize = size
			if len(files) != 2 || files[1].Name != "b.bin" {
				t.Errorf("meta files mismatch: %+v", files)
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if metaSize != 96*1024 {
		t.Errorf("selected size via OnMeta = %d, want 96KiB", metaSize)
	}
	if task.Size != 96*1024 {
		t.Errorf("task.Size = %d, want 96KiB (selection must shrink it)", task.Size)
	}
	locate := func(name string) (int64, []byte) {
		var sz int64
		var data []byte
		_ = filepath.Walk(dest, func(p string, st os.FileInfo, err error) error {
			if err == nil && !st.IsDir() && st.Name() == name {
				sz = st.Size()
				data, _ = os.ReadFile(p)
			}
			return nil
		})
		return sz, data
	}
	if sz, data := locate("a.bin"); sz != 0 || len(data) != 0 {
		t.Errorf("unselected a.bin on disk: size=%d len=%d (must stay empty)", sz, len(data))
	}
	if sz, data := locate("b.bin"); sz != 96*1024 || !bytes.Equal(data, b) {
		t.Errorf("selected b.bin on disk: size=%d bytes-equal=%v", sz, bytes.Equal(data, b))
	}
}

// TestFileSelectionSwitch pins the live path: SetFileSelection on a running
// torrent re-lifts and drops pieces both ways (file priorities alone can
// never re-add pieces that were cancelled). Uses its own two-file fixture so
// it never reuses the single-file makeTorrent path.
func TestFileSelectionSwitch(t *testing.T) {
	tmp := scratchTree(t)
	metaPath, seedDir, a, b := twoFileTorrent(t, tmp, "switch")

	seed, addr := seedClient(t, seedDir, metaPath)
	defer seed.Close()
	awaitSeedComplete(t, seed)

	dest := filepath.Join(tmp, "dest")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEngine(t, []string{addr}, dest)
	defer e.Close()

	task := &model.Task{ID: "t-swap", Req: model.Request{URL: metaPath}, BTSel: "0"}
	live := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.Run(ctx, task, &RunOptions{
			Emit:    func(model.Event) {},
			Persist: func(int64) {},
			OnLive:  func() { close(live) },
		})
	}()
	<-live

	// Phase 1: a.bin gains progress under the initial selection; then switch
	// to b only, let the engine react, and re-select a as well. Both files
	// must finish afterwards — proof the switch dropped and re-added pieces.
	deadline := time.Now().Add(45 * time.Second)
	for {
		if sz, _ := fileAt(dest, "a.bin"); sz >= 16*1024 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a.bin never progressed under initial selection")
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("run died while waiting for a.bin: %v (last: %v)", err, <-errCh)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// No sleep between the two switches: b.bin is at 0 bytes until the first one
	// re-adds its pieces, so the task provably cannot complete in between, and
	// the second switch must land on the live torrent. A sleep here is what
	// used to let the b-only target finish first, turning a passing engine into
	// a flaky test (SetFileSelection then correctly reports ErrBTNotRunning).
	if err := e.SetFileSelection("t-swap", []int{1}); err != nil {
		t.Fatalf("SetFileSelection: %v", err)
	}
	if err := e.SetFileSelection("t-swap", []int{0, 1}); err != nil {
		t.Fatalf("SetFileSelection re-select: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run after switch: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("task never completed after selection switch")
	}

	if sz, data := fileAt(dest, "a.bin"); sz != 96*1024 || !bytes.Equal(data, a) {
		t.Errorf("a.bin after re-select: size=%d equal=%v", sz, bytes.Equal(data, a))
	}
	if sz, data := fileAt(dest, "b.bin"); sz != 96*1024 || !bytes.Equal(data, b) {
		t.Errorf("b.bin: size=%d equal=%v", sz, bytes.Equal(data, b))
	}
}

// fileAt finds name anywhere under root and returns its size and contents.
func fileAt(root, name string) (int64, []byte) {
	var sz int64
	var data []byte
	_ = filepath.Walk(root, func(p string, st os.FileInfo, err error) error {
		if err == nil && !st.IsDir() && st.Name() == name {
			sz = st.Size()
			data, _ = os.ReadFile(p)
		}
		return nil
	})
	return sz, data
}

// TestSelectionHelpers pins the selection string format and clamping used by
// manager.SetFileSelection and bttx.Run alike.
func TestSelectionHelpers(t *testing.T) {
	if ParseSelection("") != nil {
		t.Error(`ParseSelection("") must mean "all files"`)
	}
	sel := ParseSelection("2,0, 2 ,x")
	if !sel[0] || !sel[2] || len(sel) != 2 {
		t.Errorf("ParseSelection tolerant parse broke: %v", sel)
	}
	if got := JoinSelection([]int{3, 1, 3, 0}); got != "0,1,3" {
		t.Errorf("JoinSelection = %q, want 0,1,3", got)
	}
	if JoinSelection(nil) != "" {
		t.Error("JoinSelection(nil) must be empty (= all)")
	}
	files := []model.FileEntry{{Name: "a", Size: 10}, {Name: "b", Size: 20}, {Name: "c", Size: 30}}
	if got := SelectedSize(files, nil); got != 60 {
		t.Errorf("SelectedSize(all) = %d, want 60", got)
	}
	if got := SelectedSize(files, map[int]bool{2: true}); got != 30 {
		t.Errorf("SelectedSize({2}) = %d, want 30", got)
	}
	items := FileItems(files, map[int]bool{0: true, 2: true})
	if len(items) != 3 || !items[0].Selected || items[1].Selected || !items[2].Selected {
		t.Errorf("FileItems flags wrong: %+v", items)
	}
	if items[1].Index != 1 || items[1].Name != "b" || items[1].Size != 20 {
		t.Errorf("FileItems metadata wrong: %+v", items[1])
	}
}

// TestFileSelectionPauseResume pins M0 判据 (b)「任务暂停后改选 → 恢复后选择仍生效」。
//
// Construction (deliberately race-free): the seeder holds b.bin but its copy of
// a.bin is emptied, so an a-only task is live (peers connected) yet can NEVER
// finish. Pausing it and re-selecting to b.bin then has exactly one way out:
// the resumed run must honour the persisted selection. If Run ignored task.BTSel
// and fell back to "all files", a.bin's missing pieces would make the task hang
// until the test times out — the assertion below is therefore falsifiable
// without depending on how fast loopback BT happens to be.
//
// Both halves of the engine contract are asserted:
//  1. while the task is not live, SetFileSelection must answer ErrBTNotRunning
//     rather than pretend to apply — the manager persists the choice on the task;
//  2. Run registers ParseSelection(task.BTSel), so the resumed run recomputes the
//     finish line (task.Size == b.bin) and completes.
func TestFileSelectionPauseResume(t *testing.T) {
	tmp := scratchTree(t)
	metaPath, seedDir, payloads := fileTreeTorrent(t, tmp, "pauseresume", 96*1024, 96*1024)
	// Withhold a.bin from the seeder: a-only can connect but never complete.
	if err := os.WriteFile(filepath.Join(seedDir, "a.bin"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	seed, addr := seedClient(t, seedDir, metaPath)
	defer seed.Close()
	// Only b.bin is seedable here, so "complete" is never reached; wait for the
	// seeder to know its half instead.
	seedT := seed.Torrents()[0]
	deadline := time.Now().Add(15 * time.Second)
	for seedT.BytesCompleted() == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if seedT.BytesCompleted() == 0 {
		t.Fatal("seeder verified no pieces from the half-populated fixture")
	}

	dest := filepath.Join(tmp, "dest")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEngine(t, []string{addr}, dest)
	defer e.Close()

	// Phase 1 — a-only: must go live (a peer appears), then pause.
	task := &model.Task{ID: "t-pause", Req: model.Request{URL: metaPath}, BTSel: "0"}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	live := make(chan struct{})
	go func() {
		first <- e.Run(ctx, task, &RunOptions{
			Emit: func(ev model.Event) {
				if ev.Peers > 0 {
					select {
					case <-live:
					default:
						close(live)
					}
				}
			},
			Persist: func(int64) {},
		})
	}()
	select {
	case <-live:
	case err := <-first:
		cancel()
		t.Fatalf("phase 1 ended before going live: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("phase 1 never saw a peer (swarm broken; the whole test loses meaning)")
	}
	cancel()
	select {
	case err := <-first:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("phase 1 (pause) returned %v, want context.Canceled", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("phase 1 stuck after cancel")
	}
	// Re-selection during the paused window: no live torrent exists for this id,
	// so the engine must say so; the manager keeps the choice on the task.
	if err := e.SetFileSelection(task.ID, []int{1}); !errors.Is(err, ErrBTNotRunning) {
		t.Fatalf("SetFileSelection while paused = %v, want ErrBTNotRunning", err)
	}
	task.BTSel = "1" // exactly what manager.SetFileSelection persists

	// Phase 2 — resume. Only this selection can ever finish.
	metaSize := int64(0)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel2()
	second := make(chan error, 1)
	go func() {
		second <- e.Run(ctx2, task, &RunOptions{
			Emit:    func(model.Event) {},
			Persist: func(int64) {},
			OnMeta: func(_ string, size int64, _ []model.FileEntry, _ string) error {
				metaSize = size
				return nil
			},
		})
	}()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("resumed Run: %v", err)
		}
	case <-time.After(110 * time.Second):
		t.Fatal("resumed task never completed: the paused-window selection was not honoured")
	}

	if metaSize != 96*1024 {
		t.Errorf("OnMeta size = %d, want 96KiB (resumed run must honour BTSel)", metaSize)
	}
	if task.Size != 96*1024 {
		t.Errorf("task.Size = %d, want 96KiB", task.Size)
	}
	if sz, data := fileAt(dest, "b.bin"); sz != 96*1024 || !bytes.Equal(data, payloads[1]) {
		t.Errorf("b.bin after resume: size=%d bytes-equal=%v (want 96KiB/true)", sz, bytes.Equal(data, payloads[1]))
	}
}
