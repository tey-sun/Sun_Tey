package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/event"
	"github.com/tey-sun/suntey/core/internal/model"
	"github.com/tey-sun/suntey/core/internal/store"
)

// 断点续传矩阵（施工图 §7「续传矩阵」，M1.5 判据）的 Go 层。
//
// 分工：真·进程被杀（场景 A/E/F）需要真进程，放 scripts/test_resume_matrix.py；
// 本文件负责**必须在单元层锁死**的那几条——「先证伪再续传」的判定本身
// （场景 D 全部变体）、磁盘与计数器冲突时的夹紧（不跳字节）、以及
// 「完成改名绝不覆盖用户文件」。这些用真进程测反而测不准：时机不可控。
//
// 场景 B（重启整台机器）无法自动化，标 [U]；场景 G（磁力 piece 位图）由
// internal/download/bttx 的既有测试覆盖。

// ---- 可控内容源 -----------------------------------------------------------

// matrixSrc 是一个内容可换、身份头可关、并且会统计「一共又付了多少流量」的
// Range 服务器。续传判定的核心问题是「有没有白付流量」，所以流量计数器
// 和字节正确性一样是一等断言。
type matrixSrc struct {
	mu       sync.Mutex
	data     []byte
	etag     string
	lastMod  string
	capBytes int64 // >0 时：累计响应超过它就挂起新请求（确定性地「下到一半停住」）
	served   int64
	request  int
}

// matrixPattern 生成逐块变化的确定性内容（哈希流，不是常数填充）。
// 常数填充会让「源被替换成等长内容」的头部校验检不出来——那正是场景 D 要抓的。
func matrixPattern(seed byte, n int) []byte {
	out := make([]byte, 0, n)
	for i := 0; len(out) < n; i++ {
		h := sha256.New()
		h.Write([]byte{seed})
		fmt.Fprintf(h, "|%d", i)
		out = append(out, h.Sum(nil)...)
	}
	return out[:n]
}

func newMatrixSrc(t *testing.T, data []byte, etag, lastMod string) (*matrixSrc, *httptest.Server) {
	t.Helper()
	s := &matrixSrc{data: data, etag: etag, lastMod: lastMod}
	srv := httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *matrixSrc) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data, etag, lm := s.data, s.etag, s.lastMod
	s.request++
	s.mu.Unlock()

	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	if lm != "" {
		w.Header().Set("Last-Modified", lm)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	body := data
	if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
		a, b := parseRangeTest(strings.TrimPrefix(rng, "bytes="))
		if a >= int64(len(data)) || a < 0 {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if b < 0 || b >= int64(len(data)) {
			b = int64(len(data)) - 1
		}
		if a > b {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body = data[a : b+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(data)))
		w.WriteHeader(http.StatusPartialContent)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))

	if !s.reserve(r.Context(), int64(len(body))) {
		return // 客户端已取消（暂停）
	}
	_, _ = w.Write(body)
}

func (s *matrixSrc) addServed(n int64) {
	s.mu.Lock()
	s.served += n
	s.mu.Unlock()
}

// servedNow 返回累计响应字节（同时可作为「清零」用的基线）。
func (s *matrixSrc) servedNow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served
}

func (s *matrixSrc) swap(data []byte) {
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
}

func (s *matrixSrc) setIdentity(etag, lastMod string) {
	s.mu.Lock()
	s.etag, s.lastMod = etag, lastMod
	s.mu.Unlock()
}

// capAt 设字节闸门：累计响应超过 n 之后，新请求全部挂起（不是报错，是"卡住"）。
// 用闸门而不是定时器做「下到一半」，用例才不会偶发——暂停时机由测试说了算。
func (s *matrixSrc) capAt(n int64) {
	s.mu.Lock()
	s.capBytes = n
	s.mu.Unlock()
}

func (s *matrixSrc) uncap() {
	s.mu.Lock()
	s.capBytes = 0
	s.mu.Unlock()
}

// reserve 原子地预扣本次响应的字节数；闸门关着就等，客户端取消就返回 false。
func (s *matrixSrc) reserve(ctx context.Context, n int64) bool {
	for {
		s.mu.Lock()
		lim, served := s.capBytes, s.served
		if lim <= 0 || served+n <= lim {
			s.served += n
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		if !sleepCtx(ctx, 20*time.Millisecond) {
			return false
		}
	}
}

// ---- 公共夹具 -------------------------------------------------------------

func matrixManager(t *testing.T, dir string) *Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m := NewManager(st, event.NewBus(), model.Config{DownloadDir: dir, DefaultConn: 4})
	m.dl.PersistEvery = 200 * time.Millisecond
	// M1 的 aria2 规则只拆 >= 2*MinSplitSize 的文件；缩小它，让几百 KiB 的
	// 夹具真的走多段布局（否则测的是单连接顺序下载，chunk 夹紧路径根本没跑到）。
	m.dl.MinSplitSize, m.dl.SegmentFloor = 64<<10, 32<<10
	return m
}

// pauseWhileDownloading 起任务，等它真的下过 minBytes 后点暂停，返回暂停时的进度。
func pauseWhileDownloading(t *testing.T, m *Manager, id string, minBytes int64) int64 {
	t.Helper()
	if err := m.Start(id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Second)
	var last *model.Task
	for time.Now().Before(deadline) {
		cur, err := m.Store.GetTask(id)
		if err == nil {
			last = cur
			if cur.Status == model.StatusError {
				t.Fatalf("下载中意外失败：%s", cur.Err)
			}
			if cur.Downloaded >= minBytes {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if last == nil || last.Downloaded < minBytes {
		t.Fatalf("没能在超时内下过 %d 字节（当前 %+v）", minBytes, last)
	}
	m.Stop(id) // 用户点暂停：这也是「永不自动续传」的语义来源
	return waitTaskStatus(t, m, id, model.StatusPaused).Downloaded
}

func waitTaskStatus(t *testing.T, m *Manager, id string, want ...model.TaskStatus) *model.Task {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := m.Store.GetTask(id)
		if err == nil {
			for _, w := range want {
				if cur.Status == w {
					return cur
				}
			}
			if cur.Status == model.StatusError && !containsStatus(want, model.StatusError) {
				t.Fatalf("任务进入错误态（期望 %v）：%s", want, cur.Err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cur, _ := m.Store.GetTask(id)
	t.Fatalf("等待状态 %v 超时，当前 %+v", want, cur)
	return nil
}

func containsStatus(list []model.TaskStatus, s model.TaskStatus) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func shaOf(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// ---- 场景 D：源/盘任一变了就拒绝续传 --------------------------------------

// TestMatrixDSizeChangedRefusesResume（场景 D 变体 1）：续传前服务器说文件变大了。
// 必须（1）拒绝续传并给出可操作提示，（2）一个字节都不许多下——拒绝要免费，
// （3）磁盘上已有的半截文件必须原样保留（施工图禁止「静默清零重下」）。
func TestMatrixDSizeChangedRefusesResume(t *testing.T) {
	orig := matrixPattern('A', 512<<10)
	src, srv := newMatrixSrc(t, orig, `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")
	src.capAt(160 << 10) // 停在 160 KiB：够下过断点，又绝跑不完

	dir := t.TempDir()
	m := matrixManager(t, dir)
	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/d.bin", Filename: "d.bin"})
	if err != nil {
		t.Fatal(err)
	}
	pauseWhileDownloading(t, m, task.ID, 96<<10)
	src.uncap() // 续传阶段的探测/校验必须能走网络

	partials := findPartials(t, dir)
	if len(partials) != 1 {
		t.Fatalf("期望恰好一个 %s 中间文件，实际 %v", model.PartialSuffix, partials)
	}
	untouched := shaOf(t, partials[0])

	src.swap(matrixPattern('A', 700<<10)) // 同一个源，长度变了

	base := src.servedNow()
	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}
	cur := waitTaskStatus(t, m, task.ID, model.StatusError)

	if !strings.Contains(cur.Err, "已变更") {
		t.Fatalf("提示必须说清是源变了并给出动作，实际：%q", cur.Err)
	}
	if delta := src.servedNow() - base; delta > 4096 {
		t.Fatalf("拒绝续传不该付流量，实付 %d 字节", delta)
	}
	if shaOf(t, partials[0]) != untouched {
		t.Fatal("拒绝续传时改了磁盘文件：用户已下载的字节必须原样保留")
	}
	if cur.ResumeNote == "" {
		t.Fatal("错误原因必须同时落到 resumeNote，任务卡片才显示得出人话")
	}
}

// TestMatrixDSameSizeSwapRefusesResume（场景 D 变体 2）：等长替换 + 服务器什么身份头都不给。
// 这是最阴的一种——size 没变、没有 ETag/Last-Modified，弱信号全瞎。
// 必须靠「本地已下载头部 vs 服务器当前头部」的强校验拦住，否则两份内容
// 拼成一个文件，静默损坏，比下不动糟糕得多。
func TestMatrixDSameSizeSwapRefusesResume(t *testing.T) {
	src, srv := newMatrixSrc(t, matrixPattern('B', 400<<10), "", "") // 故意不给弱身份
	src.capAt(160 << 10)

	dir := t.TempDir()
	m := matrixManager(t, dir)
	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/swap.bin", Filename: "swap.bin"})
	if err != nil {
		t.Fatal(err)
	}
	pauseWhileDownloading(t, m, task.ID, 64<<10)
	src.uncap() // 校验要花 1 字节探测 + 头部窗口，闸门必须开

	cur, err := m.Store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	src.swap(matrixPattern('Z', 400<<10)) // 等长、内容完全不同的另一份文件

	_, err = m.verifyResume(t.Context(), cur)
	var re *ResumeError
	if !errors.As(err, &re) {
		t.Fatalf("等长替换必须判为不可续传，实际 err=%v", err)
	}
	if !strings.Contains(re.Reason, "替换") {
		t.Fatalf("提示应说明源已被替换，实际：%q", re.Reason)
	}
}

// TestMatrixDLocalFileRewrittenRefusesResume（场景 D 变体 3）：服务器没变，
// 本地半截文件被别的程序改写了（播放器/字幕工具/云盘同步都是真实来源）。
// 顺带锁死一条成本要求：弱身份齐备时，续传校验**不许**付那 256 KiB 窗口流量。
func TestMatrixDLocalFileRewrittenRefusesResume(t *testing.T) {
	data := matrixPattern('C', 400<<10)
	src, srv := newMatrixSrc(t, data, `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")
	src.capAt(192 << 10)

	dir := t.TempDir()
	m := matrixManager(t, dir)
	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/local.bin", Filename: "local.bin"})
	if err != nil {
		t.Fatal(err)
	}
	pauseWhileDownloading(t, m, task.ID, 96<<10)
	src.uncap()

	cur, err := m.Store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	base := src.servedNow()
	if _, err := m.verifyResume(t.Context(), cur); err != nil {
		t.Fatalf("源没变就该放行，实际拒绝：%v", err)
	}
	if delta := src.servedNow() - base; delta > 1 {
		t.Fatalf("ETag/Last-Modified 齐备时校验必须免费（只花 1 字节探测），实付 %d 字节", delta)
	}
	if cur.Identity == nil || cur.Identity.HeadSHA256 == "" {
		t.Fatal("首次放行必须登记头部指纹，否则以后本地被改坏再也查不出来")
	}
	if err := m.Store.PutTask(cur); err != nil { // 指纹要落盘，下次启动才有的比
		t.Fatal(err)
	}

	partials := findPartials(t, dir)
	rewriteAt(t, partials[0], 4096, []byte("另一个程序写的字节"))

	fresh, err := m.Store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.verifyResume(t.Context(), fresh)
	var re *ResumeError
	if !errors.As(err, &re) {
		t.Fatalf("本地文件被改写必须判为不可续传，实际 err=%v", err)
	}
	if !strings.Contains(re.Reason, "本地") {
		t.Fatalf("提示应说明是本地文件被改写（不是源变了），实际：%q", re.Reason)
	}
}

// ---- 磁盘比计数器少：夹紧补下，绝不跳字节 --------------------------------

// TestMatrixTruncatedFileNeverSkipsBytes（施工图 §6.1 约束 3）：进度计数器说
// 下了 128 KiB+，磁盘上的文件却被截到 40 KiB（复制一半就拔盘 / 云盘只落了个头 /
// 预分配的稀疏文件没持久化）。此时**计数器不是可信来源**：越过文件末尾的
// 「已完成字节」一律作废并补下，最终文件必须与源逐字节一致。
func TestMatrixTruncatedFileNeverSkipsBytes(t *testing.T) {
	data := matrixPattern('D', 400<<10)
	src, srv := newMatrixSrc(t, data, `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")
	src.capAt(256 << 10)

	dir := t.TempDir()
	m := matrixManager(t, dir)
	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/trunc.bin", Filename: "trunc.bin"})
	if err != nil {
		t.Fatal(err)
	}
	down := pauseWhileDownloading(t, m, task.ID, 128<<10)
	src.uncap()

	partials := findPartials(t, dir)
	keep := down / 3 // 只留三分之一在盘上：其余「已完成字节」全是计数器的谎话
	if keep <= 0 {
		t.Fatalf("夹具没造出可截断的进度：down=%d", down)
	}
	if err := os.Truncate(partials[0], keep); err != nil {
		t.Fatal(err)
	}

	cur, err := m.Store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Downloaded <= keep {
		t.Fatalf("夹具没造出「计数器比文件大」的谎话：downloaded=%d file=%d", cur.Downloaded, keep)
	}
	note, err := m.verifyResume(t.Context(), cur)
	if err != nil {
		t.Fatalf("文件被截断不是源变更，不该拒绝续传：%v", err)
	}
	if cur.Downloaded > keep {
		t.Fatalf("夹紧没生效：计数器仍说下了 %d，文件只有 %d", cur.Downloaded, keep)
	}
	if !strings.Contains(note, "重新下载") {
		t.Fatalf("放弃的进度必须写成人话（禁止静默清零）：%q", note)
	}
	if err := m.Store.PutTask(cur); err != nil {
		t.Fatal(err)
	}

	base := src.servedNow()
	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}
	done := waitTaskStatus(t, m, task.ID, model.StatusComplete)

	if shaOf(t, done.SavePath) != shaHex(data) {
		t.Fatal("截断后续传出来的文件与源不一致：被跳过的字节留下了洞")
	}
	// 只该补下被截掉的那部分（外加探测/窗口的零头），不得整文件重下。
	if delta, need := src.servedNow()-base, int64(len(data))-keep+(384<<10); delta > need {
		t.Fatalf("续传付了 %d 字节，上限 %d 字节（截断点到文件尾）", delta, need)
	}
	if done.Downloaded != int64(len(data)) {
		t.Fatalf("完成态进度应等于文件大小：%d != %d", done.Downloaded, len(data))
	}
}

// ---- 中间文件不见了 / 改名不覆盖 / 自动续传白名单 ------------------------

// TestMatrixPartialMissingZeroesProgress：计数器说有进度、磁盘上一个字节都没有
// （用户在下载目录里手动删了半截文件）。这不是「源变更」，不该报错停住任务，
// 而是清零放行 + 写清楚为什么，让用户一眼看懂「这次是从头下，不是进度丢了」。
func TestMatrixPartialMissingZeroesProgress(t *testing.T) {
	dir := t.TempDir()
	_, srv := newMatrixSrc(t, matrixPattern('G', 512<<10), `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")
	m := matrixManager(t, dir)

	const size = 512 << 10
	var chunks []model.ChunkState
	for i := 0; i < 4; i++ {
		start := int64(i * size / 4)
		chunks = append(chunks, model.ChunkState{
			Index: i, Start: start, End: start + int64(size/4) - 1, Downloaded: size / 8,
		})
	}
	save := filepath.Join(dir, "gone.bin")
	task := &model.Task{
		ID: newID(), Req: model.Request{URL: srv.URL + "/gone.bin", Filename: "gone.bin"},
		Status: model.StatusPaused, PauseReason: model.PauseCrash,
		Name: "gone.bin", SavePath: save, TempPath: save + model.PartialSuffix,
		Size: size, SupportRange: true, Conn: 4,
		Downloaded: size / 2, Chunks: chunks,
		Identity:    model.FromProbe(size, "Tue, 01 Nov 2022 00:00:00 GMT", `"v1"`),
		SnapVersion: model.SnapshotVersion,
	}
	if err := m.Store.PutTask(task); err != nil {
		t.Fatal(err)
	}

	cur, err := m.Store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	note, err := m.verifyResume(t.Context(), cur)
	if err != nil {
		t.Fatalf("中间文件不见了不该判不可续传（源没变），实际：%v", err)
	}
	if cur.Downloaded != 0 || len(cur.Chunks) != 0 {
		t.Fatalf("磁盘上没有文件却保留了进度：%d / %v", cur.Downloaded, cur.Chunks)
	}
	if !strings.Contains(note, "从头") {
		t.Fatalf("必须说明为什么这次从头下，避免用户以为进度被吞：%q", note)
	}
}

// TestMatrixFinishNeverOverwritesUserFile（§6.1 约束 4）：下载期间最终名上出现了
// 用户自己的文件。完成时改名必须让路（加序号），**绝不覆盖**——用户的一个文件
// 比我们的文件名整齐重要得多。
func TestMatrixFinishNeverOverwritesUserFile(t *testing.T) {
	data := matrixPattern('E', 200<<10)
	_, srv := newMatrixSrc(t, data, `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")

	dir := t.TempDir()
	m := matrixManager(t, dir)
	task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/final.bin", Filename: "final.bin"})
	if err != nil {
		t.Fatal(err)
	}
	// 建任务之后才出现的同名文件（另一个程序刚写完 / 用户自己拷进来的）。
	// Create 时的 uniquePath 让过一次，不代表这一刻还让得开。
	user := []byte("这是用户自己的文件，任何情况下都不许被下载改名覆盖")
	if err := os.WriteFile(task.SavePath, user, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := m.Start(task.ID); err != nil {
		t.Fatal(err)
	}
	done := waitTaskStatus(t, m, task.ID, model.StatusComplete)

	if got, err := os.ReadFile(task.SavePath); err != nil || !bytes.Equal(got, user) {
		t.Fatalf("用户已有文件被覆盖了：%v / %q", err, got)
	}
	if shaOf(t, done.SavePath) != shaHex(data) {
		t.Fatal("下载产物字节不正确")
	}
	if done.SavePath == task.SavePath {
		t.Fatal("任务记录的 SavePath 必须指向真实产物")
	}
	if !strings.Contains(done.SavePath, " (1)") {
		t.Fatalf("目标被占用应自动加序号，实际落到 %s", done.SavePath)
	}
	if done.Name != filepath.Base(done.SavePath) {
		t.Fatalf("显示名要跟着真实文件名走：%s vs %s", done.Name, done.SavePath)
	}
	if leftovers := findPartials(t, dir); len(leftovers) != 0 {
		t.Fatalf("完成后不该留下中间文件：%v", leftovers)
	}
}

// TestMatrixAutoResumeSkipsUserPaused（场景 B 的判定侧）：重启后自动续传只捞
// 「系统替他停的」（关机/崩溃/降级），用户点过暂停的永远留在原地。
// 自动开跑一批用户明令暂停的任务，等于软件替用户做主——IDM/迅雷都不这么干。
func TestMatrixAutoResumeSkipsUserPaused(t *testing.T) {
	const size = 4 << 20
	src, srv := newMatrixSrc(t, matrixPattern('H', size), `"v1"`, "Tue, 01 Nov 2022 00:00:00 GMT")
	src.capAt(size) // 四个任务共享同一道闸门：谁都跑不完，断言时仍在下

	dir := t.TempDir()
	m := matrixManager(t, dir)

	rows := []struct {
		name   string
		status model.TaskStatus
		reason model.PauseReason
	}{
		{"user.bin", model.StatusPaused, model.PauseUser},         // 用户点的：不许自动开
		{"shutdown.bin", model.StatusPaused, model.PauseShutdown}, // 关机前替他停的：该捞
		{"crash.bin", model.StatusPaused, model.PauseCrash},       // 强杀/断电遗留：该捞
		{"done.bin", model.StatusComplete, model.PauseNone},       // 已完成：无事可做
	}
	for i := range rows {
		task, err := m.Create(t.Context(), model.Request{URL: srv.URL + "/" + rows[i].name, Filename: rows[i].name})
		if err != nil {
			t.Fatal(err)
		}
		cur, err := m.Store.GetTask(task.ID)
		if err != nil {
			t.Fatal(err)
		}
		cur.Status = rows[i].status
		cur.PauseReason = rows[i].reason
		if rows[i].status == model.StatusComplete {
			cur.Downloaded = size
		}
		if err := m.Store.PutTask(cur); err != nil {
			t.Fatal(err)
		}
		rows[i].name = task.ID
	}

	if n := m.AutoResume(t.Context()); n != 2 {
		t.Fatalf("只该自动续传 shutdown/crash 两个，实际启动了 %d", n)
	}

	user, err := m.Store.GetTask(rows[0].name)
	if err != nil {
		t.Fatal(err)
	}
	if user.Status != model.StatusPaused || user.PauseReason != model.PauseUser {
		t.Fatalf("用户暂停的任务被自动开跑了：%s/%s", user.Status, user.PauseReason)
	}
	if user.Downloaded != 0 {
		t.Fatalf("用户暂停的任务一个字节都不该下，实际 %d", user.Downloaded)
	}
	done, err := m.Store.GetTask(rows[3].name)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != model.StatusComplete {
		t.Fatalf("已完成的任务不该被动：现在 %s", done.Status)
	}

	// 收尾：先开闸（关着的闸门会让连接永远挂着，httptest 关不掉），再优雅停机，
	// 然后等两行落到终态并给 defer 一点时间释放文件句柄——否则 TempDir 删不掉，
	// 报「file is being used by another process」，那是用例没收尾干净，不是产品问题。
	src.uncap()
	m.GracefulShutdown()
	for _, id := range []string{rows[1].name, rows[2].name} {
		waitTaskStatus(t, m, id, model.StatusPaused, model.StatusComplete)
	}
	time.Sleep(300 * time.Millisecond)
}

// ---- 夹具内共用的小工具 ---------------------------------------------------

// findPartials 列出目录里所有 .suntey-partial 中间文件（绝对路径，已排序）。
func findPartials(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(p, model.PartialSuffix) {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// rewriteAt 从文件中部改写若干字节——「文件被别的程序动过」的最小复现。
func rewriteAt(t *testing.T, path string, off int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func shaHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
