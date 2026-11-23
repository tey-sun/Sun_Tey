package download

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/httpx"
	"github.com/tey-sun/suntey/core/internal/model"
)

// 断点续传的续传侧（施工图 §6.1 / R1.5 → M1.5）。
//
// 一句话：**先证伪，再续传**。磁盘上有多少字节不算数，服务器说它没变也不算数，
// 只有「布局自洽 + 文件实测 + 身份一致」三条都过了，才允许在旧字节后面继续写。

// ResumeError 表示「不能续传」，且原因必须是人话 + 可操作动作。
// 任务停在错误态（不会被自动续传反复撞同一次失败），由用户决定重下还是换源。
type ResumeError struct {
	Reason string // 已包含建议动作
}

func (e *ResumeError) Error() string { return e.Reason }

// verifyResume 在续传写盘之前核对磁盘与服务器；返回的 note 是给人看的说明
// （可为空），err != nil 表示必须拒绝续传。
//
// 校验顺序刻意从便宜到昂贵：
//  1. 中间文件在不在（不在就没什么可续的，也不算丢数据）；
//  2. 持久化的分块布局是否自洽（布局坏了继续写 = 静默损坏）；
//  3. Stat 实测文件大小，夹紧「已完成字节」这个不可信计数器（§6.1 约束 3）；
//  4. 源文件身份：ETag / Last-Modified（免费）→ 前 1 MiB SHA-256（仅在弱信号
//     不足或需要复核本地文件时才付这 1 MiB 流量）。
func (m *Manager) verifyResume(ctx context.Context, t *model.Task) (string, error) {
	work := t.WorkPath()
	if work == "" {
		return "", nil
	}
	recorded := t.Downloaded
	if len(t.Chunks) > 0 {
		recorded = sumDownloaded(t.Chunks)
	}

	st, statErr := os.Stat(work)
	if statErr != nil || st.Size() == 0 {
		if recorded <= 0 {
			return "", nil // 全新任务：没有进度需要相信
		}
		// 计数器说有进度、磁盘上却没有一个字节。清零不是「重下已有字节」，
		// 而是拒绝相信谎话；文件本来也不存在，谈不上覆盖。
		t.Chunks = nil
		t.Downloaded = 0
		t.ResumeNote = "上次下载的中间文件已不存在（被移动或删除），进度已归零，将从头开始下载。"
		return t.ResumeNote, nil
	}

	// 2) 布局自洽：不自洽就保留文件、停下来说清楚，绝不按坏布局写盘。
	if err := model.ValidateChunkLayout(t.Chunks, t.Size); err != nil {
		return "", &ResumeError{Reason: fmt.Sprintf(
			"进度记录与文件布局对不上（%v）。已保留磁盘文件：删除本任务后重新下载最稳妥", err)}
	}

	// 3) Stat 实测：越过文件末尾的「已完成字节」一律作废。
	dropped := int64(0)
	if len(t.Chunks) > 0 {
		dropped = model.ClampChunksToFile(t.Chunks, st.Size())
		t.Downloaded = sumDownloaded(t.Chunks)
	} else if t.Downloaded > st.Size() {
		dropped = t.Downloaded - st.Size()
		t.Downloaded = st.Size()
	}
	note := ""
	if dropped > 0 {
		note = fmt.Sprintf("磁盘上比进度记录少了 %s（文件被截断或预分配未持久化），这部分将重新下载。", humanBytes(dropped))
		t.ResumeNote = note
	}

	// 4) 源文件身份（只在真的有进度要保护时才花流量）。
	if t.Downloaded <= 0 {
		return note, nil
	}
	res, err := httpx.Probe(ctx, m.Client, t.Req, m.Cfg.UserAgent)
	if err != nil {
		// 探测失败（网络抖动 / 临时 5xx）不该拒绝续传：弱身份拿不到就跳过，
		// 真正的字节校验仍在 Range 请求与完成后的哈希里。
		return note, nil
	}
	if res.Size > 0 && res.Size != t.Size {
		return "", &ResumeError{Reason: fmt.Sprintf(
			"服务器上的源文件已变更：大小从 %s 变成 %s，继续拼下去会得到坏文件。已停止续传：删除本任务后重新下载",
			humanBytes(t.Size), humanBytes(res.Size))}
	}
	now := model.FromProbe(res.Size, res.LastModified, res.ETag)
	if t.Identity == nil {
		t.Identity = now
	} else if reason := t.Identity.HeadersDiffer(now); reason != "" {
		return "", &ResumeError{Reason: reason +
			"。已停止续传（继续拼会损坏文件）：删除本任务后重新下载"}
	} else {
		// 只补记录，不推翻已有指纹：有些服务器首探没带 ETag、后面又带上了。
		if t.Identity.LastModified == "" {
			t.Identity.LastModified = now.LastModified
		}
		if t.Identity.ETag == "" {
			t.Identity.ETag = now.ETag
		}
	}

	// 强校验的成本规则（施工图 §6.1 约束 2）：
	//   · 读自己的盘免费 → 每次续传都用本地头部对照已登记的指纹；
	//   · 读服务器的窗口要付 min(窗口, 已下载) 流量 → 只有服务器**既没给 ETag
	//     也没给 Last-Modified** 时才付，且这时候它是唯一的源变更证据；
	//   · 有弱身份就信任它，指纹就地用本地头部登记，续传全程零额外流量。
	// 把窗口摊到每次续传上，就是 gopeed 一类「一动就慢、越用越像重下」的成因；
	// 用户不会容忍慢，只会直接关掉续传，最后连数据正确性一起失去。
	head := int64(model.HeadVerifyBytes)
	if st.Size() < head {
		head = st.Size()
	}
	if t.Downloaded < head {
		head = t.Downloaded
	}
	if head <= 0 {
		return note, nil
	}
	local, localN, lerr := httpx.LocalHeadHash(work, head)
	if lerr != nil {
		return note, nil // 读不动本地文件（占用/权限）：不因此拒绝续传
	}
	// 弱身份只要有一个标准变更检测头就算数（两者都来自文件元数据）。
	weak := t.Identity != nil && (t.Identity.ETag != "" || t.Identity.LastModified != "")
	haveFP := t.Identity.HeadSHA256 != ""

	// 免费的那一半：磁盘上的头部必须等于我们登记过的那一份。
	if haveFP && t.Identity.HeadSize == localN && local != t.Identity.HeadSHA256 {
		return "", &ResumeError{Reason: "本地已下载的文件内容与下载时不一致（前 1 MiB 校验失败），" +
			"文件可能已被其他程序改写。已停止续传：删除本任务后重新下载"}
	}

	// 要付费的那一半：没有弱身份时，服务器头部是唯一的源变更证据，只能每次付。
	if !weak {
		remote, remoteN, rerr := m.dl.HeadHash(ctx, t.Req, localN)
		switch {
		case rerr != nil:
			// 服务器不给头部：没有证据就继续（别把网络抖动变成续传失败）。
			return note, nil
		case remoteN != localN:
			// 两边长度不同（服务器变短了）：长度差本身就是源变更证据。
			return "", &ResumeError{Reason: fmt.Sprintf(
				"服务器上的文件比本地可校验窗口更短（%d < %d 字节），文件已变更。已停止续传：删除本任务后重新下载",
				remoteN, localN)}
		case remote != local:
			return "", &ResumeError{Reason: "服务器上的文件已被替换（前 1 MiB 内容校验不同），" +
				"继续续传会把两份不同内容拼成一个坏文件。已停止续传：删除本任务后重新下载"}
		}
		if !haveFP {
			t.Identity.HeadSHA256 = remote
			t.Identity.HeadSize = remoteN
		}
		return note, nil
	}

	// 有弱身份且还没建指纹：读盘免费，直接登记本地头部。之后每次续传都免费
	// 复核本地文件，源变更仍由 ETag / Last-Modified 拦住。
	// 已知边界：等长替换 + 服务器复用同一个 ETag 时检不出来（施工图认这个账，
	// M8 高级设置里给一个「续传强校验（每次付窗口）」开关，默认关）。
	if !haveFP {
		t.Identity.HeadSHA256 = local
		t.Identity.HeadSize = localN
	}
	return note, nil
}

// finishWorkFile 把中间文件原子改名为最终文件（§6.1 约束 4）。
// 目标名已被占用时自动加序号——绝不覆盖用户已有文件。
func (m *Manager) finishWorkFile(t *model.Task) error {
	if t.TempPath == "" || t.TempPath == t.SavePath {
		return nil
	}
	final, err := freePath(t.SavePath)
	if err != nil {
		return err
	}
	if err := os.Rename(t.TempPath, final); err != nil {
		return fmt.Errorf("下载已完成，但改名失败（文件可能被占用）：%w", err)
	}
	t.SavePath = final
	t.Name = baseName(final)
	t.TempPath = ""
	return nil
}

// AutoResume 把「不是用户主动暂停」的任务重新捞起来跑（M1.5 场景 B：
// 重启系统 / 进程被强杀 / 关机前自动暂停）。用户点了暂停的任务永不自动开跑。
// 返回真正启动的任务数；并发打满时剩下的留在原地（下次启动再捞）。
func (m *Manager) AutoResume(ctx context.Context) int {
	ts, err := m.Store.ListTasks()
	if err != nil {
		return 0
	}
	started := 0
	for _, t := range ts {
		if t == nil || !t.AutoResumeAllowed() {
			continue
		}
		m.mu.Lock()
		_, running := m.running[t.ID]
		m.mu.Unlock()
		if running {
			continue
		}
		if err := m.Start(t.ID); err != nil {
			// 并发已满或存储异常：剩下的任务保持暂停，不排队轰炸服务器。
			continue
		}
		started++
	}
	if started > 0 {
		m.Bus.Publish(model.Event{
			Type:  "resume",
			Level: "info",
			Err:   fmt.Sprintf("已自动续传 %d 个未完成任务", started),
			TS:    time.Now().UnixMilli(),
		})
	}
	return started
}

// freePath 返回一个肯定不存在的路径：被占用就在扩展名前加 " (n)"（与 uniquePath 同规则）。
//
// 序号必须加在**同一个目录**里。早期实现只拼文件名没拼目录：跨盘时 Windows 直接报
// 「cannot move the file to a different disk drive」，把已经下完的任务判成错误；
// 同盘时更糟——改名真的成功，文件却落进了进程工作目录，用户在下载目录里
// 找不到刚下完的东西。M1.5 续传矩阵的「不覆盖用户已有文件」用例抓到了它。
func freePath(p string) (string, error) {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return p, nil
	}
	dir := filepath.Dir(p)
	stem := stemOf(p)
	ext := extOf(p)
	for i := 1; ; i++ {
		cand := filepath.Join(dir, fmt.Sprintf("%s (%d)%s", stem, i, ext))
		if _, err := os.Stat(cand); os.IsNotExist(err) {
			return cand, nil
		}
	}
}

// ---- 小工具（展示与改名用；规则与 uniquePath 保持一致） -------------------

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func baseName(p string) string { return filepath.Base(p) }

func stemOf(p string) string {
	base := filepath.Base(p)
	if i := lastDot(base); i > 0 {
		return base[:i]
	}
	return base
}

func extOf(p string) string {
	base := filepath.Base(p)
	if i := lastDot(base); i > 0 {
		return base[i:]
	}
	return ""
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}

// takeReason 取走（并清除）run() 收尾时要写的暂停原因。没记录就当用户点的：
// 默认必须是「不自动续传」，否则误重启会替用户开始一堆下载。
func (m *Manager) takeReason(id string) model.PauseReason {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.stopReasons[id]; ok {
		delete(m.stopReasons, id)
		if r != model.PauseNone {
			return r
		}
	}
	return model.PauseUser
}

// sleepCtx 可被 ctx 打断的等待；返回 false 表示被打断。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
