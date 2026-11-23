package model

import (
	"fmt"
	"sort"
)

// 续传契约（施工图 §6.1 / R11 / M1.5）。
//
// 三条硬约束在这里落地成可测的纯函数：
//  1. 快照带版本号，旧快照必须经迁移函数才能被采信；
//  2. 「已下载字节」永远不是可信来源——落盘真相由 Stat 实测（见 httpx/manager 的校验）；
//  3. 中间文件与最终文件分离，完成时原子改名，绝不覆盖用户已有文件。
const (
	// SnapshotVersion 是续传快照的 schema 版本。
	// 0 = M1.5 之前写入的旧快照，读入时必须走 store.migrateTask。
	SnapshotVersion = 1

	// PartialSuffix 是「下载中」中间文件后缀。完成后原子改名为最终名，
	// 因此任何以此结尾的文件必然属于未完成任务：崩溃/重启后一眼可辨，
	// 也不会让别的程序误读半截文件，更不会覆盖用户已有的同名文件。
	PartialSuffix = ".suntey-partial"

	// HeadVerifyBytes 是续传强校验的头部窗口大小。
	//
	// 校验本地文件是**免费**的（读自己的盘）；校验服务器要付 min(窗口, 已下载)
	// 的流量，所以：服务器给了 ETag 或 Last-Modified 就信任它（绝大多数 CDN 都有），
	// 两个头都没有时才付这个窗口。256 KiB 是把「源文件被替换」的检出率
	// 和「每次续传的固定开销」放在一起权衡后的取值。
	HeadVerifyBytes int64 = 256 << 10
)

// PauseReason 说明任务为什么停在暂停态：用户点的，还是系统停的。
// 只有 PauseUser 之外的原因才会被开机/重启后自动续传（施工图 M1.5 场景 B）。
type PauseReason string

const (
	PauseNone      PauseReason = ""
	PauseUser      PauseReason = "user"      // 用户点了暂停：永不自动续传
	PauseShutdown  PauseReason = "shutdown"  // 退出程序时优雅暂停
	PauseCrash     PauseReason = "crash"     // 进程被强杀/断电后遗留
	PauseDowngrade PauseReason = "downgrade" // 服务器不守 Range，自动降级后重开
	// PauseInvalid 表示进度记录无法安全采信（例如旧版本快照迁移失败）：
	// 文件保留，但必须等用户点「开始」，绝不自动重下。
	PauseInvalid PauseReason = "invalid"
)

// FileIdentity 是源文件的身份指纹：任一项变了即判定「不可续」。
//
// 弱信号（size / Last-Modified / ETag）来自探测响应头，零成本；
// 强信号（HeadSHA256）是已落盘头部窗口的 SHA-256，续传前用 1 MiB 的代价
// 同时校验「服务器文件」与「本地文件」，两者都必须等于它。
type FileIdentity struct {
	Size         int64  `json:"size,omitempty"`
	LastModified string `json:"lastModified,omitempty"`
	ETag         string `json:"etag,omitempty"`
	HeadSHA256   string `json:"headSha256,omitempty"` // 十六进制小写
	HeadSize     int64  `json:"headSize,omitempty"`   // 参与哈希的字节数
}

// FromProbe 把探测结果折叠成身份指纹（只保留有值的字段）。
func FromProbe(size int64, lastModified, etag string) *FileIdentity {
	if size == 0 && lastModified == "" && etag == "" {
		return nil
	}
	return &FileIdentity{Size: size, LastModified: lastModified, ETag: etag}
}

// Empty 报告指纹是否没有任何可用信息（这种情况下无法判定源变更）。
func (id *FileIdentity) Empty() bool {
	return id == nil || (id.Size == 0 && id.LastModified == "" && id.ETag == "")
}

// HeadersDiffer 比较两个身份的弱信号，返回人话原因（空串表示没有冲突）。
//
// 判定刻意保守：只有「双方都有同一项且不相等」才算变更。缺 ETag 的服务器
// 很常见，把「缺失」当「变更」会让大量正常任务无法续传——那正是 gopeed
// 一类工具被抱怨「一动就重下」的反面。
func (id *FileIdentity) HeadersDiffer(other *FileIdentity) string {
	if id == nil || other == nil {
		return ""
	}
	if id.Size > 0 && other.Size > 0 && id.Size != other.Size {
		return fmt.Sprintf("服务器上的源文件已变更（大小 %d → %d 字节）", id.Size, other.Size)
	}
	if id.ETag != "" && other.ETag != "" && id.ETag != other.ETag {
		return "服务器上的文件已更新（ETag 变化）"
	}
	if id.LastModified != "" && other.LastModified != "" && id.LastModified != other.LastModified {
		return "服务器上的文件已更新（Last-Modified 变化）"
	}
	return ""
}

// WorkPath 返回本次下载实际读写的文件路径：中间文件优先，兼容 M1.5 之前
// 没有 TempPath 的旧任务（它们继续用 SavePath，进度不清零、不重下）。
func (t *Task) WorkPath() string {
	if t == nil {
		return ""
	}
	if t.TempPath != "" {
		return t.TempPath
	}
	return t.SavePath
}

// AutoResumeAllowed 报告这个任务是否可以被「重启后自动续传」捞起来。
// 用户手动暂停的任务永远不自动开跑——那是 IDM/迅雷的一致体验。
func (t *Task) AutoResumeAllowed() bool {
	if t == nil {
		return false
	}
	if t.Status == StatusComplete || t.Status == StatusError {
		return false
	}
	if t.Status == StatusRunning { // 崩溃遗留的「下载中」也要能自动恢复
		return true
	}
	if t.Status != StatusPaused {
		return false
	}
	// 白名单而不是「非用户暂停」：任何没见过的原因都留在原地等用户，
	// 避免将来新增的暂停态被自动续传误捞（那等于替用户决定重下）。
	switch t.PauseReason {
	case PauseShutdown, PauseCrash, PauseDowngrade:
		return true
	default:
		return false
	}
}

// ValidateChunkLayout 校验持久化的分块布局是否自洽：连续、不重叠、
// 完整覆盖 [0, size)，且每块已完成字节不超过自身长度。
//
// 迁移与续传都用它：布局坏了就绝不按它写盘（那会写出一个静默损坏的文件）。
func ValidateChunkLayout(chunks []ChunkState, size int64) error {
	if size <= 0 {
		if len(chunks) > 0 {
			return fmt.Errorf("文件大小未知（%d）却存在 %d 个分块", size, len(chunks))
		}
		return nil
	}
	if len(chunks) == 0 {
		return nil // 单连接顺序下载：进度是 task.Downloaded，不是分块
	}
	sorted := make([]ChunkState, len(chunks))
	copy(sorted, chunks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })

	var expect int64
	for i, c := range sorted {
		if c.End < c.Start {
			return fmt.Errorf("分块 %d 区间非法：[%d,%d]", i, c.Start, c.End)
		}
		if c.Start != expect {
			return fmt.Errorf("分块 %d 起点 %d 与上一块不连续（期望 %d）", i, c.Start, expect)
		}
		if c.End >= size {
			return fmt.Errorf("分块 %d 超出文件大小：end=%d size=%d", i, c.End, size-1)
		}
		if c.Downloaded < 0 || c.Downloaded > c.End-c.Start+1 {
			return fmt.Errorf("分块 %d 已完成字节越界：%d/%d", i, c.Downloaded, c.End-c.Start+1)
		}
		expect = c.End + 1
	}
	if expect != size {
		return fmt.Errorf("分块未覆盖完整文件：覆盖到 %d，文件 %d", expect, size)
	}
	return nil
}

// ClampChunksToFile 用磁盘实测大小夹紧进度：任何越过实际文件末尾的
// 「已完成字节」都不作数（返回被丢弃的字节数）。
//
// 这是施工图 §6.1 约束 3 的执行处——预分配（稀疏文件）在复制/云盘同步/
// 磁盘压缩后可能退化成 0 字节，此时持久化计数器说的是谎话。
func ClampChunksToFile(chunks []ChunkState, fileSize int64) int64 {
	var dropped int64
	for i := range chunks {
		c := &chunks[i]
		// 整块都在文件末尾之后：块长就是丢弃量。
		if c.Start >= fileSize {
			dropped += c.Downloaded
			c.Downloaded = 0
			continue
		}
		// 块跨过了文件末尾：只保留末尾之前的部分。
		if limit := fileSize - c.Start; c.Downloaded > limit {
			dropped += c.Downloaded - limit
			c.Downloaded = limit
		}
	}
	return dropped
}
