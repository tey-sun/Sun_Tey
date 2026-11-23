package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/tey-sun/suntey/core/internal/model"
	bolt "go.etcd.io/bbolt"
)

// 快照 schema 迁移（施工图 §6.1 约束 1 / M1.5）。
//
// 规则只有一条：迁移**绝不静默重下**。要么原样保留进度并打上版本号，
// 要么保留磁盘文件、把放弃的进度写成人话留在任务上（ResumeNote），
// 并且不再自动开跑——等用户确认。

var bucketMeta = []byte("meta")

const metaSchema = "schema"

// Migration 是一条启动期迁移记录：任务、文件名、对用户可见的说明。
type Migration struct {
	TaskID string `json:"taskId"`
	Name   string `json:"name"`
	Note   string `json:"note"`
}

// Migrations 返回本次启动实际做过的迁移（打开日志、任务卡片都靠它）。
func (s *Store) Migrations() []Migration { return s.migrations }

func (s *Store) schemaVersion() (int, error) {
	var v int
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(metaSchema))
		if raw == nil {
			return nil
		}
		n, err := strconv.Atoi(string(raw))
		if err != nil {
			return fmt.Errorf("schema 版本无法解析：%q", raw)
		}
		v = n
		return nil
	})
	return v, err
}

func (s *Store) setSchemaVersion(v int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		return b.Put([]byte(metaSchema), []byte(strconv.Itoa(v)))
	})
}

// migrate 把所有旧快照提升到当前 schema，并记录每条目做了什么。
// 幂等：第二次打开时版本已一致，直接返回。
func (s *Store) migrate() ([]Migration, error) {
	cur, err := s.schemaVersion()
	if err != nil {
		return nil, err
	}
	if cur >= model.SnapshotVersion {
		return nil, nil
	}

	var notes []Migration
	err = s.db.Update(func(tx *bolt.Tx) error {
		tb := tx.Bucket(bucketTasks)
		if tb == nil {
			return nil
		}
		return tb.ForEach(func(k, v []byte) error {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				// 坏行绝不能吞掉整个数据库：保留原始键值，另存一份副本文件，
				// 让用户还能手动救回（施工图 §6.1「保留原文件并提示」）。
				notes = append(notes, Migration{
					TaskID: string(k),
					Name:   "(无法解析的任务记录)",
					Note:   "该任务记录已损坏，已原样备份到数据库旁边的 recover 文件；未改动任何下载文件。",
				})
				return s.backupRaw(k, v)
			}
			note := migrateTask(&t)
			t.SnapVersion = model.SnapshotVersion
			t.UpdatedAt = time.Now()
			b, err := json.Marshal(&t)
			if err != nil {
				return err
			}
			if err := tb.Put(k, b); err != nil {
				return err
			}
			if note != "" {
				notes = append(notes, Migration{TaskID: t.ID, Name: t.Name, Note: note})
			}
			return nil
		})
	})
	if err != nil {
		return notes, err
	}
	return notes, s.setSchemaVersion(model.SnapshotVersion)
}

// backupRaw 把解析失败的原始记录写到数据库旁边（不覆盖已有备份）。
func (s *Store) backupRaw(k, v []byte) error {
	dir := s.db.Path() + ".recover"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("task-%s-%d.json", sanitizeKey(string(k)), time.Now().UnixNano())
	return os.WriteFile(filepath.Join(dir, name), v, 0o600)
}

// migrateTask 把一个 M1.5 之前的任务提升到当前 schema，返回给用户看的说明。
func migrateTask(t *model.Task) string {
	if t.SnapVersion >= model.SnapshotVersion {
		return ""
	}
	// 已完成/已失败的任务不再涉及续传，只补版本号。
	if t.Status == model.StatusComplete || t.Status == model.StatusError {
		t.SnapVersion = model.SnapshotVersion
		return ""
	}

	// 1) 崩溃遗留的「下载中」：进程早就不在了，落到暂停并标成崩溃，
	//    这样「重启系统后自动续传」（M1.5 场景 B）才知道该捞它。
	if t.Status == model.StatusRunning {
		t.Status = model.StatusPaused
		t.PauseReason = model.PauseCrash
	}

	// 2) 旧分块布局必须自洽才允许采信；不自洽就放弃进度，但**保留磁盘文件**，
	//    并且不再自动开跑（ResumeInvalid 不在自动续传白名单里）。
	if err := model.ValidateChunkLayout(t.Chunks, t.Size); err != nil {
		lost := t.Downloaded
		t.Chunks = nil
		t.Downloaded = 0
		t.Status = model.StatusPaused
		t.PauseReason = model.PauseInvalid
		t.ResumeNote = fmt.Sprintf(
			"旧版本进度记录不完整（%v），已放弃 %s 的进度并保留已下载文件；点「开始」将安全重下", err, humanBytes(lost))
		return t.ResumeNote
	}

	// 3) 旧任务没有中间文件路径：磁盘上已经有半成品就原地继续（绝不重下），
	//    全新的才启用 .suntey-partial。
	if t.SavePath != "" && !bttxTask(t) {
		if _, err := os.Stat(t.SavePath); err == nil {
			t.TempPath = ""
		} else {
			t.TempPath = t.SavePath + model.PartialSuffix
		}
	}
	return ""
}

func bttxTask(t *model.Task) bool {
	// bttx 包不认识任务（避免 import 环），这里只按协议特征粗判：
	// BT 任务的 Size 在元数据到达前是 -1 且 Files 非空，或者 URL 是磁力/种子。
	return len(t.Files) > 0 || t.HLS != nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// sanitizeKey 只保留文件名安全字符（任务 ID 本就是十六进制，防御性处理）。
func sanitizeKey(k string) string {
	out := make([]byte, 0, len(k))
	for i := 0; i < len(k); i++ {
		c := k[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return "task"
	}
	return string(out)
}
