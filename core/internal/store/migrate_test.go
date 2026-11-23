package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tey-sun/suntey/core/internal/model"
	bolt "go.etcd.io/bbolt"
)

// legacyTask 是 M1.5 之前写进库的任务形状（没有 snapVersion / tempPath / identity）。
type legacyTask map[string]any

func writeLegacyDB(t *testing.T, path string, tasks map[string]legacyTask, rawKey string, rawVal []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("config")); err != nil {
			return err
		}
		tb, err := tx.CreateBucketIfNotExists([]byte("tasks"))
		if err != nil {
			return err
		}
		for k, v := range tasks {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if err := tb.Put([]byte(k), b); err != nil {
				return err
			}
		}
		if rawKey != "" {
			return tb.Put([]byte(rawKey), rawVal)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func chunksJSON(n int, size int64, done int64) []map[string]any {
	out := make([]map[string]any, 0, n)
	per := size / int64(n)
	for i := 0; i < n; i++ {
		start := int64(i) * per
		end := start + per - 1
		if i == n-1 {
			end = size - 1
		}
		d := done
		if d > end-start+1 {
			d = end - start + 1
		}
		out = append(out, map[string]any{"index": i, "start": start, "end": end, "downloaded": d})
		done -= d
		if done < 0 {
			done = 0
		}
	}
	return out
}

func TestMigrateKeepsValidLegacyProgress(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		"t1": {"id": "t1", "req": map[string]any{"url": "http://x/a.bin"}, "status": 4,
			"name": "a.bin", "savePath": filepath.Join(dir, "a.bin"), "size": 100,
			"supportRange": true, "conn": 2, "downloaded": 40,
			"chunks": chunksJSON(2, 100, 40)},
	}, "", nil)

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	got, err := st.GetTask("t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.SnapVersion != model.SnapshotVersion {
		t.Fatalf("SnapVersion=%d want %d", got.SnapVersion, model.SnapshotVersion)
	}
	if got.Downloaded != 40 {
		t.Errorf("合法旧进度被丢掉：downloaded=%d want 40", got.Downloaded)
	}
	if len(got.Chunks) != 2 {
		t.Errorf("旧分块布局合法却被清空：%v", got.Chunks)
	}
	if got.ResumeNote != "" {
		t.Errorf("合法迁移不该产生说明：%q", got.ResumeNote)
	}
	// 磁盘上没有半成品：新任务用中间文件继续写。
	if want := filepath.Join(dir, "a.bin") + model.PartialSuffix; got.TempPath != want {
		t.Errorf("TempPath=%q want %q", got.TempPath, want)
	}
	if len(st.Migrations()) != 0 {
		t.Errorf("合法迁移不该记录： %+v", st.Migrations())
	}
}

func TestMigrateContinuesInPlaceWhenFileExists(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(final, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		"t2": {"id": "t2", "req": map[string]any{"url": "http://x/b.bin"}, "status": 4,
			"name": "b.bin", "savePath": final, "size": 100,
			"supportRange": true, "conn": 2, "downloaded": 40,
			"chunks": chunksJSON(2, 100, 40)},
	}, "", nil)

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _ := st.GetTask("t2")
	// 磁盘上已有 40 字节半成品：必须原地续传，换个名字写就等于把已下字节扔掉。
	if got.TempPath != "" {
		t.Errorf("已有半成品却改用了中间文件：%q", got.TempPath)
	}
	if got.Downloaded != 40 {
		t.Errorf("旧进度丢失：%d", got.Downloaded)
	}
}

func TestMigrateBrokenLayoutKeepsFileAndWarns(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "c.bin")
	if err := os.WriteFile(final, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		// 布局有洞：[0,39] 与 [50,99]，50 个字节凭空消失。按它写盘会静默损坏。
		"t3": {"id": "t3", "req": map[string]any{"url": "http://x/c.bin"}, "status": 4,
			"name": "c.bin", "savePath": final, "size": 100,
			"supportRange": true, "conn": 2, "downloaded": 40,
			"chunks": []map[string]any{
				{"index": 0, "start": 0, "end": 39, "downloaded": 40},
				{"index": 1, "start": 50, "end": 99, "downloaded": 0},
			}},
	}, "", nil)

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	got, _ := st.GetTask("t3")
	if got.Downloaded != 0 || len(got.Chunks) != 0 {
		t.Errorf("坏布局必须放弃进度：%+v", got.Chunks)
	}
	if got.ResumeNote == "" {
		t.Error("放弃进度必须写人话说明（禁止静默重下）")
	}
	if got.PauseReason != model.PauseInvalid {
		t.Errorf("PauseReason=%v want invalid（进度不可信的任务不得被自动续传）", got.PauseReason)
	}
	if got.Status != model.StatusPaused {
		t.Errorf("Status=%v want paused", got.Status)
	}
	// 磁盘文件必须一个字节没动。
	fi, err := os.Stat(final)
	if err != nil || fi.Size() != 100 {
		t.Errorf("磁盘文件被动过：%v %v", fi, err)
	}
	if len(st.Migrations()) != 1 {
		t.Errorf("Migrations 应记录 1 条：%+v", st.Migrations())
	}
}

func TestMigrateCrashLeftoverIsAutoResumable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		"t4": {"id": "t4", "req": map[string]any{"url": "http://x/d.bin"}, "status": 3, // 3 = running
			"name": "d.bin", "savePath": filepath.Join(dir, "d.bin"), "size": 100,
			"supportRange": true, "conn": 2, "downloaded": 10,
			"chunks": chunksJSON(1, 100, 10)},
	}, "", nil)

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _ := st.GetTask("t4")
	if got.Status != model.StatusPaused {
		t.Fatalf("崩溃遗留的 running 必须落到 paused，实际 %v", got.Status)
	}
	if got.PauseReason != model.PauseCrash || !got.AutoResumeAllowed() {
		t.Errorf("崩溃遗留应可自动续传：reason=%v allowed=%v", got.PauseReason, got.AutoResumeAllowed())
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		"t5": {"id": "t5", "req": map[string]any{"url": "http://x/e.bin"}, "status": 4,
			"name": "e.bin", "savePath": filepath.Join(dir, "e.bin"), "size": 100,
			"supportRange": true, "conn": 2, "downloaded": 40,
			"chunks": chunksJSON(2, 100, 40)},
	}, "", nil)

	for i := 0; i < 3; i++ {
		st, err := Open(dbPath)
		if err != nil {
			t.Fatalf("第 %d 次打开失败：%v", i+1, err)
		}
		got, _ := st.GetTask("t5")
		if got.Downloaded != 40 {
			t.Fatalf("第 %d 次打开后进度变了：%d", i+1, got.Downloaded)
		}
		if got.SnapVersion != model.SnapshotVersion {
			t.Fatalf("第 %d 次打开版本不对：%d", i+1, got.SnapVersion)
		}
		st.Close()
	}
}

func TestMigrateCorruptRowIsBackedUp(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	writeLegacyDB(t, dbPath, map[string]legacyTask{
		"good": {"id": "good", "req": map[string]any{"url": "http://x/g.bin"}, "status": 2,
			"name": "g.bin", "savePath": filepath.Join(dir, "g.bin"), "size": 10},
	}, "broken", []byte("{ not json"))

	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.GetTask("good"); err != nil {
		t.Errorf("坏行不该影响好行：%v", err)
	}
	found := false
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(filepath.Base(p), "broken") {
			found = true
		}
		return nil
	})
	if !found {
		t.Error("坏记录必须原样备份到 *.recover/ 下（保留现场，不静默删除）")
	}
	if len(st.Migrations()) == 0 {
		t.Error("坏记录必须产生一条可见说明")
	}
}
