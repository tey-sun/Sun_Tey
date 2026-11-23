package model

import "testing"

func chunk(i int, s, e, d int64) ChunkState {
	return ChunkState{Index: i, Start: s, End: e, Downloaded: d}
}

func TestValidateChunkLayout(t *testing.T) {
	ok := [][]ChunkState{
		nil,
		{chunk(0, 0, 99, 0)},
		{chunk(0, 0, 49, 50), chunk(1, 50, 99, 10)},
	}
	for i, cs := range ok {
		if err := ValidateChunkLayout(cs, 100); err != nil {
			t.Errorf("case %d: 合法布局被拒绝：%v", i, err)
		}
	}

	bad := []struct {
		name   string
		chunks []ChunkState
	}{
		{"空洞", []ChunkState{chunk(0, 0, 39, 40), chunk(1, 50, 99, 0)}},
		{"重叠", []ChunkState{chunk(0, 0, 60, 61), chunk(1, 50, 99, 0)}},
		{"覆盖不全", []ChunkState{chunk(0, 0, 89, 90)}},
		{"越界", []ChunkState{chunk(0, 0, 120, 0)}},
		{"已完成字节越界", []ChunkState{chunk(0, 0, 99, 500)}},
	}
	for _, c := range bad {
		if err := ValidateChunkLayout(c.chunks, 100); err == nil {
			t.Errorf("%s：坏布局被判为合法", c.name)
		}
	}
	// 单连接顺序下载没有分块，只认 task.Downloaded。
	if err := ValidateChunkLayout(nil, 0); err != nil {
		t.Errorf("顺序任务（无分块、大小未知）应为合法：%v", err)
	}
	if err := ValidateChunkLayout([]ChunkState{chunk(0, 0, 9, 1)}, -1); err == nil {
		t.Error("大小未知却有分块：布局不可信，必须判非法")
	}
}

func TestClampChunksToFile(t *testing.T) {
	cases := []struct {
		name        string
		size        int64
		chunks      []ChunkState
		wantDropped int64
		wantDone    []int64
	}{
		{"文件完好", 100, []ChunkState{chunk(0, 0, 49, 50), chunk(1, 50, 99, 25)}, 0, []int64{50, 25}},
		{"第二块被截断", 60, []ChunkState{chunk(0, 0, 49, 50), chunk(1, 50, 99, 25)}, 15, []int64{50, 10}},
		{"整块丢了", 40, []ChunkState{chunk(0, 0, 49, 50), chunk(1, 50, 99, 25)}, 35, []int64{40, 0}},
		{"文件空了", 0, []ChunkState{chunk(0, 0, 99, 70)}, 70, []int64{0}},
	}
	for _, c := range cases {
		got := ClampChunksToFile(c.chunks, c.size)
		if got != c.wantDropped {
			t.Errorf("%s: dropped=%d want %d", c.name, got, c.wantDropped)
		}
		for i, want := range c.wantDone {
			if c.chunks[i].Downloaded != want {
				t.Errorf("%s: chunk %d done=%d want %d", c.name, i, c.chunks[i].Downloaded, want)
			}
		}
	}
}

func TestFileIdentityHeadersDiffer(t *testing.T) {
	base := &FileIdentity{Size: 1000, ETag: `"a"`, LastModified: "Mon, 01 Jan 2025 00:00:00 GMT"}
	if r := base.HeadersDiffer(&FileIdentity{Size: 1000, ETag: `"a"`, LastModified: "Mon, 01 Jan 2025 00:00:00 GMT"}); r != "" {
		t.Errorf("完全相同被判为变更：%q", r)
	}
	if r := base.HeadersDiffer(&FileIdentity{Size: 999}); r == "" {
		t.Error("大小变化必须判为变更")
	}
	if r := base.HeadersDiffer(&FileIdentity{Size: 1000, ETag: `"b"`}); r == "" {
		t.Error("ETag 变化必须判为变更")
	}
	// 缺失 ≠ 变更：把「服务器没给 ETag」当变更会让大量正常任务无法续传。
	if r := base.HeadersDiffer(&FileIdentity{Size: 1000}); r != "" {
		t.Errorf("仅缺少弱信号被判为变更：%q", r)
	}
	if r := (&FileIdentity{}).HeadersDiffer(nil); r != "" {
		t.Errorf("空身份不得判定任何结论：%q", r)
	}
}

func TestTaskWorkPathAndAutoResume(t *testing.T) {
	tr := &Task{SavePath: `C:\d\a.bin`}
	if tr.WorkPath() != `C:\d\a.bin` {
		t.Error("没有 TempPath 时必须回退到 SavePath（兼容 M1.5 之前的旧任务）")
	}
	tr.TempPath = `C:\d\a.bin` + PartialSuffix
	if tr.WorkPath() != `C:\d\a.bin.suntey-partial` {
		t.Error("有 TempPath 时必须写中间文件")
	}

	cases := []struct {
		name string
		task Task
		want bool
	}{
		{"崩溃遗留的下载中", Task{Status: StatusRunning}, true},
		{"关机自动暂停", Task{Status: StatusPaused, PauseReason: PauseShutdown}, true},
		{"进程被强杀", Task{Status: StatusPaused, PauseReason: PauseCrash}, true},
		{"降级后重开", Task{Status: StatusPaused, PauseReason: PauseDowngrade}, true},
		{"用户点了暂停", Task{Status: StatusPaused, PauseReason: PauseUser}, false},
		{"没写原因的暂停", Task{Status: StatusPaused}, false},
		{"未来新增的暂停原因", Task{Status: StatusPaused, PauseReason: PauseReason("whatever")}, false},
		{"进度不可信", Task{Status: StatusPaused, PauseReason: PauseInvalid}, false},
		{"已完成", Task{Status: StatusComplete, PauseReason: PauseCrash}, false},
		{"已失败", Task{Status: StatusError, PauseReason: PauseCrash}, false},
	}
	for _, c := range cases {
		if got := c.task.AutoResumeAllowed(); got != c.want {
			t.Errorf("%s: AutoResumeAllowed=%v want %v", c.name, got, c.want)
		}
	}
}
