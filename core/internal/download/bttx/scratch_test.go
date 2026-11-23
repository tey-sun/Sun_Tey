package bttx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// scratchTree is t.TempDir for tests that let the BT engine open files.
//
// Why not t.TempDir: anacrolix v1.61 never closes the shared read handles it
// opens for hashing/reading. classicFileIo uses the plain sharedFiles pool
// (storage/file-handle-cache.go: regularFsSharedFiles.CloseAll is a no-op) and
// fileTorrentImpl.Close returns nil (storage/file-torrent.go), so a finished
// task's files stay open until the process exits. On Windows that makes
// RemoveAll fail with "used by another process", which would fail every BT test
// for a reason unrelated to the assertion under test. The sweep below retries
// for a bounded window and then only reports the leftover; the product-side
// consequence (删除任务/文件 must tolerate 文件被占用) is tracked in
// docs/待办.md.
func scratchTree(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", scratchPrefix)
	if err != nil {
		t.Fatalf("scratch dir: %v", err)
	}
	t.Cleanup(func() { sweepScratch(t, root) })
	return root
}

const scratchPrefix = "suntey-bt-"

// sweepScratch removes root, tolerating the leaked handles described above.
func sweepScratch(t *testing.T, root string) {
	t.Helper()
	var last error
	for i := 0; i < 15; i++ {
		if last = os.RemoveAll(root); last == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("scratch tree left behind (anacrolix keeps file handles open after Close): %s: %v", root, last)
}

// TestMain reclaims scratch trees a previous run could not delete. Handles die
// with the process, so they are removable by the next run; only our own prefix
// under the OS temp dir is ever touched.
func TestMain(m *testing.M) {
	code := m.Run()
	if err := reclaimScratch(); err != nil {
		fmt.Fprintf(os.Stderr, "reclaim scratch: %v\n", err)
	}
	os.Exit(code)
}

func reclaimScratch() error {
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), scratchPrefix) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(os.TempDir(), e.Name()))
	}
	return nil
}
