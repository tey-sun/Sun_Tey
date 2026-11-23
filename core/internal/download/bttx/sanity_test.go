package bttx

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
)

func TestPiecesMatchFile(t *testing.T) {
	tmp := t.TempDir()
	data := make([]byte, 300*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	metaPath, name := makeTorrent(t, tmp, data, 16384)
	mf, merr := os.Open(metaPath)
	if merr != nil {
		t.Fatal(merr)
	}
	mi, err := metainfo.Load(mf)
	mf.Close() // Windows: metainfo.Load does not close the reader
	if err != nil {
		t.Fatal(err)
	}
	info, err := mi.UnmarshalInfo()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(tmp, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, data) {
		t.Fatalf("file data changed on disk")
	}
	want := info.Pieces
	if len(want) != 20*((len(data)+16384-1)/16384) {
		t.Fatalf("pieces length %d", len(want))
	}
	for i := 0; i*16384 < len(data); i++ {
		lo := i * 16384
		hi := lo + 16384
		if hi > len(data) {
			hi = len(data)
		}
		h := sha1.Sum(raw[lo:hi])
		if !bytes.Equal(want[i*20:i*20+20], h[:]) {
			t.Fatalf("piece %d hash mismatch (meta vs file)", i)
		}
	}

	t.Log("meta pieces == file hashes ✓")
}
