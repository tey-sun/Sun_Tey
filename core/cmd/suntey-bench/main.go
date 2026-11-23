// Command suntey-bench measures the real Sun_Tey HTTP engine (internal/download/
// httpx) at a fixed connection count. It exists for the M1 acceptance gate
// (blueprint R1): a reproducible MB/s table for 1 / 8 / 16 connections against
// the same URL, plus a byte-exact SHA-256 of what landed on disk.
//
// It deliberately bypasses the dispatcher and the store: the number we care
// about is the scheduler (slow start + segment queue + stealing), not the API.
//
//	suntey-bench -url http://127.0.0.1:8000/blob -conns 8 -repeat 3 -out bench.tmp
//
// One JSON object per completed run goes to stdout so scripts can aggregate it.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/tey-sun/suntey/core/internal/download/httpx"
	"github.com/tey-sun/suntey/core/internal/model"
)

func main() {
	url := flag.String("url", "", "resource to download (required)")
	conns := flag.Int("conns", 8, "connections for this task (task.Conn)")
	repeat := flag.Int("repeat", 1, "number of runs")
	out := flag.String("out", "", "directory for the downloaded file (required)")
	maxConnHost := flag.Int("max-per-host", 0, "override MaxConnPerHost (0 = engine default)")
	maxSegs := flag.Int("max-segments", 0, "override MaxSegments (0 = engine default)")
	keep := flag.Bool("keep", false, "keep the downloaded file instead of deleting it")
	timeout := flag.Duration("timeout", 5*time.Minute, "per-run timeout")
	flag.Parse()

	if *url == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "suntey-bench: -url and -out are required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}
	size, rangeOK, err := probe(*url)
	if err != nil {
		fatal(err)
	}

	for rep := 1; rep <= *repeat; rep++ {
		save := filepath.Join(*out, fmt.Sprintf("bench-%d-%d.bin", *conns, rep))
		_ = os.Remove(save)

		d := httpx.NewDownloader(httpx.NewClient(), "suntey-bench")
		d.MaxConns = *conns // the process-wide budget must not cap the test
		if *maxConnHost > 0 {
			d.MaxConnPerHost = *maxConnHost
		}
		if *maxSegs > 0 {
			d.MaxSegments = *maxSegs
		}

		task := &model.Task{
			ID:           fmt.Sprintf("bench-%d-%d", *conns, rep),
			Req:          model.Request{URL: *url},
			Name:         filepath.Base(save),
			SavePath:     save,
			Size:         size,
			SupportRange: rangeOK,
			Conn:         *conns,
		}

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		started := time.Now()
		runErr := d.Run(ctx, task, &httpx.RunOptions{})
		elapsed := time.Since(started)
		cancel()
		if runErr != nil {
			fatal(fmt.Errorf("conns=%d rep=%d: %w", *conns, rep, runErr))
		}

		sum, bytesOnDisk, err := sha256File(save)
		if err != nil {
			fatal(err)
		}
		if size > 0 && bytesOnDisk != size {
			fatal(fmt.Errorf("conns=%d rep=%d: short file %d/%d bytes", *conns, rep, bytesOnDisk, size))
		}
		mbps := 0.0
		if s := elapsed.Seconds(); s > 0 {
			mbps = float64(bytesOnDisk) / (1 << 20) / s
		}
		enc := json.NewEncoder(os.Stdout)
		_ = enc.Encode(map[string]any{
			"conns": *conns, "rep": rep, "bytes": bytesOnDisk,
			"seconds": elapsed.Seconds(), "mbps": mbps,
			"sha256": sum, "range": rangeOK, "url": *url,
		})
		if !*keep {
			_ = os.Remove(save) // the measurement is the point, not the disk space
		}
	}
}

// probe asks the server for the resource size and whether it honours Range.
// A HEAD is not always supported (some servers answer 405), so fall back to a
// one-byte ranged GET.
func probe(url string) (size int64, rangeOK bool, err error) {
	c := httpx.NewClient()
	if req, e := http.NewRequest(http.MethodHead, url, nil); e == nil {
		if resp, e := c.Do(req); e == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
			resp.Body.Close()
			if resp.StatusCode < 400 && resp.ContentLength > 0 {
				_ = body
				return resp.ContentLength, resp.Header.Get("Accept-Ranges") == "bytes", nil
			}
		}
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := c.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return 0, false, err
	}
	_ = body
	if resp.StatusCode != http.StatusPartialContent {
		return resp.ContentLength, false, nil // no Range support: single stream
	}
	var lo, hi, total int64 = -1, -1, -1
	if _, e := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &lo, &hi, &total); e != nil || total < 0 {
		return 0, false, fmt.Errorf("probe: unparsable Content-Range %q", resp.Header.Get("Content-Range"))
	}
	return total, true, nil
}

func sha256File(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "suntey-bench:", err)
	os.Exit(1)
}
