package httpx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/tey-sun/suntey/core/internal/model"
)

// 续传强校验（施工图 §6.1 约束 2/3，M1.5 场景 D）。
//
// 弱身份（ETag / Last-Modified）免费但可被服务器随意改；强身份是「前 1 MiB 的
// SHA-256」，代价 1 MiB 流量，能证明「本地已下载的头部字节」与「服务器当前
// 内容的头部字节」是同一份东西。两者都不满足时宁可不续传，也绝不把两份不同
// 内容拼成一个静默损坏的文件——那比慢一点下载糟糕得多。

// HeadHash 取回源文件前 n 字节并计算 SHA-256。
//
// 优先用 Range；服务器无视 Range 返回 200 时只读前 n 字节就断开，
// 最坏代价就是 n 字节。返回实际参与哈希的字节数，调用方必须用同一个 n
// 去算本地哈希，否则两边永远不相等。
func (d *Downloader) HeadHash(ctx context.Context, r model.Request, n int64) (string, int64, error) {
	if n <= 0 {
		return "", 0, errors.New("校验窗口必须大于 0")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return "", 0, err
	}
	applyHeader(req, r, d.UA)
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", n-1))

	cl := d.Client
	if cl == nil {
		cl = NewClient()
	}
	resp, err := cl.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("head-hash: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("head-hash: HTTP %d %s", resp.StatusCode, resp.Status)
	}
	// 只读 n 字节：Close 之后连接丢弃，不会把整个文件拖下来。
	h := sha256.New()
	n2, err := io.Copy(h, io.LimitReader(resp.Body, n))
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, context.Canceled) {
		return "", 0, err
	}
	if n2 == 0 {
		return "", 0, errors.New("head-hash: 服务器没有返回任何字节")
	}
	return hex.EncodeToString(h.Sum(nil)), n2, nil
}

// LocalHeadHash 计算本地文件前 n 字节的 SHA-256，并返回实际读取的字节数
// （文件比 n 短时按真实长度，避免「本地 900 KB vs 服务器 1 MiB」误判成损坏）。
func LocalHeadHash(path string, n int64) (string, int64, error) {
	if n <= 0 {
		return "", 0, errors.New("校验窗口必须大于 0")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n2, err := io.Copy(h, io.LimitReader(f, n))
	if err != nil {
		return "", 0, err
	}
	if n2 == 0 {
		return "", 0, errors.New("head-hash: 本地文件是空的")
	}
	return hex.EncodeToString(h.Sum(nil)), n2, nil
}
