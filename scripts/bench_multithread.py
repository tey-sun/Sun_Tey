#!/usr/bin/env python3
"""M1 多线程加速实测（施工图 R1 验收门禁）。

用一个「按连接限速」的本地 HTTP 服务器复现真实 CDN 的行为——单条连接被限
速、多条连接可以叠加——这正是 IDM/aria2 式多线程的意义。纯回环不限速时，
一条连接就能跑满，测不出差异，所以限速是必须的实验条件。

用法：
    python scripts/bench_multithread.py                      # 内置限速服务器
    python scripts/bench_multithread.py --size 128 --limit 8192
    python scripts/bench_multithread.py --url https://... --conns 1,8,16

输出：MB/s 表（每个连接数重复 --repeat 次，给出中位数与相对 1 路的加速比）
+ 每次落盘文件的 SHA-256 是否一致（多线程正确性）。
退出码：0 = 全部跑通且校验一致；1 = 任一次下载失败或哈希不一致。
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORE = os.path.join(ROOT, "core")


# --------------------------------------------------------------------------
# 限速 Range 服务器
# --------------------------------------------------------------------------
class ThrottledRangeServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, path: str, limit_kbps: int):
        super().__init__(("127.0.0.1", 0), self.handler_class(limit_kbps))
        self.path = path
        self.size = os.path.getsize(path)
        self.limit_kbps = limit_kbps

    def handle_error(self, request, client_address):
        # 下载器会主动切断慢连接（steal / 取消），回环上表现为 10054，
        # 这是被测行为的一部分，不是服务器故障：安静地丢掉即可。
        return

    @staticmethod
    def handler_class(limit_kbps: int):
        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):  # 静音
                pass

            def _target(self):
                srv = self.server  # type: ignore[attr-defined]
                return srv.path, srv.size

            def _send(self, head_only: bool):
                path, size = self._target()
                rng = self.headers.get("Range")
                start, end, code = 0, size - 1, 200
                if rng and rng.startswith("bytes="):
                    unit, _, spec = rng.partition("=")
                    first, _, last = spec.partition("-")
                    if first == "":
                        start = max(0, size - int(last))
                    else:
                        start = int(first)
                        end = min(int(last) if last else size - 1, size - 1)
                    if start > end or start >= size:
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{size}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    code = 206
                self.send_response(code)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(end - start + 1))
                if code == 206:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
                self.end_headers()
                if head_only:
                    return
                # 按连接限速：这就是"服务器对单连接限速、多连接才能叠加"的模型
                chunk = 256 * 1024
                budget = limit_kbps * 1024.0  # bytes/s
                with open(path, "rb") as f:
                    f.seek(start)
                    left = end - start + 1
                    while left > 0:
                        n = min(chunk, left)
                        buf = f.read(n)
                        if not buf:
                            break
                        t0 = time.perf_counter()
                        try:
                            self.wfile.write(buf)
                        except (ConnectionError, OSError):
                            return
                        left -= len(buf)
                        wait = len(buf) / budget - (time.perf_counter() - t0)
                        if wait > 0:
                            time.sleep(wait)

            def do_GET(self):
                self._send(False)

            def do_HEAD(self):
                self._send(True)

        return H


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(blk)
    return h.hexdigest()


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def build_bench() -> str:
    exe = os.path.join(tempfile.gettempdir(), "suntey-bench.exe")
    print(f"[build] go build -o {exe} ./cmd/suntey-bench", flush=True)
    subprocess.run(["go", "build", "-o", exe, "./cmd/suntey-bench"], cwd=CORE, check=True)
    return exe


def run_one(bench: str, url: str, conns: int, rep: int, workdir: str,
            max_per_host: int = 0) -> dict:
    argv = [bench, "-url", url, "-conns", str(conns), "-repeat", "1", "-out", workdir]
    if max_per_host:
        argv += ["-max-per-host", str(max_per_host)]
    # encoding=utf-8：被测内核的报错是中文，Windows 默认 GBK 解码会让
    # subprocess 的读线程直接抛 UnicodeDecodeError（实测），必须显式指定。
    r = subprocess.run(argv, capture_output=True, text=True, timeout=1800,
                       encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise RuntimeError(f"conns={conns} rep={rep}: {r.stderr.strip() or r.stdout.strip()}")
    line = [x for x in r.stdout.splitlines() if x.strip().startswith("{")][-1]
    return json.loads(line)


def main() -> int:
    ap = argparse.ArgumentParser(description="Sun_Tey M1 多线程实测")
    ap.add_argument("--url", default="", help="外部 URL（省略则启动内置限速服务器）")
    ap.add_argument("--conns", default="1,8,16", help="逗号分隔的连接数，默认 1,8,16")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--size", type=int, default=64, help="合成文件大小（MiB）")
    ap.add_argument("--limit", type=int, default=8192, help="单连接限速（KiB/s）")
    ap.add_argument("--out-json", default="", help="把结果写入 JSON 文件")
    ap.add_argument("--max-per-host", type=int, default=0,
                    help="覆盖 MaxConnPerHost（0 = 引擎默认 8，即 aria2 -x）")
    args = ap.parse_args()

    conns_list = [int(x) for x in args.conns.split(",") if x.strip()]
    bench = build_bench()
    workdir = tempfile.mkdtemp(prefix="suntey-bench-")
    server = None
    url = args.url

    try:
        if not url:
            fd, blob = tempfile.mkstemp(suffix=".bin", prefix="suntey-blob-")
            os.close(fd)
            print(f"[fixture] 生成 {args.size} MiB 随机文件 ...", flush=True)
            with open(blob, "wb") as f:
                written = 0
                while written < args.size * 1024 * 1024:
                    blk = os.urandom(min(4 * 1024 * 1024, args.size * 1024 * 1024 - written))
                    f.write(blk)
                    written += len(blk)
            print(f"[fixture] sha256 = {sha256_file(blob)}", flush=True)
            server = ThrottledRangeServer(blob, args.limit)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            url = f"http://127.0.0.1:{server.server_address[1]}/blob.bin"
            print(f"[server] {url} 单连接限速 {args.limit} KiB/s", flush=True)

        results: dict[int, list[dict]] = {c: [] for c in conns_list}
        for c in conns_list:
            for rep in range(1, args.repeat + 1):
                got = run_one(bench, url, c, rep, workdir, args.max_per_host)
                results[c].append(got)
                print(f"  conns={c:<3} rep={rep}  {got['mbps']:8.2f} MB/s  "
                      f"({got['seconds']:6.2f}s)  sha256={got['sha256'][:12]}", flush=True)

        base = statistics.median(r["mbps"] for r in results[conns_list[0]]) if results[conns_list[0]] else 0.0
        digests = {r["sha256"] for c in conns_list for r in results[c]}
        print()
        print("| 连接数 | 次数 | 中位 MB/s | 最快 MB/s | 最慢 MB/s | 相对 1 路 |")
        print("|---:|---:|---:|---:|---:|---:|")
        for c in conns_list:
            mbps = [r["mbps"] for r in results[c]]
            med = statistics.median(mbps)
            ratio = f"{med / base:.2f}x" if base else "n/a"
            print(f"| {c} | {len(mbps)} | {med:.2f} | {max(mbps):.2f} | {min(mbps):.2f} | {ratio} |")
        print()
        print(f"[integrity] 不同连接数落盘文件 SHA-256 种类 = {len(digests)}"
              f"（1 = 多线程切片字节级一致）")
        if args.out_json:
            with open(args.out_json, "w", encoding="utf-8") as f:
                json.dump({"url": url, "limit_kbps": args.limit,
                           "max_per_host": args.max_per_host, "results": results},
                          f, ensure_ascii=False, indent=2)
        return 0 if len(digests) <= 1 else 1
    finally:
        if server:
            server.shutdown()


if __name__ == "__main__":
    sys.exit(main())
