#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M1.5 断点续传矩阵（施工图 §M1.5 验收 / R11，横切能力）。

真进程：本脚本只走 REST 契约（在 core/bin 下的 suntey-core.exe serve + /api/v1/*），
用 taskkill /F 强杀内核进程模拟崩溃/断电/重启，不碰任何 Go 内部函数。
每个场景一套独立的 store 与下载目录，互不污染。

场景（施工图 §M1.5 验收清单，逐条对应）：
  A 下载中 taskkill /F 内核进程   -> 重启 -> 自动续传完成
  B 下载中重启整台机器            -> 脚本不重启机器：标 [U]，由 A 的崩溃状态机路径
                                     （Running -> Paused(crash) -> AutoResume）+
                                     Go 单测 TestMatrixAutoResumeSkipsUserPaused 覆盖
  C 服务器 30 s 不回数据（断网）  -> 恢复 -> 任务不断、从断点继续
  D 中途改源（size 变 / 等长替换+ETag 变）-> 拒绝续传 + 可操作提示
  E Accept-Ranges: none           -> 单连接回退（并记录崩溃后的真实行为）
  F HLS 下载一半杀进程             -> 重启 -> 只补缺失分片
  G 磁力 piece 位图恢复            -> 需真实 swarm：标 [U]，--go 时跑 bttx Go 测试

判据（施工图 §M1.5）：A/C/E/F/G 重下字节 < 5%（理想为 0）且最终 SHA-256 与完整
下载一致；D 必须拒绝续传并给出提示。

重下字节 = 服务端两次进程累计发出的载荷字节 - 文件大小。
完美续传 = 0（一个字节都不该重发）。快照 2 s 落盘窗口内的字节会计入这里，
那是施工图 R11 第 2 条「最坏丢失 <=2 s 进度」的既定代价，不是重复下载已校验字节。

用法：
  python scripts/test_resume_matrix.py            # 全量（约 3-4 分钟，真杀进程）
  python scripts/test_resume_matrix.py --fast     # 小样本冒烟（判据窗口会相应变大）
  python scripts/test_resume_matrix.py --go       # 追加跑 Go 侧矩阵测试
  python scripts/test_resume_matrix.py --only A,C # 只跑指定场景
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:  # 中文输出在 GBK 控制台上会炸，统一按 UTF-8 写
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")
KIB, MIB = 1024, 1024 * 1024

ST_READY, ST_RUNNING, ST_PAUSED, ST_DONE, ST_ERR = 2, 3, 4, 5, 6


# --------------------------------------------------------------------------- #
#  被测内核：serve 子进程 + loopback REST 客户端
# --------------------------------------------------------------------------- #

class Core:
    """一个 suntey-core serve 进程 + loopback REST 客户端。

    与 e2e-graceful.py 同一套接线方式。注意 stdout 上 listening 之前可能先吐
    migration / resumed 事件，所以必须按 type 找 listening 行，不能读第一行。
    """

    def __init__(self, store, dldir, tag):
        if not os.path.exists(EXE):
            raise SystemExit("找不到内核可执行文件：" + EXE + "（先 go build -o core/bin/suntey-core.exe ./cmd/suntey-core）")
        self.tag = tag
        self.store = store
        self.dldir = dldir
        self.errfile = open(store + ".err", "wb")
        self.p = subprocess.Popen(
            [EXE, "serve", "--port", "0", "--store", store, "--dir", dldir],
            stdout=subprocess.PIPE, stderr=self.errfile, text=True, encoding="utf-8")
        self.events = []  # listening 之前吐出的 migration / resumed 事件
        self.base = None
        self.token = ""
        deadline = time.time() + 25
        while time.time() < deadline:
            line = self.p.stdout.readline()
            if not line:
                raise SystemExit("core serve 没吐 listening（看 " + store + ".err）")
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if d.get("type") == "listening":
                self.base = "http://127.0.0.1:%d" % d["port"]
                self.token = d.get("token") or ""
                break
            self.events.append(d)

    def req(self, method, path, body=None, timeout=10):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if self.token:
            r.add_header("X-SunTey-Token", self.token)
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                return json.loads(resp.read() or b'{"ok":true}')
        except urllib.error.HTTPError as e:
            return {"ok": False, "error": "HTTP %d %s" % (e.code, e.read()[:180].decode("utf-8", "replace"))}
        except Exception as e:  # noqa: BLE001 - 测试脚本只关心"没连上"
            return {"ok": False, "error": repr(e)}

    def data(self, method, path, body=None):
        resp = self.req(method, path, body)
        if resp.get("ok") is not True:
            raise AssertionError("%s %s -> %s" % (method, path, resp))
        return resp["data"]

    def task(self, tid):
        for t in self.data("GET", "/api/v1/tasks"):
            if t["id"] == tid:
                return t
        raise AssertionError("任务不在列表里: " + tid)

    def kill(self):
        """真·强杀：taskkill /F，等同崩溃/断电/关机，不触发任何优雅保存。"""
        subprocess.run(["taskkill", "/F", "/PID", str(self.p.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.p.kill()
        try:
            self.errfile.close()
        except Exception:
            pass


class Env:
    """一个场景的独立世界：独立 store + 独立下载目录。"""

    def __init__(self, name, root):
        self.name = name
        self.home = os.path.join(root, name)
        self.dldir = os.path.join(self.home, "dl")
        self.store = os.path.join(self.home, "core.db")
        os.makedirs(self.dldir, exist_ok=True)
        self.cores = []

    def start(self):
        c = Core(self.store, self.dldir, self.name)
        self.cores.append(c)
        return c

    def close(self):
        for c in self.cores:
            try:
                c.kill()
            except Exception:
                pass


def partials(dldir):
    """下载目录里的中间文件（*.suntey-partial）：{路径: 字节数}。"""
    out = {}
    for n in os.listdir(dldir):
        if n.endswith(".suntey-partial"):
            pth = os.path.join(dldir, n)
            try:
                out[pth] = os.path.getsize(pth)
            except OSError:
                out[pth] = -1
    return out


def poll(fn, timeout, interval=0.25):
    """轮询直到 fn() 返回真值；超时返回最后一次结果（可能是 None）。"""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = fn()
        except AssertionError:
            last = None
        if last:
            return last
        time.sleep(interval)
    return last


# --------------------------------------------------------------------------- #
#  测试源服务器（可控：Range / 无 Range / 限速 / 30 s 静默 / 中途改源）
# --------------------------------------------------------------------------- #

def make_data(seed, size):
    """确定性伪随机数据（32 MiB 也只要毫秒级），SHA-256 用于终局校验。"""
    d = hashlib.sha256(seed).digest()
    return (d * (size // len(d) + 1))[:size]


class Src:
    """单文件 HTTP 源。

    throttle   每写一块睡多久（模拟限速；强杀前开着，续传阶段关掉，省时间）
    ranges     False = 服务器无视 Range 只回全量 200（Accept-Ranges: none）
    outage     到某个时刻之前一个字节都不写（连接不断开）= 拔网线的 HTTP 层等价物
    """

    def __init__(self, seed, size, ranges=True, throttle=0.05, chunk=16 * KIB):
        self.data = make_data(seed, size)
        self.size = len(self.data)
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.ranges = ranges
        self.throttle = throttle
        self.chunk = chunk
        self.etag = '"src-v1"'
        self.last_modified = "Wed, 21 Oct 2021 07:28:00 GMT"
        self.outage_until = 0.0

        self.lock = threading.Lock()
        self.payload = 0      # 生产载荷字节（真正的下载流量，参与重下判定）
        self.verify = 0       # 探测/头部校验字节（续传机制的固定开销，单列）
        self.streams = 0      # 当前并发响应数（含探针，诊断用）
        self.peak_streams = 0
        self.pstreams = 0     # 当前并发**载荷**响应数（判据看这个）
        self.payload_peak = 0
        self.requests = 0

    def count(self, n, is_verify):
        with self.lock:
            if is_verify:
                self.verify += n
            else:
                self.payload += n

    def snapshot(self):
        with self.lock:
            return self.payload, self.verify, self.peak_streams

    def mutate(self, seed, size_delta=0, new_etag='"src-v2"'):
        """服务端换文件：size_delta!=0 改长度，否则等长替换；ETag 一起变。"""
        new = make_data(seed, self.size + size_delta)
        self.data = new
        self.size = len(new)
        self.sha = hashlib.sha256(new).hexdigest()
        if new_etag:
            self.etag = new_etag

    def arm_outage(self, seconds):
        self.outage_until = time.time() + seconds

    def _idle(self):
        while True:
            left = self.outage_until - time.time()
            if left <= 0:
                return
            time.sleep(min(0.2, left))

    def stream(self, wfile, body, is_verify):
        self._idle()
        step = 4096 if is_verify else self.chunk
        for i in range(0, len(body), step):
            part = body[i:i + step]
            if self.outage_until > time.time():
                self._idle()
            if self.throttle and not is_verify:
                time.sleep(self.throttle)
            wfile.write(part)
            wfile.flush()
            self.count(len(part), is_verify)


def probe_window(m):
    """Range 头窗口 <= 4 KiB 视为「探活/头部校验」，不是下载载荷。"""
    if not m:
        return False
    s0 = int(m.group(1) or 0)
    e0 = int(m.group(2)) if m.group(2) else -1
    return e0 >= 0 and (e0 - s0 + 1) <= 4096


def file_server(src):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive：不制造连接抖动

        def log_message(self, *a):
            pass

        def handle_one_request(self):
            try:
                super().handle_one_request()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                self.close_connection = True

        def _common(self):
            self.send_header("ETag", src.etag)
            self.send_header("Last-Modified", src.last_modified)
            self.send_header("Accept-Ranges", "bytes" if src.ranges else "none")

        def _guard(self, write_body, payload=False):
            src.requests += 1
            with src.lock:
                src.streams += 1
                src.peak_streams = max(src.peak_streams, src.streams)
                if payload:
                    src.pstreams += 1
                    src.payload_peak = max(src.payload_peak, src.pstreams)
            try:
                write_body()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                self.close_connection = True
            finally:
                with src.lock:
                    src.streams -= 1
                    if payload:
                        src.pstreams -= 1

        def do_HEAD(self):
            def body():
                self.send_response(200)
                self._common()
                self.send_header("Content-Length", str(src.size))
                self.end_headers()
            self._guard(body)

        def do_GET(self):
            rng = self.headers.get("Range", "")
            data = src.data
            size = len(data)
            m = re.match(r"bytes=(\d*)-(\d*)$", rng)

            if not src.ranges:
                # 装死服务器：带 Range 也只回全量 200。只有**小窗口**请求
                # （bytes=0-0 这类，客户端最多读 1 KiB 就断开）才是探针；
                # bytes=2162688- 这种开放窗口是真在续传/重下，属于载荷。
                # 早先按「带 Range 就算探针」记账，把重启后整文件重下的字节
                # 记成了探测开销，E2 的「重发 0 B」是假绿（2026-11-24 修正）。
                is_verify = bool(m) and probe_window(m)

                def full():
                    self.send_response(200)
                    self._common()
                    self.send_header("Content-Length", str(size))
                    self.end_headers()
                    src.stream(self.wfile, data, is_verify)
                self._guard(full, payload=not is_verify)
                return

            if m:
                s = int(m.group(1) or 0)
                e = int(m.group(2)) if m.group(2) else size - 1
                e = min(e, size - 1)
                if s >= size or s > e:
                    self.send_response(416)
                    self._common()
                    self.send_header("Content-Range", "bytes */%d" % size)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = data[s:e + 1]
                probe = (e - s + 1) <= 4096  # bytes=0-0 这类探活

                def part():
                    self.send_response(206)
                    self._common()
                    self.send_header("Content-Range", "bytes %d-%d/%d" % (s, e, size))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    src.stream(self.wfile, body, probe)
                self._guard(part, payload=not probe)
                return

            def whole():
                self.send_response(200)
                self._common()
                self.send_header("Content-Length", str(size))
                self.end_headers()
                src.stream(self.wfile, data, False)
            self._guard(whole, payload=True)

    class Srv(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

    srv = Srv(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/file.bin" % srv.server_address[1]


# --------------------------------------------------------------------------- #
#  HLS 源（场景 F）：清单 + 分片，按分片计请求次数
# --------------------------------------------------------------------------- #

def hls_server(nseg, seg_size, throttle):
    segs = [make_data(b"seg-%03d" % i, seg_size) for i in range(nseg)]
    st = {"lock": threading.Lock(), "playlist": 0, "hits": [0] * nseg,
          "payload": 0, "throttle": throttle}

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def handle_one_request(self):
            try:
                super().handle_one_request()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                self.close_connection = True

        def do_GET(self):
            path = self.path.split("?")[0]
            if path.endswith(".m3u8"):
                with st["lock"]:
                    st["playlist"] += 1
                txt = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"
                for i in range(nseg):
                    txt += "#EXTINF:4.0,\nseg%03d\n" % i
                txt += "#EXT-X-ENDLIST\n"
                body = txt.encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.close_connection = True
                return
            m = re.match(r"/seg(\d+)$", path)
            if not m:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            i = int(m.group(1))
            if i >= nseg:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            with st["lock"]:
                st["hits"][i] += 1
            body = segs[i]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for j in range(0, len(body), 16 * KIB):
                    if st["throttle"]:
                        time.sleep(st["throttle"])
                    part = body[j:j + 16 * KIB]
                    self.wfile.write(part)
                    self.wfile.flush()
                    with st["lock"]:
                        st["payload"] += len(part)
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.close_connection = True

    class Srv(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

    srv = Srv(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/v.m3u8" % srv.server_address[1], segs, st


# --------------------------------------------------------------------------- #
#  事件流旁路（SSE）：证明"断网期间任务没死，是在重试"
# --------------------------------------------------------------------------- #

class EventTap:
    def __init__(self, core):
        self.core = core
        self.events = []
        self.stop = threading.Event()
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()

    def _run(self):
        url = self.core.base + "/api/v1/events?token=" + self.core.token
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=600) as resp:
                buf = b""
                while not self.stop.is_set():
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n\n" in buf:
                        head, buf = buf.split(b"\n\n", 1)
                        for ln in head.decode("utf-8", "replace").splitlines():
                            if ln.startswith("data: "):
                                try:
                                    self.events.append(json.loads(ln[6:]))
                                except ValueError:
                                    pass
        except Exception:
            pass

    def count(self, type_):
        return len([e for e in self.events if e.get("type") == type_])

    def close(self):
        self.stop.set()


# --------------------------------------------------------------------------- #
#  通用小工具
# --------------------------------------------------------------------------- #

class Cfg:
    size = 32 * MIB
    conn = 4
    throttle = 0.09      # 每 16 KiB 睡 0.09 s；4 路合计约 0.7 MB/s
    kill_at = 0.25       # 下载到 25% 时动手
    outage = 32          # 场景 C：静默秒数（> 内核 IdleTimeout 30 s）
    kill_timeout = 180
    finish_timeout = 240


def create_start(core, url, conn=0):
    body = {"url": url}
    if conn:
        body["conn"] = conn
    t = core.data("POST", "/api/v1/tasks", body)
    core.data("POST", "/api/v1/tasks/start", {"id": t["id"]})
    return t


def wait_running_progress(core, tid, target, timeout):
    """等到「已下载 >= target 且仍在下载中」，返回那一刻的任务快照。"""
    end = time.time() + timeout
    while time.time() < end:
        time.sleep(0.25)
        t = core.task(tid)
        if t["status"] != ST_RUNNING:
            continue
        if t["downloaded"] >= target:
            return t
        # 已经下完（文件太小）也算到位，交给上层判定
    return None


def file_sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def rows_append(rows, scen, verdict, reserved, size, sha_ok, note):
    pct = "" if size <= 0 or reserved is None else "%.2f%%" % (100.0 * reserved / size)
    rows.append({
        "scen": scen, "verdict": verdict,
        "reserved": "-" if reserved is None else str(reserved),
        "pct": pct, "sha": {True: "一致", False: "不一致", None: "-"}[sha_ok],
        "note": note,
    })


def wait_final(core, tid, timeout):
    """等到完成/错误终态并返回那条任务；超时返回 None。"""
    box = {}

    def once():
        t = core.task(tid)
        if t["status"] in (ST_DONE, ST_ERR):
            box["t"] = t
            return t
        return None

    poll(once, timeout, 0.3)
    return box.get("t")


class Check:
    """攒断言：一个场景里所有不合格项都记下来，最后一起进表，别改一个跑一遍。"""

    def __init__(self):
        self.bad = []

    def ck(self, cond, msg):
        if not cond:
            self.bad.append(msg)

    def verdict(self):
        return "FAIL" if self.bad else "PASS"

    def suffix(self):
        return ("；" + "；".join(self.bad)) if self.bad else ""


def window_budget(cfg):
    """快照 2 s 落盘窗口的字节预算 = 2 s x 总速率（R11 第 2 条允许丢失的上限）。"""
    if not cfg.throttle:
        return 0
    return int(2.0 * cfg.conn * (16 * KIB) / cfg.throttle)


def glob_partial(dldir):
    for n in os.listdir(dldir):
        if n.endswith(".suntey-partial"):
            return os.path.join(dldir, n)
    return ""


def glob_parts_dir(dldir):
    for n in os.listdir(dldir):
        if n.endswith(".parts") and os.path.isdir(os.path.join(dldir, n)):
            return os.path.join(dldir, n)
    return ""


# --------------------------------------------------------------------------- #
#  场景 A / C / D / E / F（B、G 见 main 里的 [U] 行）
# --------------------------------------------------------------------------- #

def scen_A(env, cfg, rows):
    """A：下载中 taskkill /F 内核进程 -> 重启 -> 自动续传完成。"""
    k = Check()
    src = Src(b"matrix-A", cfg.size, throttle=cfg.throttle)
    srv, url = file_server(src)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": cfg.conn})
        t = create_start(c, url, cfg.conn)
        tid = t["id"]
        size = t["size"] if t["size"] > 0 else src.size
        at = wait_running_progress(c, tid, int(size * cfg.kill_at), cfg.kill_timeout)
        if not at:
            raise AssertionError("没等到「下载中且已过目标进度」，崩溃现场没造出来")
        persisted = at["downloaded"]
        c.kill()                    # 真·强杀：不给任何优雅保存的机会
        src.throttle = 0.0          # 续传阶段全速（只省时间，不影响字节账）
        c2 = env.start()            # 崩溃现场由新进程恢复
        resumed = [e for e in c2.events if e.get("type") == "resumed"]
        fin = wait_final(c2, tid, cfg.finish_timeout)
        payload, verify, peak = src.snapshot()
        reserved = max(0, payload - size)
        if not fin:
            raise AssertionError("重启后没等到终态（自动续传可能没触发）")
        k.ck(bool(resumed), "重启后没有 resumed 事件：自动续传没触发")
        k.ck(fin["status"] == ST_DONE, "终态不是完成：" + str(fin.get("err")))
        sha_ok = fin["status"] == ST_DONE and file_sha(fin["savePath"]) == src.sha
        k.ck(sha_ok, "SHA-256 与完整下载不一致")
        k.ck(reserved * 20 <= size, "重下 %d B（%.2f%%）> 5%%；2 s 快照窗口预算 %d B"
             % (reserved, 100.0 * reserved / size, window_budget(cfg)))
        rows_append(rows, "A 强杀进程后续传", k.verdict(), reserved, size, sha_ok,
                    "强杀时已持久化 %d B / 共 %d B；重启自动续传 %d 个任务；身份探测开销 %d B%s"
                    % (persisted, size, (resumed[0].get("count") if resumed else 0), verify, k.suffix()))
    finally:
        srv.shutdown()


def scen_C(env, cfg, rows):
    """C：服务器静默 30 s（HTTP 层的拔网线）-> 恢复 -> 任务不断、从断点继续。"""
    k = Check()
    src = Src(b"matrix-C", cfg.size, throttle=cfg.throttle)
    srv, url = file_server(src)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": cfg.conn})
        t = create_start(c, url, cfg.conn)
        tid = t["id"]
        size = t["size"] if t["size"] > 0 else src.size
        tap = EventTap(c)
        at = wait_running_progress(c, tid, int(size * cfg.kill_at), cfg.kill_timeout)
        if not at:
            raise AssertionError("没等到下载中的目标进度")
        before = at["downloaded"]
        src.arm_outage(cfg.outage)
        outage_end = time.time() + cfg.outage
        time.sleep(8)
        mid = c.task(tid)
        during = mid["downloaded"]
        k.ck(mid["status"] == ST_RUNNING,
             "静默第 8 秒任务已不是下载中（status=%s err=%s）" % (mid["status"], mid.get("err")))
        while time.time() < outage_end + 1.5:
            time.sleep(0.5)
        src.throttle = 0.0
        fin = wait_final(c, tid, cfg.finish_timeout)
        payload, verify, peak = src.snapshot()
        reserved = max(0, payload - size)
        retries = tap.count("retry")
        tap.close()
        if not fin:
            raise AssertionError("网络恢复后没等到终态")
        k.ck(during <= before + int(0.02 * size),
             "静默期间进度还在涨（%d -> %d），说明静默没生效" % (before, during))
        k.ck(fin["status"] == ST_DONE,
             "断网后任务没自愈：终态 %s err=%s" % (fin["status"], fin.get("err")))
        sha_ok = fin["status"] == ST_DONE and file_sha(fin["savePath"]) == src.sha
        k.ck(sha_ok, "SHA-256 与完整下载不一致")
        k.ck(reserved * 20 <= size, "重下 %d B（%.2f%%）> 5%%" % (reserved, 100.0 * reserved / size))
        k.ck(retries >= 1, "事件流里没有 retry 事件：静默可能被当成正常慢速，测试有效性存疑")
        rows_append(rows, "C 断网 30 s 后续传", k.verdict(), reserved, size, sha_ok,
                    "静默 %d s：进度 %d -> %d B（不回退）；事件流 retry %d 次；静默中 status=%s%s"
                    % (cfg.outage, before, during, retries, mid["status"], k.suffix()))
    finally:
        srv.shutdown()


def _scen_D(env, cfg, rows, tag, delta, label):
    k = Check()
    src = Src(b"matrix-D" + tag.encode(), cfg.size, throttle=cfg.throttle)
    srv, url = file_server(src)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": cfg.conn})
        t = create_start(c, url, cfg.conn)
        tid = t["id"]
        size = t["size"] if t["size"] > 0 else src.size
        at = wait_running_progress(c, tid, int(size * cfg.kill_at), cfg.kill_timeout)
        if not at:
            raise AssertionError("没等到下载中的目标进度")
        persisted = at["downloaded"]
        c.kill()
        partial = glob_partial(env.dldir)
        p_before, _v, _p = src.snapshot()
        src.mutate(b"mutated-" + tag.encode(), size_delta=delta)
        src.throttle = 0.0
        c2 = env.start()            # 自动续传会立刻撞上源变更
        fin = wait_final(c2, tid, 90)
        payload, verify, peak = src.snapshot()
        after = max(0, payload - p_before)
        if not fin:
            raise AssertionError("改源后 90 s 内没等到终态")
        k.ck(fin["status"] == ST_ERR, "没有拒绝续传：终态 %s（应为错误）" % fin["status"])
        # 内核把同一句人话同时写进 err 与 resumeNote（任务卡片两处都要显示），
        # 取证时按一句看，别把同一段话读两遍。
        err = fin.get("err") or ""
        note_only = fin.get("resumeNote") or ""
        if note_only and note_only != err:
            err = (err + " " + note_only).strip()
        k.ck(any(x in err for x in ("变更", "变化", "替换", "不一致")),
             "错误提示不可操作：" + err[:120])
        k.ck(after < (1 << 20), "拒绝续传之前又传了 %d B" % after)
        k.ck(bool(partial) and os.path.exists(partial), "拒绝续传时把已下载文件删了")
        rows_append(rows, "D " + label, k.verdict(), 0, size, None,
                    "拒绝续传提示「%s」；改源前已下 %d B（文件保留），改源后服务端再发 %d B（探测）%s"
                    % (err.strip()[:56] or "无", persisted, after, k.suffix()))
    finally:
        srv.shutdown()


def scen_D1(env, cfg, rows):
    """D1：源文件长度变了 -> 必须判定不可续传。"""
    _scen_D(env, cfg, rows, "1", MIB, "源文件变长")


def scen_D2(env, cfg, rows):
    """D2：等长替换 + ETag 变了 -> 必须判定不可续传。"""
    _scen_D(env, cfg, rows, "2", 0, "等长替换(ETag 变)")


def scen_E1(env, cfg, rows):
    """E1：Accept-Ranges: none -> 必须回退单连接，且下下来的文件是对的。"""
    k = Check()
    size = max(2 * MIB, cfg.size // 8)
    src = Src(b"matrix-E1", size, ranges=False, throttle=0.02)
    srv, url = file_server(src)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": cfg.conn})
        t = create_start(c, url, cfg.conn)
        tid = t["id"]
        k.ck(t.get("supportRange") is False, "任务竟宣称支持 Range（服务器写的是 Accept-Ranges: none）")
        fin = wait_final(c, tid, cfg.finish_timeout)
        payload, verify, peak = src.snapshot()
        if not fin:
            raise AssertionError("无 Range 服务器上下完超时")
        k.ck(fin["status"] == ST_DONE, "终态不是完成：" + str(fin.get("err")))
        sha_ok = fin["status"] == ST_DONE and file_sha(fin["savePath"]) == src.sha
        k.ck(sha_ok, "SHA-256 与完整下载不一致")
        k.ck(src.payload_peak == 1, "并发**载荷**响应峰值 = %d，没有回退成单连接"
             % src.payload_peak)
        k.ck(not fin.get("chunks"), "回退后仍保留了分块布局：%s" % (fin.get("chunks"),))
        rows_append(rows, "E 无 Range 单连接回退", k.verdict(), max(0, payload - size), size, sha_ok,
                    "回退单连接：并发载荷峰值 %d（全部响应峰值 %d，差值是客户端已放弃的探针响应），"
                    "服务端载荷 %d B / 文件 %d B，身份校验开销 %d B%s"
                    % (src.payload_peak, peak, payload, size, verify, k.suffix()))
    finally:
        srv.shutdown()


def scen_E2(env, cfg, rows):
    """E2：无 Range 服务器上崩溃重启的真实行为（协议上无法续传，重点是不出错）.

    施工图把 E 列入「重下字节 < 5%」，但没有 Range 就不可能续传——这里如实记录
    实际比例，判据改成：完成且字节正确、没有 Range 探测风暴、仍然只有一条连接。
    """
    k = Check()
    size = max(8 * MIB, cfg.size // 4)
    src = Src(b"matrix-E2", size, ranges=False, throttle=cfg.throttle)
    srv, url = file_server(src)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": cfg.conn})
        t = create_start(c, url, cfg.conn)
        tid = t["id"]
        at = wait_running_progress(c, tid, int(size * cfg.kill_at), cfg.kill_timeout)
        if not at:
            raise AssertionError("没等到下载中的目标进度")
        persisted = at["downloaded"]
        c.kill()
        src.throttle = 0.0
        c2 = env.start()
        fin = wait_final(c2, tid, cfg.finish_timeout)
        payload, verify, peak = src.snapshot()
        reserved = max(0, payload - size)
        if not fin:
            raise AssertionError("重启后没等到终态")
        k.ck(fin["status"] == ST_DONE, "重启后没下完：终态 %s err=%s" % (fin["status"], fin.get("err")))
        sha_ok = fin["status"] == ST_DONE and file_sha(fin["savePath"]) == src.sha
        k.ck(sha_ok, "SHA-256 与完整下载不一致")
        k.ck(src.payload_peak == 1, "并发**载荷**响应峰值 = %d，无 Range 服务器上出现了多连接"
             % src.payload_peak)
        k.ck(src.requests < 16, "请求次数 %d 次：无 Range 服务器上疑似反复探测" % src.requests)
        verdict = "PASS*" if not k.bad else "FAIL"
        rows_append(rows, "E2 无 Range + 强杀重启", verdict, reserved, size, sha_ok,
                    "无 Range 无法续传：重启后整文件重发 %d B（%.1f%%，协议限制，判据不适用，已记入待办）；"
                    "杀进程前已下 %d B，请求 %d 次，并发载荷峰值 %d（全部响应峰值 %d）%s"
                    % (reserved, 100.0 * reserved / size, persisted, src.requests,
                       src.payload_peak, peak, k.suffix()))
    finally:
        srv.shutdown()


def scen_F(env, cfg, rows):
    """F：HLS 下载一半杀进程 -> 重启 -> 只补缺失分片（分片粒度续传）."""
    k = Check()
    nseg, segsz = 48, 64 * KIB
    total = nseg * segsz
    srv, url, segs, st = hls_server(nseg, segsz, 0.08)
    try:
        c = env.start()
        c.req("PUT", "/api/v1/config", {"defaultConn": 2})
        t = create_start(c, url, 2)
        tid = t["id"]
        at = wait_running_progress(c, tid, int(total * 0.35), cfg.kill_timeout)
        if not at:
            raise AssertionError("没等到 HLS 下载中的目标进度")
        pd = glob_parts_dir(env.dldir)
        done_before = sorted(int(n[4:]) for n in os.listdir(pd) if n.startswith("seg-")) if pd else []
        hits_before = list(st["hits"])
        c.kill()
        st["throttle"] = 0
        c2 = env.start()
        fin = wait_final(c2, tid, cfg.finish_timeout)
        hits_after = list(st["hits"])
        refetched = [i for i in done_before if hits_after[i] > hits_before[i]]
        reserved = max(0, st["payload"] - total)
        if not fin:
            raise AssertionError("重启后没等到 HLS 终态")
        k.ck(bool(done_before), "杀进程时没有已完成分片，现场没造出来")
        k.ck(not refetched, "重启后重复下载了已完成分片：%s" % (refetched,))
        k.ck(fin["status"] == ST_DONE, "重启后没下完：终态 %s err=%s" % (fin["status"], fin.get("err")))
        out = fin.get("savePath") or ""
        if out.endswith(".ts"):
            sha_ok = os.path.exists(out) and file_sha(out) == hashlib.sha256(b"".join(segs)).hexdigest()
            k.ck(sha_ok, "合并后的 .ts 与分片拼接不一致")
        else:
            sha_ok = None  # 有 ffmpeg 时产出 .mp4（重封装容器，SHA 天然不等于 TS 拼接）
            k.ck(bool(out) and os.path.exists(out) and os.path.getsize(out) > total // 2,
                 "输出文件不对：" + out)
        inflight = reserved - sum(segsz for _ in refetched)
        k.ck(inflight * 20 <= total, "在途分片重取 %d B（%.2f%%）> 5%%" % (inflight, 100.0 * inflight / total))
        rows_append(rows, "F HLS 半程杀进程续传", k.verdict(), reserved, total, sha_ok,
                    "杀进程时已完成 %d/%d 分片，重启后重复请求已完成分片 %d 个；重发字节全部来自在途分片 %d B；"
                    "输出 %s%s" % (len(done_before), nseg, len(refetched), inflight,
                                  os.path.basename(out) or "无", k.suffix()))
    finally:
        srv.shutdown()


SCENARIOS = [
    ("A", scen_A),
    ("C", scen_C),
    ("D1", scen_D1),
    ("D2", scen_D2),
    ("E1", scen_E1),
    ("E2", scen_E2),
    ("F", scen_F),
]


def run_go_matrix(rows):
    """--go：跑 Go 侧矩阵/续传相关单测（场景 D 全变体、B 白名单、G 磁力位图、F HLS）."""
    pat = "Matrix|Resume|Snapshot|Graceful|HLSResume|BTResume"
    cmd = ["go", "test", "-count=1", "-run", pat,
           "./internal/download/...", "./internal/model/...", "./internal/store/..."]
    try:
        r = subprocess.run(cmd, cwd=os.path.join(ROOT, "core"), capture_output=True,
                           text=True, encoding="utf-8", errors="replace", timeout=900)
    except Exception as e:  # noqa: BLE001
        rows_append(rows, "Go 侧矩阵（--go）", "FAIL", None, 0, None, "go test 没跑起来：" + repr(e))
        return
    out = ((r.stdout or "") + "\n" + (r.stderr or "")).splitlines()
    keep = " ; ".join(l.strip() for l in out if l.strip().startswith(("ok ", "FAIL", "---")))[:320]
    rows_append(rows, "Go 侧矩阵（--go）", "PASS" if r.returncode == 0 else "FAIL", None, 0, None,
                "go test -run Matrix/Resume/Snapshot/Graceful/HLSResume/BTResume exit=%d；%s"
                % (r.returncode, keep or "（无匹配用例输出）"))


def print_table(rows):
    print("")
    print("| 场景 | 结论 | 重下字节 | 占比 | SHA-256 | 说明 |")
    print("|---|---|---|---|---|---|")
    for r in rows:
        print("| %s | %s | %s | %s | %s | %s |" % (
            r["scen"], r["verdict"], r["reserved"], r["pct"], r["sha"], r["note"]))
    print("")


# --only 过滤：前缀匹配，空列表 = 全跑。
def wanted(only, name):
    return not only or any(name.upper().startswith(x) for x in only)


def main():
    ap = argparse.ArgumentParser(description="Sun_Tey M1.5 断点续传矩阵（真进程 + taskkill /F）")
    ap.add_argument("--size-mb", type=int, default=32, help="主场景文件大小（MiB，默认 32）")
    ap.add_argument("--conn", type=int, default=4, help="下载连接数（默认 4）")
    ap.add_argument("--throttle", type=float, default=0.09,
                    help="服务端每 16 KiB 睡眠秒数（决定 2 s 快照窗口占比）")
    ap.add_argument("--kill-at", type=float, default=0.25, help="下载到多少比例时动手（默认 0.25）")
    ap.add_argument("--outage", type=int, default=32, help="场景 C 的静默秒数（默认 32，> 内核 30 s 空闲阈值）")
    ap.add_argument("--only", default="", help="只跑指定场景，逗号分隔，如 A,C,D")
    ap.add_argument("--fast", action="store_true", help="小样本冒烟（重下占比会变大，判据以默认尺寸为准）")
    ap.add_argument("--go", action="store_true", help="追加跑 Go 侧矩阵单测")
    ap.add_argument("--keep", action="store_true", help="保留临时 store/下载目录")
    a = ap.parse_args()

    cfg = Cfg()
    cfg.size = a.size_mb * MIB
    cfg.conn = a.conn
    cfg.throttle = a.throttle
    cfg.kill_at = a.kill_at
    cfg.outage = a.outage
    if a.fast:
        cfg.size = 4 * MIB
        cfg.throttle = 0.02
        print("[fast] 小样本冒烟：重下占比会显著变大（2 s 快照窗口不随之缩小），判据以默认尺寸的运行结果为准")

    only = [x.strip().upper() for x in a.only.split(",") if x.strip()]
    root = os.path.join(tempfile.gettempdir(), "suntey-m15-%d" % os.getpid())
    os.makedirs(root, exist_ok=True)
    print("Sun_Tey M1.5 断点续传矩阵")
    print("  内核   : " + EXE)
    print("  临时目录: " + root)
    print("  参数   : size=%d MiB conn=%d throttle=%.3f kill_at=%.2f outage=%ds"
          % (cfg.size // MIB, cfg.conn, cfg.throttle, cfg.kill_at, cfg.outage))
    print("  2 s 快照窗口预算: %d B（%s）"
          % (window_budget(cfg), "占总大小 %.2f%%" % (100.0 * window_budget(cfg) / cfg.size) if cfg.size else "-"))

    rows = []
    t0 = time.time()
    try:
        for name, fn in SCENARIOS:
            if not wanted(only, name):
                continue
            rows_append_pending = len(rows)
            env = Env(name, root)
            ts = time.time()
            try:
                fn(env, cfg, rows)
            except Exception as e:  # noqa: BLE001 - 一个场景炸了不影响后面的场景
                if len(rows) == rows_append_pending:
                    rows_append(rows, name + " 场景", "FAIL", None, cfg.size, None, "场景异常：" + str(e)[:220])
                else:
                    rows[-1]["verdict"] = "FAIL"
                    rows[-1]["note"] += "；场景收尾异常：" + str(e)[:180]
            finally:
                env.close()
            print("  [%-2s] %-26s %6.1fs  %s"
                  % (name, rows[-1]["verdict"], time.time() - ts, rows[-1]["note"][:110]))

        if wanted(only, "B"):
            rows_append(rows, "B 整机重启后自动续传", "[U]", None, 0, None,
                        "脚本不重启真机。等价路径已由 A 覆盖（进程消失 -> Running 修正为 Paused(crash) -> AutoResume 捞回），"
                        "「用户暂停的绝不自动开跑」由 Go 单测 TestMatrixAutoResumeSkipsUserPaused 覆盖。"
                        "人工验证：下载中直接重启系统，开机后任务应自动接着下")
        if wanted(only, "G"):
            rows_append(rows, "G 磁力 piece 位图恢复", "[U]", None, 0, None,
                        "需真实 swarm，脚本不造种子。Go 侧 TestBTResumeAfterCancel 覆盖取消/重开不重复校验已完成 piece"
                        "（--go 会跑）。人工验证：公开种子下到一半 taskkill /F 后重启，应只补缺失 piece，不做全文件重下")

        if a.go:
            run_go_matrix(rows)
    finally:
        if not a.keep:
            try:
                shutil.rmtree(root, ignore_errors=True)
            except Exception:
                pass

    print_table(rows)
    fails = [r for r in rows if "FAIL" in r["verdict"]]
    print("耗时 %.1fs；结论：PASS %d / PASS* %d / [U] %d / FAIL %d"
          % (time.time() - t0,
             len([r for r in rows if r["verdict"] == "PASS"]),
             len([r for r in rows if r["verdict"] == "PASS*"]),
             len([r for r in rows if r["verdict"] == "[U]"]),
             len(fails)))
    if fails:
        for r in fails:
            print("  FAIL -> " + r["scen"] + "：" + r["note"])
        return 1
    print("M1.5 续传矩阵通过：强杀/断网/改源/无 Range/HLS 全部按契约处理，最终字节与完整下载一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
