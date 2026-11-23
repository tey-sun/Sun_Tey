#!/usr/bin/env python3
"""E2E: app-style window close (graceful shutdown) then resume.
Flow: slow Range server -> core serve (port 0) -> create+start task ->
wait 1.5s -> POST /shutdown (like lib.rs does) -> wait core exit ->
relaunch core -> GET /tasks must show Paused with Downloaded>0 ->
start again -> poll to Complete -> sha256 + assert server never saw a
full re-download after the first pause (total served <= size + slack).
"""
import hashlib, json, os, re, socket, subprocess, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = 3_000_000
CHUNK = 16 * 1024
DATA = (hashlib.sha256(b"suntey-graceful").digest() * 60)[:SIZE]
served_total = 0
lock = threading.Lock()

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive (Content-Length set): no socket churn
    def log_message(self, *a): pass
    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError):
            self.close_connection = True
    def do_GET(self):
        global served_total
        rng = self.headers.get("Range", "")
        if rng == "bytes=0-0":
            # range-support probe: answer instantly (status line BEFORE headers)
            self.send_response(206)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Range", f"bytes 0-0/{SIZE}")
            self.send_header("Content-Length", "1")
            self.end_headers()
            try: self.wfile.write(DATA[:1])
            except (BrokenPipeError, ConnectionResetError): pass
            return
        m = re.match(r"bytes=(\d*)-(\d*)$", rng)
        if m:
            s = int(m.group(1) or 0)
            e = int(m.group(2)) if m.group(2) else SIZE - 1
            e = min(e, SIZE - 1)
            if s > e:
                self.send_response(416); self.end_headers(); return
            body = DATA[s:e+1]
            with lock: served_total += len(body)
            self.send_response(206)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Range", f"bytes {s}-{e}/{SIZE}")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for i in range(0, len(body), 16384):
                    self.wfile.write(body[i:i+16384])
                    self.wfile.flush()
                    time.sleep(0.04)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
        else:
            with lock: served_total += len(DATA)
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(SIZE))
            self.end_headers()
            try:
                for i in range(0, len(DATA), 4096):
                    self.wfile.write(DATA[i:i+4096])
                    self.wfile.flush()
                    time.sleep(0.04)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

def _serve():
    try:
        srv.serve_forever()
    except Exception as e:
        print("[srv thread died]", repr(e), flush=True)
        raise

srv = Server(("127.0.0.1", 0), H)
threading.Thread(target=_serve, daemon=True).start()
URL = f"http://127.0.0.1:{srv.server_address[1]}/big.bin"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")

class Core:
    def __init__(self, store):
        self.errfile = open(store + ".err", "wb")
        self.p = subprocess.Popen([EXE, "serve", "--store", store],
            stdout=subprocess.PIPE, stderr=self.errfile, text=True, encoding="utf-8")
        self.base = None; self.token = None
        line = self.p.stdout.readline()
        d = json.loads(line)
        assert d.get("type") == "listening", line
        self.base = f"http://127.0.0.1:{d['port']}"
        self.token = d.get("token") or ""
    def req(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if self.token: r.add_header("X-SunTey-Token", self.token)
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                return json.loads(resp.read() or b'{"ok":true}')
        except Exception as e:
            return {"error": str(e)}
    def shutdown_like_app(self):
        """Exact wire behaviour of lib.rs graceful_shutdown."""
        hostport = self.base.split("//")[1]
        port = int(hostport.split(":")[1])
        s = socket.create_connection(("127.0.0.1", port), timeout=1)
        s.settimeout(1.5)
        req = (f"POST /api/v1/shutdown HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
               f"X-SunTey-Token: {self.token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        s.sendall(req.encode())
        ack = s.recv(128)
        s.close()
        return ack
    def wait_exit(self, secs):
        try:
            self.p.wait(timeout=secs); return True
        except subprocess.TimeoutExpired:
            self.p.kill(); return False

import os, tempfile
STORE = os.path.join(tempfile.gettempdir(), f"suntey-e2e-{os.getpid()}.db")

# kill orphan test cores from earlier crashed runs (skip children of Sun_Tey.exe)
try:
    import subprocess as _sp, csv as _csv
    out = _sp.check_output(["wmic", "process", "where", "name='suntey-core.exe'",
                            "get", "ProcessId,ParentProcessId,CommandLine", "/format:csv"],
                           text=True, errors="ignore")
    me = _sp.check_output(["tasklist", "/FI", "IMAGENAME eq Sun_Tey.exe", "/FO", "CSV", "/NH"],
                          text=True, errors="ignore")
    suntey_pids = {ln.split('","')[1] for ln in me.splitlines() if '"' in ln}
    for row in _csv.DictReader(out.splitlines()):
        if not row.get("ProcessId", "").strip().isdigit():
            continue
        if row["ParentProcessId"].strip() in suntey_pids:
            continue
        _sp.run(["taskkill", "/F", "/PID", row["ProcessId"].strip()],
                stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
except Exception:
    pass

def unwrap(resp):
    if resp.get("ok") is not True:
        print("core error:", resp); raise SystemExit(1)
    return resp["data"]

c = Core(STORE)
c.req("PUT", "/api/v1/config", {"defaultConn": 2})
rid = unwrap(c.req("POST", "/api/v1/resolve", {"url": URL}))
tid = unwrap(c.req("POST", "/api/v1/tasks", {"url": URL, "rid": rid}))["id"]
c.req("POST", "/api/v1/tasks/start", {"id": tid})
t = None
for _ in range(40):
    time.sleep(0.4)
    t = [x for x in unwrap(c.req("GET", "/api/v1/tasks")) if x["id"] == tid][0]
    if t["downloaded"] > 200_000:
        break
paused_at = t["downloaded"]
print("phase1 at", paused_at, "status", t["status"], flush=True)
assert 200_000 <= paused_at < SIZE, f"progress not in window: {paused_at}"
ack = c.shutdown_like_app()
print("ack:", ack[:40], flush=True)
assert c.wait_exit(5), "core did not self-exit"

c2 = Core(STORE)
t = [x for x in unwrap(c2.req("GET", "/api/v1/tasks")) if x["id"] == tid][0]
print("after-close:", t["status"], t["downloaded"], "/", t["size"], flush=True)
assert t["status"] == 4, f"expected paused(4), got {t['status']} err={t.get('err')}"
assert t["downloaded"] == paused_at, f"progress lost: {t['downloaded']} != {paused_at}"
served_mid = served_total
c2.req("POST", "/api/v1/tasks/start", {"id": tid})
for i in range(240):
    t = [x for x in unwrap(c2.req("GET", "/api/v1/tasks")) if x["id"] == tid][0]
    if t["status"] in (5, 6):
        break
    if i % 20 == 0:
        print("poll", i, t["status"], t["downloaded"], flush=True)
    time.sleep(0.35)
assert t["status"] == 5, f"expected complete(5), got {t['status']} err={t.get('err')}"
resumed = served_total - served_mid
limit = SIZE - paused_at + 8192
print(f"resumed served {resumed} limit {limit}", flush=True)
assert resumed <= limit, "resume re-downloaded finished bytes"
raw = open(t["savePath"], "rb").read()
assert len(raw) == SIZE and hashlib.sha256(raw).hexdigest() == hashlib.sha256(DATA).hexdigest(), "bytes mismatch"
print("bytes exact, size", len(raw), flush=True)

srv.shutdown()
for suf in ("", "-wal"):
    try: os.remove(STORE + suf)
    except OSError: pass
print("GRACEFUL E2E OK — 关窗即暂停保进度，重开续传不重下，文件字节精确")
