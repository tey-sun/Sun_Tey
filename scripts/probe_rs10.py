"""Resume-after-pause slowness probe: HTTP/1.0 mock (exact UI smoke ZipHandler,
no protocol_version override) + conn=1 + rate32. SSE progress tick cadence is
the truth: 2s stalls => fixture keep-alive death theory (product OK-ish);
500ms continuous => resume is genuinely rate-throttled slow (product bug)."""
import http.server
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

CORE = os.path.abspath(sys.argv[1])
ZIP = 9098
CORE_PORT = 17339
TOKEN = "probe-rs10"
PAYLOAD = os.urandom(240 * 1024)


T0 = time.time()


class ZipHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive like a real server; Connection: close per response
    """EXACT copy of ui_browser_smoke.py ZipHandler (HTTP/1.0 default)."""

    def log_message(self, fmt, *a):
        print(f"  zip[{time.time()-T0:5.1f}s] {self.command} {self.path[:40]} {fmt % a}", flush=True)

    def do_GET(self):
        rng = self.headers.get("Range")
        path = self.path.split("?", 1)[0]
        if not path.endswith(".zip"):
            self.send_response(404)
            self.end_headers()
            return
        if rng and rng.startswith("bytes="):
            s, e = rng[6:].split("-")
            s = int(s)
            e = min(int(e) if e else len(PAYLOAD) - 1, len(PAYLOAD) - 1)
            chunk = PAYLOAD[s:e + 1]
            self.send_response(206)
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Content-Range", f"bytes {s}-{e}/{len(PAYLOAD)}")
            self.send_header("Connection", "close")
            self.end_headers()
            for off in range(0, len(chunk), 8192):  # streamed write, real-server style
                self.wfile.write(chunk[off:off + 8192])
                self.wfile.flush()
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Connection", "close")
            self.end_headers()
            for off in range(0, len(PAYLOAD), 8192):
                self.wfile.write(PAYLOAD[off:off + 8192])
                self.wfile.flush()

    def do_HEAD(self):
        if self.path.split("?", 1)[0].endswith(".zip"):
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Connection", "close")
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


def api(method, path, body=None, timeout=15):
    req = urllib.request.Request(f"http://127.0.0.1:{CORE_PORT}/api/v1{path}", method=method)
    req.add_header("X-SunTey-Token", TOKEN)
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(req, data, timeout=timeout) as r:
        raw = r.read().decode()
    obj = json.loads(raw)
    return obj["data"] if isinstance(obj, dict) and "data" in obj else obj


def main():
    tmp = os.path.abspath(tempfile.mkdtemp(prefix="probe_rs10_"))
    logf = open(os.path.join(tmp, "core.log"), "w", encoding="utf-8", errors="replace")
    zsrv = http.server.ThreadingHTTPServer(("127.0.0.1", ZIP), ZipHandler)
    threading.Thread(target=zsrv.serve_forever, daemon=True).start()
    proc = subprocess.Popen(
        [CORE, "serve", "--port", str(CORE_PORT), "--token", TOKEN,
         "--store", os.path.join(tmp, "suntey.db"), "--dir", tmp],
        stdout=logf, stderr=subprocess.STDOUT, text=True, cwd=tmp)
    try:
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", CORE_PORT), 0.2).close()
                break
            except OSError:
                time.sleep(0.2)
        api("PUT", "/config", {"rateLimitKBps": 32, "defaultConn": 1})
        url = f"http://127.0.0.1:{ZIP}/ui1.zip?pause=1"
        pid = api("POST", "/tasks", {"url": url})["id"]

        def get_task():
            return next((x for x in api("GET", "/tasks") if x.get("id") == pid), {"status": 0})

        t0 = time.time()
        api("POST", "/tasks/start", {"id": pid})
        while time.time() - t0 < 2.0:
            if get_task().get("downloaded", 0) > 32768:
                break
            time.sleep(0.1)
        api("POST", "/tasks/stop", {"id": pid})
        for _ in range(60):
            if get_task().get("status") == 4:
                break
            time.sleep(0.2)
        print(f"paused at {time.time()-t0:5.1f}s dl={get_task().get('downloaded')}", flush=True)

        # SSE progress ticks (only progress lines w/ timestamp — this is truth)
        stop_sse = threading.Event()

        def sse():
            try:
                with urllib.request.urlopen(
                        f"http://127.0.0.1:{CORE_PORT}/api/v1/events?token={TOKEN}", timeout=80) as r:
                    for line in r:
                        if stop_sse.is_set():
                            return
                        line = line.decode().strip()
                        if line.startswith("data:"):
                            try:
                                ev = json.loads(line[5:])
                            except Exception:
                                continue
                            if ev.get("taskId") != pid:
                                continue
                            ty = ev.get("type")
                            if ty == "progress":
                                print(f"  prog[{time.time()-t0:5.1f}s] dl={ev.get('downloaded')} sp={round(ev.get('speed') or 0)}", flush=True)
                            elif ty in ("status", "retry", "error", "stalled", "done", "downgrade"):
                                print(f"  EVT [{time.time()-t0:5.1f}s] {ty} attempt={ev.get('attempt')} lvl={ev.get('level')} st={ev.get('status')} err={str(ev.get('err'))[:60]}", flush=True)
            except Exception as e:
                print("  sse end:", repr(e)[:80], flush=True)

        th = threading.Thread(target=sse, daemon=True)
        th.start()
        api("POST", "/tasks/start", {"id": pid})
        print(f"resumed at {time.time()-t0:5.1f}s", flush=True)
        final = None
        for i in range(80):
            tt = get_task()
            st = tt.get("status")
            if st in (5, 6):
                final = tt
                break
            time.sleep(1)
        stop_sse.set()
        if final is None:
            tt = get_task()
            print(f"NOT DONE after {time.time()-t0:5.1f}s: status={tt.get('status')} dl={tt.get('downloaded')}", flush=True)
        else:
            print(f"DONE {final.get('status')} at {time.time()-t0:5.1f}s dl={final.get('downloaded')}", flush=True)
    finally:
        try:
            api("POST", "/shutdown", timeout=3)
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        zsrv.shutdown()
        logf.close()
    print("TMP", tmp, flush=True)


if __name__ == "__main__":
    main()
