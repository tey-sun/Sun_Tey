"""Scenario probe: replicate ui_browser_smoke's multi-task state, then stop the
running task at the core level, and time the settle. If this hangs like the UI
smoke does while repro_rate_stop passes, the extra tasks (paused + complete)
are the differentiator."""
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

CORE = sys.argv[1]
RATE = 32
ZIP = 245760

def api(method, port, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}/api/v1{path}",
                                 data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          "X-SunTey-Token": "probe3"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read().decode()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:120]}")
    return json.loads(raw).get("data") if raw else None


def main():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Zip(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            rng = self.headers.get("Range")
            if rng:
                a, b = rng.replace("bytes=", "").split("-")
                lo = int(a); hi = int(b) if b else ZIP - 1
                hi = min(hi, ZIP - 1)
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {lo}-{hi}/{ZIP}")
                self.send_header("Content-Length", str(hi - lo + 1))
                self.end_headers()
                self.wfile.write(b"x" * (hi - lo + 1))
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(ZIP))
                self.end_headers()
                self.wfile.write(b"x" * ZIP)

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(ZIP))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Zip)
    threading = __import__("threading")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    zport = srv.server_address[1]

    tmp = tempfile.mkdtemp(prefix="suntey-stop3-")
    core = subprocess.Popen(
        [CORE, "serve", "--store", os.path.join(tmp, "probe3.db"),
         "--dir", os.path.join(tmp, "dl"), "--port", "0", "--token", "probe3"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    hello = json.loads(core.stdout.readline())
    port = hello["port"]
    try:
        api("PUT", port, "/config", {"rateLimitKBps": RATE, "conn": 8})

        def mk(name, start=True, wait_status=None):
            t = api("POST", port, "/tasks", {"url": f"http://127.0.0.1:{zport}/{name}.zip"})
            api("POST", port, "/tasks/start", {"id": t["id"]})
            return t["id"]

        # 1) seed task, immediately paused
        a = mk("aaa")
        time.sleep(1.2)
        api("POST", port, "/tasks/stop", {"id": a})
        for _ in range(40):
            ta = next(t for t in api("GET", port, "/tasks") if t["id"] == a)
            if ta["status"] == 4:
                break
            time.sleep(0.2)
        print(f"seed aaa -> status {ta['status']} (want 4), settled ok")

        # 2) complete one at reduced rate cap so it finishes
        api("PUT", port, "/config", {"rateLimitKBps": 0})
        b = mk("ui2")
        for _ in range(60):
            tb = next(t for t in api("GET", port, "/tasks") if t["id"] == b)
            if tb["status"] in (5, 6):
                break
            time.sleep(0.4)
        print(f"ui2 -> status {tb['status']} (want 5)")
        api("PUT", port, "/config", {"rateLimitKBps": RATE})

        # 3) the running task; pause mid-flight and time settle
        c = mk("ui1")
        time.sleep(2.0)
        t0 = time.time()
        api("POST", port, "/tasks/stop", {"id": c})
        settled = None
        for _ in range(150):  # 30s
            tc = next(t for t in api("GET", port, "/tasks") if t["id"] == c)
            if tc["status"] in (4, 5, 6):
                settled = time.time() - t0
                break
            time.sleep(0.2)
        st = tc["status"]
        print(f"ui1 stop: status {st} settled={settled and round(settled, 1)}s (want 4, <2s)")
        if settled is None or settled > 2.5:
            raise SystemExit(f"STOP SLOW/LOST in multi-task state: status {st} settle {settled}")
        print("STOP3 PROBE PASS")
    finally:
        try:
            api("POST", port, "/shutdown", {})
        except Exception:
            pass
        core.wait(timeout=10)
        srv.shutdown()


if __name__ == "__main__":
    sys.exit(main())
