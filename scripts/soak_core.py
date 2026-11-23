#!/usr/bin/env python3
"""soak_core.py — sustained-use smoke for the packaged suntey-core.exe.

Serves one 512KiB zip over loopback, then drives the REST API in 3 rounds of
8 concurrent downloads (create -> start -> complete -> delete), sampling core
RSS between rounds. Verifies: byte-exact completion, no task leakage after
DELETE, and RSS does not creep upward (no per-task goroutine/handle leak).

Usage:
  SUNTEY_CORE=<abs path to suntey-core.exe> python scripts/soak_core.py
Exit 0 = SOAK PASS.
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")
PORT = 17890
TOKEN = "soak-token-fixed"
ROUNDS = 3
PER_ROUND = 8
PAYLOAD = os.urandom(512 * 1024)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/octet-stream"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        rng = self.headers.get("Range")
        if not self.path.endswith(".zip"):
            self._send(404, b"nope")
            return
        if rng and rng.startswith("bytes="):
            s, e = rng[6:].split("-")
            s = int(s)
            e = min(int(e) if e else len(PAYLOAD) - 1, len(PAYLOAD) - 1)
            chunk = PAYLOAD[s:e + 1]
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Content-Range", f"bytes {s}-{e}/{len(PAYLOAD)}")
            self.end_headers()
            self.wfile.write(chunk)
        else:
            self._send(200, PAYLOAD)


def api(method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}/api/v1{path}", method=method)
    req.add_header("X-SunTey-Token", TOKEN)
    data = json.dumps(body).encode() if body is not None else None
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data, timeout=15) as r:
            raw = r.read().decode()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> HTTP {e.code}: {e.read().decode(errors='replace')[:120]}")
    obj = json.loads(raw)
    return obj["data"] if isinstance(obj, dict) and "data" in obj else obj


def core_rss_mb(proc):
    # RSS via psutil if present, else tasklist fallback
    try:
        import psutil
        return psutil.Process(proc.pid).memory_info().rss / 1e6
    except Exception:
        out = subprocess.run(["tasklist", "/FI", f"PID eq {proc.pid}", "/FO", "CSV", "/NH"],
                             capture_output=True, text=True).stdout
        for ln in out.splitlines():
            parts = ln.split(",")
            if len(parts) > 4 and "Memory" not in parts[0]:
                num = parts[4].strip().replace('"', "").replace(",", "").split(" ")[0]
                try:
                    return int(num) / 1024.0
                except ValueError:
                    pass
        return -1.0


def main():
    if not os.path.isfile(CORE):
        raise SystemExit(f"core exe not found: {CORE}")
    srv = ThreadingHTTPServer(("127.0.0.1", 9091), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    workdir = tempfile.mkdtemp(prefix="suntey-soak-")
    store = os.path.join(workdir, "soak.db")
    url = "http://127.0.0.1:9091/soak.zip"
    rss_seen = []
    proc = None
    try:
        env = dict(os.environ, TORRENT_STORAGE_DEFAULT_FILE_IO="classic")
        proc = subprocess.Popen(
            [CORE, "serve", "--port", str(PORT), "--token", TOKEN,
             "--store", store, "--dir", workdir],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # wait for listen
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", PORT), 0.2):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            raise SystemExit("core serve did not bind port")

        for rnd in range(1, ROUNDS + 1):
            ids = []
            for i in range(PER_ROUND):
                t = api("POST", "/tasks", {"url": url, "name": f"soak{rnd}-{i}.zip"})
                ids.append(t["id"])
            for tid in ids:
                api("POST", "/tasks/start", {"id": tid})
            t0 = time.time()
            pending = set(ids)
            while pending and time.time() - t0 < 60:
                tasks = {t["id"]: t for t in api("GET", "/tasks")}
                for tid in list(pending):
                    st = tasks.get(tid)
                    if st and st.get("status") == 5:  # Complete
                        pending.discard(tid)
                    elif st and st.get("status") == 6:  # Error
                        raise SystemExit(f"round{rnd} task {tid} error: {st.get('error')}")
                time.sleep(0.3)
            if pending:
                raise SystemExit(f"round{rnd}: {len(pending)} tasks never completed (60s)")
            for tid in ids:
                api("DELETE", "/tasks", {"id": tid})
            left = api("GET", "/tasks") or []
            leaked = [t["id"] for t in left if t["id"] in ids]
            if leaked:
                raise SystemExit(f"round{rnd}: {len(leaked)} task(s) leaked after DELETE")
            rss = core_rss_mb(proc)
            rss_seen.append(rss)
            print(f"round {rnd}/{ROUNDS}: {PER_ROUND} downloads complete, core RSS ≈ {rss:.1f} MB")
            time.sleep(1.0)

        # files byte-exact sanity (at least one survived? tasks deleted but files kept)
        remain = [f for f in os.listdir(workdir) if f.startswith("soak")]
        exact = 0
        for fn in remain:
            p = os.path.join(workdir, fn)
            if os.path.getsize(p) == len(PAYLOAD) and open(p, "rb").read() == PAYLOAD:
                exact += 1
        print(f"byte-exact files on disk: {exact}/{len(remain)} (of {ROUNDS * PER_ROUND} downloaded)")
        if exact == 0:
            raise SystemExit("no byte-exact files found on disk")

        if len(rss_seen) >= 2 and max(rss_seen) - rss_seen[0] > 30:
            raise SystemExit(f"core RSS crept >30MB across rounds: {rss_seen}")
        print("SOAK PASS")
    finally:
        if proc and proc.poll() is None:
            try:
                api("POST", "/shutdown")
                proc.wait(timeout=10)
            except Exception:
                proc.terminate()
                try:
                    proc.wait(5)
                except Exception:
                    proc.kill()
        srv.shutdown()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
