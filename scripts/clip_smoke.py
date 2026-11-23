#!/usr/bin/env python3
"""clip_smoke.py — IDM-style clipboard monitor end-to-end on a packaged core.

Starts `suntey-core.exe serve` on a temp store/dir, serves one zip over
loopback, then verifies the watcher chain: copy a link -> task auto-created ->
completes; switch clipboardWatch off -> copy stops creating tasks. Preserves
the user's clipboard (saved and restored around the run).

Usage:
  SUNTEY_CORE=<abs path to suntey-core.exe> python scripts/clip_smoke.py
Exit 0 = CLIP SMOKE PASS.
"""
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")
PORT = 17900
TOKEN = "clip-smoke-token"
PAYLOAD = os.urandom(200 * 1024)


def powershell(script):
    return subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True, text=True, timeout=15)


def get_clipboard():
    r = powershell("Get-Clipboard -Raw")
    return r.stdout if r.returncode == 0 else ""


def set_clipboard(value):
    r = powershell(f"Set-Clipboard -Value '{value.replace(chr(39), chr(39)*2)}'")
    if r.returncode != 0:
        raise SystemExit(f"Set-Clipboard failed: {r.stderr[:120]}")


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        rng = self.headers.get("Range")
        if not self.path.endswith(".zip"):
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
            self.end_headers()
            self.wfile.write(chunk)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(PAYLOAD)


def api(method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}/api/v1{path}", method=method)
    req.add_header("X-SunTey-Token", TOKEN)
    data = json.dumps(body).encode() if body is not None else None
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data, timeout=15) as r:
            raw = r.read().decode()
    except Exception as e:
        raise SystemExit(f"{method} {path} -> {e}")
    obj = json.loads(raw)
    return obj["data"] if isinstance(obj, dict) and "data" in obj else obj


def find_task(url_frag):
    for t in api("GET", "/tasks") or []:
        if url_frag in (t.get("req") or {}).get("url", ""):
            return t
    return None


def main():
    if not os.path.isfile(CORE):
        raise SystemExit(f"core exe not found: {CORE}")
    srv = ThreadingHTTPServer(("127.0.0.1", 9095), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    workdir = tempfile.mkdtemp(prefix="suntey-clip-")
    store = os.path.join(workdir, "clip.db")
    saved = get_clipboard()
    env = dict(os.environ, TORRENT_STORAGE_DEFAULT_FILE_IO="classic")
    proc = None
    try:
        proc = subprocess.Popen(
            [CORE, "serve", "--port", str(PORT), "--token", TOKEN,
             "--store", store, "--dir", workdir],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", PORT), 0.2):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            raise SystemExit("core serve did not bind port")

        cfg = api("GET", "/config")
        watch = cfg.get("clipboardWatch")
        print(f"config clipboardWatch = {watch!r} (nil = enabled by default)")

        # 1) copy a plain link -> watcher must create and complete a task
        set_clipboard("http://127.0.0.1:9095/clip1.zip")
        t = None
        for _ in range(20):
            time.sleep(0.5)
            t = find_task("clip1.zip")
            if t:
                break
        if not t:
            raise SystemExit("no task auto-created after copying a link (10s)")
        print(f"watcher created task {t['id']} status={t['status']}")
        for _ in range(20):
            time.sleep(0.5)
            t = find_task("clip1.zip")
            if t and t["status"] in (5, 6):
                break
        if not t or t["status"] != 5:
            raise SystemExit(f"clip1 task did not complete: {t}")
        print("clip1 completed via clipboard watcher ✓")

        # 2) switch the watcher off -> copying no longer creates tasks
        api("PUT", "/config", {"clipboardWatch": False})
        time.sleep(0.3)
        set_clipboard("http://127.0.0.1:9095/clip2.zip")
        time.sleep(4.5)
        if find_task("clip2.zip"):
            raise SystemExit("task created while clipboardWatch=false")
        print("switch off: copy while disabled creates no task ✓")

        print("CLIP SMOKE PASS")
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
        try:
            set_clipboard(saved[:2000])
        except Exception:
            pass
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
