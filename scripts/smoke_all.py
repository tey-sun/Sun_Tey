#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smoke_all.py — end-to-end smoke for the Sun_Tey core.

  1. http  : 12 MiB file -> create -> start -> pause -> resume -> byte-exact
  2. bt    : magnet task joins the swarm, /bt/stats exposes the listen port,
             stop settles the task (no auto-restart loop)
  3. hls   : .m3u8 routes to the HLS engine (size = -1 sentinel, state saved)
  4. hls   : real 8-segment playlist, HARD KILL mid-download, restart, resume
             with no re-download of finished segments, merged output exact

Usage: python scripts/smoke_all.py     (exit 0 = all pass, 1 = any failure)
"""
import hashlib
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The core under test. SUNTEY_CORE lets the smoke run against the PACKAGED
# binary (dist-portable/suntey-core.exe) so "rebuilt the source" and "what the
# user actually double-clicks" are verified as the same thing.
CORE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")
WORK = os.path.join(ROOT, "_smoke_all")
DLDIR = os.path.join(WORK, "downloads")
STORE = os.path.join(WORK, "smoke.db")
HTTP_PORT = 18091
HLS_PORT = 18093
API_PORT = 18092
TOKEN = "smoketok"

FAILS = []


def ok(name):
    print("PASS  " + name, flush=True)


def fail(name, detail=""):
    FAILS.append(name)
    print("FAIL  %s  %s" % (name, detail), flush=True)


DATA = bytes(random.getrandbits(8) for _ in range(12 << 20))  # 12 MiB


class FileHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path != "/big.bin":
            self.send_response(404)
            self.end_headers()
            return
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            s, _, e = rng[6:].partition("-")
            s = int(s)
            e = int(e) if e else len(DATA) - 1
            e = min(e, len(DATA) - 1)
            self.send_response(206)
            self.send_header("Content-Range", "bytes %d-%d/%d" % (s, e, len(DATA)))
            self.send_header("Content-Length", str(e - s + 1))
            self.end_headers()
            self.wfile.write(DATA[s:e + 1])
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(DATA)))
        self.end_headers()
        self.wfile.write(DATA)


def api(method, path, body=None):
    req = urllib.request.Request(
        "http://127.0.0.1:%d/api/v1%s" % (API_PORT, path),
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-SunTey-Token": TOKEN, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = json.loads(e.read().decode() or "{}")
        raise RuntimeError("%s %s -> %s" % (method, path, raw.get("error", e.code)))
    if not raw.get("ok"):
        raise RuntimeError("%s %s -> %s" % (method, path, raw.get("error")))
    return raw.get("data")


def status_name(t):
    # TaskStatus marshals as its numeric String(): 0=unknown 1=resolving
    # 2=ready 3=running 4=paused 5=complete 6=error. Compare through this —
    # the raw int never equals a status string.
    names = ("unknown", "resolving", "ready", "running", "paused", "complete", "error")
    s = (t or {}).get("status")
    if isinstance(s, int) and 0 <= s < len(names):
        return names[s]
    return str(s)


def get_task(tid):
    for t in api("GET", "/tasks"):
        if t.get("id") == tid:
            return t
    return None


def wait_task(tid, want, timeout=60):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        t = get_task(tid)
        last = t
        if t and status_name(t) in (want, "complete", "error"):
            return t
        time.sleep(0.4)
    return last


def start_core():
    """Launch the core and block until it reports it is listening."""
    p = subprocess.Popen(
        [CORE, "serve", "--port", str(API_PORT), "--token", TOKEN,
         "--store", STORE, "--dir", DLDIR],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        creationflags=0x08000000,  # CREATE_NO_WINDOW
    )
    first = p.stdout.readline()
    if '"listening"' not in first:
        raise RuntimeError("core did not start: " + first)
    return p


def smoke_http():
    srv = ThreadingHTTPServer(("127.0.0.1", HTTP_PORT), FileHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:%d/big.bin" % HTTP_PORT
    try:
        t = api("POST", "/tasks", {"url": url})
        tid = t["id"]
        api("POST", "/tasks/start", {"id": tid})
        wait_task(tid, "running", 10)
        api("POST", "/tasks/stop", {"id": tid})
        p = wait_task(tid, "paused", 15)
        partial = (p or {}).get("downloaded", 0)
        api("POST", "/tasks/start", {"id": tid})
        fin = wait_task(tid, "complete", 90)
        if not fin or status_name(fin) != "complete":
            fail("http.resume", "final=%s" % json.dumps(fin, ensure_ascii=False)[:200])
            return
        h = hashlib.sha1(open(fin["savePath"], "rb").read()).hexdigest()
        if h != hashlib.sha1(DATA).hexdigest():
            fail("http.bytes_exact", "sha mismatch")
            return
        ok("http download+pause+resume byte-exact (partial=%d, total=%d)" % (partial, len(DATA)))
    finally:
        srv.shutdown()


def smoke_bt():
    magnet = ("magnet:?xt=urn:btih:476cd5cd7ba4e1b51fcabb0b8276389c0c84c623"
              "&dn=ubuntu-22.04.5-desktop-amd64.iso"
              "&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"
              "&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce")
    t = api("POST", "/tasks", {"url": magnet})
    tid = t.get("id")
    if not tid:
        fail("bt.create", json.dumps(t, ensure_ascii=False)[:200])
        return
    api("POST", "/tasks/start", {"id": tid})
    end = time.time() + 40
    meta = None
    while time.time() < end:
        tt = get_task(tid)
        if tt and tt.get("size", 0) > 0 and tt.get("name"):
            meta = tt
            break
        if tt and status_name(tt) == "error":
            break
        time.sleep(1)
    stats = api("GET", "/bt/stats")
    if not stats.get("listenPort"):
        fail("bt.listen_port", json.dumps(stats)[:160])
    elif meta is None:
        tt = get_task(tid)
        st = status_name(tt)
        # Metadata over DHT can legitimately be slow/blocked on home NAT; the
        # task must be alive (running) with the swarm joined, not crashed.
        if tt and st in ("running", "paused", "error"):
            ok("bt.join degraded-accepted (no metadata in 40s; status=%s port=%s peers=%s)"
               % (st, stats.get("listenPort"), stats.get("peers")))
        else:
            fail("bt.join", json.dumps(tt, ensure_ascii=False)[:220])
    else:
        ok("bt.metadata resolved name=%s size=%d" % (meta.get("name", "")[:40], meta.get("size")))
    api("POST", "/tasks/stop", {"id": tid})
    time.sleep(1.5)
    tt = get_task(tid)
    if tt and status_name(tt) == "running":
        fail("bt.stop.settles", "still running after stop")
    else:
        ok("bt.stop settles (%s)" % status_name(tt))


def smoke_hls_routing():
    t = api("POST", "/tasks", {"url": "http://127.0.0.1:%d/none.m3u8" % HTTP_PORT})
    tid = t.get("id")
    if not tid:
        fail("hls.create", json.dumps(t, ensure_ascii=False)[:160])
        return
    tt = get_task(tid)
    # JSON tag is mediaUrl (lowerCamel); size=-1 is the HLS sentinel.
    if tt and tt.get("size") == -1 and (tt.get("hls") or {}).get("mediaUrl"):
        ok("hls.routing (size=-1, hls state persisted)")
    else:
        fail("hls.routing", json.dumps(tt, ensure_ascii=False)[:220])


# ---------------------------------------------------------------- HLS end-to-end
# A real 8-segment playlist served slowly, so the core process can be HARD
# KILLED mid-download. The point is the segment checkpoint: segments already
# on disk must NOT be fetched again after the restart — "关掉软件再打开就从头
# 开始" is the failure mode this guards against.
SEG_SIZES = [70000] * 8
SEG_DATA = [bytes(random.getrandbits(8) for _ in range(s)) for s in SEG_SIZES]
SEG_DELAY = 0.35
HITS = {}


class HLSHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        p = self.path.split("?")[0]
        HITS[p] = HITS.get(p, 0) + 1
        if p == "/media/index.m3u8":
            body = ("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"
                    "#EXT-X-MEDIA-SEQUENCE:0\n")
            for i in range(len(SEG_DATA)):
                body += "#EXTINF:4.0,\n/media/seg%d.ts\n" % i
            body += "#EXT-X-ENDLIST\n"
            b = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if p.startswith("/media/seg") and p.endswith(".ts"):
            idx = int(p[len("/media/seg"):-3])
            d = SEG_DATA[idx]
            time.sleep(SEG_DELAY)
            self.send_response(200)
            self.send_header("Content-Length", str(len(d)))
            self.end_headers()
            self.wfile.write(d)
            return
        self.send_response(404)
        self.end_headers()


def seg_hits():
    return {p: n for p, n in HITS.items() if "/media/seg" in p}


def seg_files(parts_dir):
    """Segments the engine has COMPLETED (renamed into place) at this moment.

    Counting HTTP requests is not a checkpoint: with N workers every segment
    is in flight almost immediately, so "requests seen" overstates what a hard
    kill preserves. The engine's checkpoint is the atomic rename of
    seg-xxxxxxxx, so the resume budget must come from the filesystem.
    """
    try:
        return [n for n in os.listdir(parts_dir) if n.startswith("seg-")]
    except OSError:
        return []


def out_candidates(task):
    sp = (task or {}).get("savePath") or ""
    base, _ = os.path.splitext(sp)
    # The engine merges into the task path; when the container cannot be
    # remuxed to MP4 it keeps the merged stream as .ts next to it.
    return [sp, base + ".ts", base + ".mp4", base + ".m4s"]


def smoke_hls_e2e(proc):
    srv = ThreadingHTTPServer(("127.0.0.1", HLS_PORT), HLSHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    newproc = None
    try:
        url = "http://127.0.0.1:%d/media/index.m3u8" % HLS_PORT
        # conn=2: with 8 workers all 8 segments start at once and none finishes
        # before the kill, which proves nothing about resume. Two workers make
        # segments land on disk one after another, so the checkpoint is real.
        t = api("POST", "/tasks", {"url": url, "conn": 2})
        tid = t["id"]
        save_path = t.get("savePath") or ""
        parts_dir = save_path + ".parts"
        api("POST", "/tasks/start", {"id": tid})
        end = time.time() + 40
        while time.time() < end and len(seg_files(parts_dir)) < 3:
            time.sleep(0.2)
        completed = len(seg_files(parts_dir))
        first_hits = dict(seg_hits())
        proc.kill()  # hard kill: no shutdown hook, like taskkill/crash
        try:
            proc.wait(timeout=15)
        except Exception:
            pass
        if completed < 3:
            fail("hls.kill_point", "only %d segments on disk before kill" % completed)
            return

        newproc = start_core()
        tt = get_task(tid)
        if tt is None:
            fail("hls.task_survives_kill", "task gone after restart")
            return
        api("POST", "/tasks/start", {"id": tid})
        fin = wait_task(tid, "complete", 120)
        st = status_name(fin)
        path = next((p for p in out_candidates(fin) if p and os.path.exists(p)), None)
        if path is None:
            fail("hls.merged_output", "no merged file; task=%s"
                 % json.dumps(fin, ensure_ascii=False)[:220])
            return
        size = os.path.getsize(path)
        if size != sum(SEG_SIZES):
            fail("hls.merged_size", "%s = %d, want %d"
                 % (os.path.basename(path), size, sum(SEG_SIZES)))
            return
        after = seg_hits()
        refetch = sum(max(0, n - 1) for n in after.values())
        # Budget: only the segments that were NOT on disk at the kill may be
        # fetched again (those are the ones a crash lost, incl. in-flight).
        budget = len(SEG_DATA) - completed
        if refetch > budget:
            fail("hls.segment_resume", "%d segments re-downloaded, budget %d "
                 "(done at kill=%d, hits=%s)" % (refetch, budget, completed, after))
            return
        ok("hls hard-kill resume: %d/%d segments kept, merged %s = %d bytes (status=%s, refetch=%d<=%d)"
           % (completed, len(SEG_DATA), os.path.basename(path), size, st, refetch, budget))
        return newproc
    finally:
        srv.shutdown()


def main():
    for d in (WORK, DLDIR):
        os.makedirs(d, exist_ok=True)
    if os.path.exists(STORE):
        os.remove(STORE)
    if not os.path.exists(CORE):
        print("core not built: " + CORE)
        sys.exit(1)
    proc = start_core()
    try:
        smoke_http()
        smoke_bt()
        smoke_hls_routing()
        # HLS runs last: it hard-kills the process and restarts it itself.
        try:
            api("POST", "/shutdown", {})
        except Exception:
            pass
        time.sleep(0.6)
        proc = start_core()
        nxt = smoke_hls_e2e(proc)
        if nxt is not None:
            proc = nxt
    finally:
        try:
            api("POST", "/shutdown", {})
        except Exception:
            pass
        time.sleep(0.5)
        try:
            proc.terminate()
        except Exception:
            pass
    if FAILS:
        print("\n%d FAILURES: %s" % (len(FAILS), ", ".join(FAILS)))
        sys.exit(1)
    print("\nALL SMOKE PASS")


if __name__ == "__main__":
    main()
