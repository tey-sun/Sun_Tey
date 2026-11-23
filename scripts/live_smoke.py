#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Live-network smoke: real CDN pause/resume + public magnet metadata flow.

Solidifies the round71/72 probes into the repo alongside pubnet_probe.py.
Spawns its own core (never touches dist/db of a running GUI instance), so it
can run while Sun_Tey.exe is up. Requires outbound internet.

Usage:  python scripts/live_smoke.py [path/to/suntey-core.exe]
Exit 0 = LIVE SMOKE PASS.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

CORE = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(__file__), "..", "core", "bin", "suntey-core.exe")
TGZ_URL = "https://registry.npmjs.org/@esbuild/win32-x64/-/win32-x64-0.21.5.tgz"
MAGNET = ("magnet:?xt=urn:btih:01c137287d6f0ed05a56742dae794f632c79ff3d"
          "&dn=ubuntu-24.04.4-desktop-amd64.iso")
READY, RUNNING, PAUSED, DONE, ERROR = 2, 3, 4, 5, 6


class Rest:
    def __init__(self, port, token):
        self.base = "http://127.0.0.1:%d/api/v1" % port
        self.h = {"X-SunTey-Token": token, "Content-Type": "application/json"}

    def call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers=self.h,
                                     method=method)
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())


def get_task(rest, tid):
    for t in rest.call("GET", "/tasks")["data"]:
        if t.get("id") == tid:
            return t
    raise AssertionError("task %s missing" % tid)


def create(rest, url):
    out = rest.call("POST", "/tasks", {"url": url})
    t = out.get("data") or out
    tid = t.get("id")
    assert tid, "no task id in %r" % out
    return tid


def serve():
    tmp = tempfile.mkdtemp(prefix="suntey_live_")
    env = dict(os.environ, TORRENT_STORAGE_DEFAULT_FILE_IO="classic")
    p = subprocess.Popen(
        [CORE, "serve", "--store", os.path.join(tmp, "live.db"),
         "--dir", tmp, "--port", "0", "--token", "livesmoke"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
    line = p.stdout.readline()
    info = json.loads(line)
    assert info.get("type") == "listening", "bad handshake %r" % line
    return p, tmp, Rest(info["port"], "livesmoke")


def test_pause_resume(rest, tmp):
    tid = create(rest, TGZ_URL)
    rest.call("POST", "/tasks/start", {"id": tid})
    # pause mid-flight once ~500KB landed
    t0 = time.time()
    while time.time() - t0 < 60:
        t = get_task(rest, tid)
        if t["status"] == ERROR:
            raise AssertionError("download error: %r" % t)
        if t.get("downloaded", 0) > 500_000:
            break
        time.sleep(0.3)
    else:
        raise AssertionError("no progress before 500KB")
    rest.call("POST", "/tasks/stop", {"id": tid})
    t0 = time.time()
    while time.time() - t0 < 5:
        if get_task(rest, tid)["status"] == PAUSED:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("not paused within 5s of stop")
    frozen = get_task(rest, tid).get("downloaded", 0)
    time.sleep(2)
    delta = get_task(rest, tid).get("downloaded", 0) - frozen
    assert delta < 64_000, "still growing after pause: +%d" % delta
    rest.call("POST", "/tasks/start", {"id": tid})
    t0 = time.time()
    while time.time() - t0 < 120:
        t = get_task(rest, tid)
        if t["status"] == DONE:
            break
        if t["status"] == ERROR:
            raise AssertionError("resume error: %r" % t)
        time.sleep(0.5)
    else:
        raise AssertionError("resume did not finish in 120s")
    size = t.get("size")
    assert t.get("downloaded") == size, "downloaded %s != size %s" % (
        t.get("downloaded"), size)
    for fn in os.listdir(tmp):
        if fn.endswith(".tgz"):
            disk = os.path.getsize(os.path.join(tmp, fn))
            assert disk == size, "disk %d != size %d" % (disk, size)
            break
    else:
        raise AssertionError("no .tgz on disk in download dir")
    print("PAUSE/RESUME PASS (mid-flight pause, freeze check, resume exact)")


def test_magnet(rest):
    tid = create(rest, MAGNET)
    rest.call("POST", "/tasks/start", {"id": tid})
    t0 = time.time()
    while time.time() - t0 < 180:
        t = get_task(rest, tid)
        if t["status"] == ERROR:
            raise AssertionError("magnet error: %r" % t)
        if t.get("downloaded", 0) > 2_000_000 and "ubuntu" in (t.get("name") or ""):
            print("MAGNET PASS: name=%s dl=%.1fMB in %.0fs" % (
                t.get("name"), t["downloaded"] / 1e6, time.time() - t0))
            rest.call("POST", "/tasks/stop", {"id": tid})
            return
        time.sleep(1)
    raise AssertionError("magnet metadata/data flow incomplete: %r" % t)


def main():
    p, tmp, rest = serve()
    try:
        test_pause_resume(rest, tmp)
        test_magnet(rest)
        print("LIVE SMOKE PASS")
        rc = 0
    finally:
        try:
            rest.call("POST", "/shutdown")
            p.wait(timeout=10)
        except Exception:
            p.terminate()
        shutil.rmtree(tmp, ignore_errors=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
