#!/usr/bin/env python3
"""Shell smoke: launch the packaged Sun_Tey.exe (Tauri shell -> core sidecar)
and drive the full product path over REST, the same way the UI does.

Unlike smoke_all.py (which runs the core directly), this exercises:
  * sidecar spawn + classic fileIo env injection (production-only path)
  * the db-generated api token (no --token arg in shell mode)
  * magnet/BT lifecycle under the shell env (the bttx mmap failure used to
    zero-out BT downloads; bytes>0 under the shell proves the env works)

Public-network magnet: we only assert the task is accepted, starts, and the
BT subsystem engages (metadata resolved / downloaded grows / stats reachable).
A dead public swarm (error after timeout) is also recorded, not fatal.
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(ROOT, "dist-portable")
EXE = os.environ.get("SUNTEY_SHELL") or os.path.join(DIST, "Sun_Tey.exe")
DB = os.path.join(DIST, "suntey.db")
BASE = "http://127.0.0.1:17332/api/v1"

MAGNET = ("magnet:?xt=urn:btih:dd8255ec0d5bc39e6f7261c1c51b74b340a2a72c"
          "&dn=Ubuntu-18.04.6-desktop-amd64.iso"
          "&tr=udp://tracker.opentrackr.org:1337/announce")

TOKEN_RE = rb'"apiToken":"([0-9a-fA-F]{16,64})"'


def read_token():
    # bbolt file: the config value JSON is stored verbatim; regex the bytes.
    for _ in range(20):
        with open(DB, "rb") as f:
            m = re.search(TOKEN_RE, f.read())
        if m:
            return m.group(1).decode()
        time.sleep(0.5)
    raise SystemExit("api token not found in suntey.db (core not started?)")


def api(method, path, body=None):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json",
                 "X-SunTey-Token": TOKEN},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode())
        return d["data"] if isinstance(d, dict) and "data" in d else d
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:120]}")


def task(tid=None):
    for t in api("GET", "/tasks") or []:
        if tid is None or t["id"] == tid:
            return t
    return None


TOKEN = None


def main():
    global TOKEN
    if not os.path.exists(EXE):
        raise SystemExit(f"missing {EXE}")
    procs = subprocess.run(["tasklist", "/FI", "IMAGENAME eq Sun_Tey.exe"],
                           capture_output=True, text=True)
    if "Sun_Tey.exe" in procs.stdout:
        raise SystemExit("Sun_Tey.exe already running (close it first)")

    shell = subprocess.Popen([EXE], cwd=DIST)
    try:
        TOKEN = read_token()
        print("token found in db; shell sidecar is up")

        ver = api("GET", "/version")
        print(f"core v{ver['version']} build={ver['build']} features={len(ver.get('features', []))}")

        # 1) magnet -> BT path under the production env (classic fileIo)
        t = api("POST", "/tasks", {"url": MAGNET})
        tid = t["id"]
        api("POST", "/tasks/start", {"id": tid})
        deadline = time.time() + 25
        seen = {}
        while time.time() < deadline:
            cur = task(tid)
            assert cur, f"task {tid} vanished"
            seen = cur
            st = cur.get("status")
            bt = api("GET", "/bt/stats")
            if (cur.get("downloaded", 0) > 0 or cur.get("name")
                    or bt.get("torrents", 0) > 0):
                break
            if st in (5, 6):  # complete / error (dead swarm)
                break
            time.sleep(2)
        bt = api("GET", "/bt/stats")
        print(f"magnet: status={seen.get('status')} name={seen.get('name')!r} "
              f"downloaded={seen.get('downloaded')} bt={bt}")
        assert bt.get("torrents", 0) > 0 or seen.get("downloaded", 0) > 0 \
            or seen.get("name"), "BT subsystem never engaged under the shell"
        api("POST", "/tasks/stop", {"id": tid})
        api("DELETE", "/tasks", {"id": tid})
        assert task(tid) is None, "delete failed"
        print("magnet task stopped and deleted cleanly")

        # 2) version route + config: GET through the shell auth gate
        cfg = api("GET", "/config")
        assert cfg.get("downloadDir"), "config missing downloadDir"
        print(f"config ok: dir={cfg['downloadDir']} conn={cfg['defaultConn']}")
        print("ALL SHELL SMOKE PASS")
    finally:
        for name in ("Sun_Tey.exe", "suntey-core.exe"):
            subprocess.run(["taskkill", "/F", "/IM", name],
                           capture_output=True)
        shell.wait(timeout=10)


if __name__ == "__main__":
    main()
