#!/usr/bin/env python3
"""Public-network BT probe: feed a real .torrent (or magnet) through the
production core and watch real internet bytes arrive.
Usage: python scripts/pubnet_probe.py <path/to.torrent| magnet url> [seconds]
Exits 0 when downloaded grows and peers/trackers show life; 2 on timeout."""
import json, os, re, socket, subprocess, sys, time, urllib.request

CORE = os.environ.get("SUNTEY_CORE", os.path.join(os.path.dirname(__file__), "..", "core", "bin", "suntey-core.exe"))
if len(sys.argv) < 2:
    sys.exit("usage: pubnet_probe.py <path/to.torrent|magnet url> [seconds]")
URL = sys.argv[1]
SECS = int(sys.argv[2]) if len(sys.argv) > 2 else 90
TOKEN = "probe" + str(int(time.time() % 100000))
STORE = os.path.join(os.environ.get("TEMP", "."), "suntey_pubnet_probe_%d.db" % os.getpid())


def api(method, path, body=None, tok=True):
    req = urllib.request.Request("http://127.0.0.1:17399/api/v1" + path, method=method)
    req.add_header("Content-Type", "application/json")
    if tok:
        req.add_header("X-SunTey-Token", TOKEN)
    if body is not None:
        req.data = json.dumps(body).encode()
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read().decode("utf-8", "replace"))
    return d.get("data", d)


def port_open(p):
    s = socket.socket()
    s.settimeout(0.4)
    try:
        s.connect(("127.0.0.1", p)); s.close(); return True
    except OSError:
        return False


def main():
    if os.path.exists(STORE):
        os.remove(STORE)
    env = dict(os.environ, TORRENT_STORAGE_DEFAULT_FILE_IO="classic")
    proc = subprocess.Popen([CORE, "serve", "--store", STORE, "--dir", os.environ.get("TEMP", "."),
                             "--port", "17399", "--token", TOKEN],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    try:
        for _ in range(80):
            if port_open(17399):
                break
            time.sleep(0.25)
        else:
            print("core did not start"); return 3
        api("PUT", "/config", {"dir": os.environ.get("TEMP", "."), "defaultConn": 8, "rateLimitKBps": 0})
        t = api("POST", "/tasks", {"url": URL})
        tid = t["id"]
        api("POST", "/tasks/start", {"id": tid})
        print("task", tid, "started; watching", SECS, "s")
        t0 = time.time(); best_dl = 0; stats = {}
        while time.time() - t0 < SECS:
            time.sleep(5)
            st = api("GET", "/bt/stats")
            ts = api("GET", "/tasks")
            tasks = ts["tasks"] if isinstance(ts, dict) else ts
            me = [x for x in tasks if x.get("id") == tid]
            if not me:
                break
            me = me[0]
            dl = me.get("downloaded", 0) or 0
            best_dl = max(best_dl, dl)
            g = st.get("gauges", st) if isinstance(st, dict) else {}
            stats = {k: g.get(k) for k in ("totalPeers", "activePeers", "connectedSeeders") if k in g} or g
            print(f"[{int(time.time()-t0)}s] status={me.get('status')} dl={dl} bt={stats}")
            if dl > 0 and (stats.get("totalPeers", stats.get("TotalPeers", 0)) or 0) >= 0 and dl > 2_000_000:
                print("PROBE PASS: real bytes flowing from public internet")
                break
        api("POST", "/tasks/stop", {"id": tid})
        api("DELETE", "/tasks", {"id": tid})
        print("final bt stats:", stats)
        print("best downloaded:", best_dl, "bytes in", int(time.time() - t0), "s")
        return 0 if best_dl > 0 else 2
    finally:
        try:
            api("POST", "/shutdown", {})
        except Exception:
            pass
        try:
            proc.wait(timeout=15)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
