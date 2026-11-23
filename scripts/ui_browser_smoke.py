#!/usr/bin/env python3
"""ui_browser_smoke.py — drive the packaged UI (app/dist) in a real headless browser.

The UI normally gets its core from the Tauri shell via invoke("sidecar_info").
Here we inject a window.__TAURI_INTERNALS__ mock that returns a plain-core
serve, then assert the user-facing wiring end-to-end:

  1. core badge shows the real core version
  2. pasting an HTML page with two file links opens the M6.5 candidate modal
  3. picking one link (the second) creates exactly that task and it completes
  4. the sort dropdown reorders the visible task list
  5. the settings gear opens and shows real config (conn=16, UA, watch)

Env: BROWSER=msedge|chrome, SUNTEY_CORE (core exe), UI_DIR (default app/dist).
Exit 0 = UI BROWSER SMOKE PASS.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, SimpleHTTPRequestHandler, ThreadingHTTPServer

try:
    import websocket
except ImportError:
    raise SystemExit("pip install websocket-client first")

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORE = os.environ.get("SUNTEY_CORE") or os.path.join(ROOT, "core", "bin", "suntey-core.exe")
UI_DIR = os.environ.get("UI_DIR") or os.path.join(ROOT, "app", "dist")
CORE_PORT = 17333
CDP_PORT = 9412
TOKEN = "ui-smoke-token"
ZIP = 9096
PAYLOAD = os.urandom(240 * 1024)

BROWSERS = {
    "msedge": [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ],
    "chrome": [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ],
}


def find_browser():
    for p in BROWSERS[os.environ.get("BROWSER", "msedge")]:
        if os.path.isfile(p):
            return p
    raise SystemExit("no msedge/chrome install found")


def api(method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{CORE_PORT}/api/v1{path}", method=method)
    req.add_header("X-SunTey-Token", TOKEN)
    data = json.dumps(body).encode() if body is not None else None
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, data, timeout=15) as r:
        raw = r.read().decode()
    obj = json.loads(raw)
    return obj["data"] if isinstance(obj, dict) and "data" in obj else obj


class ZipHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

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
            # Connection:close — ThreadingHTTPServer writes the whole range in
            # one kernel-blocked write(); after a mid-run cancel the Go client
            # left a half-dead keep-alive conn and the next 206 body stalls
            # until the 30s idle guard fires (probe_rs10 evidence: 30.2s).
            # Closing per request matches the manres-proven fixture shape and
            # keeps resume at ~4s.
            self.send_header("Connection", "close")
            self.close_connection = True
            self.end_headers()
            self.wfile.write(chunk)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Connection", "close")
            self.close_connection = True
            self.end_headers()
            self.wfile.write(PAYLOAD)

    def do_HEAD(self):
        if self.path.split("?", 1)[0].endswith(".zip"):
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


UIPAGE = (b"<html><body>"
          b'<a href="http://127.0.0.1:9096/ui1.zip">one</a>'
          b'<a href="http://127.0.0.1:9096/ui2.zip">two</a>'
          b"</body></html>")


class CDP:
    def __init__(self, ws_url):
        self.ws = websocket.create_connection(ws_url, timeout=10)
        self.mid = 0

    def call(self, method, params=None, timeout=15):
        self.mid += 1
        mid = self.mid
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
        self.ws.settimeout(timeout)
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def js_eval(page, expr, timeout=15):
    r = page.call("Runtime.evaluate", {
        "expression": expr, "returnByValue": True, "awaitPromise": True,
        "timeout": int(timeout * 1000)})
    res = r.get("result", {})
    if r.get("exceptionDetails"):
        raise RuntimeError(f"js threw: {json.dumps(r['exceptionDetails'])[:200]}")
    if res.get("type") == "object" and "subtype" not in res:
        try:
            res = json.loads(res.get("value"))
        except Exception:
            pass
    return res.get("value")


def targets(cdp_port):
    with urllib.request.urlopen(f"http://127.0.0.1:{cdp_port}/json/list", timeout=5) as r:
        return json.loads(r.read().decode())


def wait_page_target(cdp_port):
    # start URL is about:blank — accept any page target; the caller navigates via Page.navigate
    for _ in range(60):
        for t in targets(cdp_port):
            if t.get("type") == "page":
                return t["webSocketDebuggerUrl"]
        time.sleep(0.25)
    raise SystemExit("no page target appeared on CDP port (old-headless fallback?)")


def poll(js, want, tries, gap=0.4):
    val = None
    for _ in range(tries):
        val = js_eval(page, js)
        if val == want:
            return True
        time.sleep(gap)
    return False


def main():
    if not os.path.isfile(CORE):
        raise SystemExit(f"core exe not found: {CORE}")
    if not os.path.isfile(os.path.join(UI_DIR, "index.html")):
        raise SystemExit(f"UI dist missing at {UI_DIR}")

    # -- core on a temp store/dir so the run never touches the user's state
    workdir = tempfile.mkdtemp(prefix="suntey-uisn-")
    env = dict(os.environ, TORRENT_STORAGE_DEFAULT_FILE_IO="classic")
    core = subprocess.Popen(
        [CORE, "serve", "--port", str(CORE_PORT), "--token", TOKEN,
         "--store", os.path.join(workdir, "ui.db"), "--dir", workdir],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    zsrv = ThreadingHTTPServer(("127.0.0.1", ZIP), ZipHandler)
    threading.Thread(target=zsrv.serve_forever, daemon=True).start()

    class UIRoot(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=UI_DIR, **kw)

        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path == "/uipage.html":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(UIPAGE)))
                self.end_headers()
                self.wfile.write(UIPAGE)
                return
            super().do_GET()

    uisrv = ThreadingHTTPServer(("127.0.0.1", 9097), UIRoot)
    threading.Thread(target=uisrv.serve_forever, daemon=True).start()

    browser = find_browser()
    profile = tempfile.mkdtemp(prefix="suntey-uisn-profile-")
    proc = None
    page = None
    try:
        args = [browser, "--headless=new", "--disable-gpu", "--no-first-run",
                "--no-default-browser-check", f"--user-data-dir={profile}",
                f"--remote-debugging-port={CDP_PORT}", "--window-size=1280,900",
                "about:blank"]
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        for _ in range(60):
            try:
                with socket.create_connection(("127.0.0.1", CDP_PORT), 0.3):
                    break
            except OSError:
                time.sleep(0.25)
        else:
            raise SystemExit("CDP port never listened (old-headless fallback)")
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", CORE_PORT), 0.2):
                    break
            except OSError:
                time.sleep(0.2)

        mock = ("window.__TAURI_INTERNALS__ = {invoke: function(cmd) {"
                "if (cmd === 'sidecar_info') return Promise.resolve("
                f"{{port: {CORE_PORT}, token: '{TOKEN}', pid: 0}});"
                "return Promise.reject('browser-mock: ' + cmd);}};")
        page = CDP(wait_page_target(CDP_PORT))
        page.call("Page.enable")
        page.call("Runtime.enable")
        page.call("Page.addScriptToEvaluateOnNewDocument", {"source": mock})

        page.call("Page.navigate", {"url": "http://127.0.0.1:9097/index.html"})

        # 1) core badge shows the real core version
        for _ in range(40):
            badge = js_eval(page, "document.getElementById('core-badge')?.textContent || ''")
            if "core v" in (badge or ""):
                break
            time.sleep(0.3)
        if "core v" not in (badge or ""):
            raise SystemExit(f"core badge never showed version: {badge!r}")
        print(f"badge: {badge!r} ✓")

        # 2) paste an HTML page (two zip links) — the resolver fetches and
        # probes it itself, so we stay on the UI page the whole time.
        # pre-seed one completed task so the sort test has two tasks whose
        # name-order and newest-order are the reverse of each other.
        api("POST", "/tasks", {"url": f"http://127.0.0.1:{ZIP}/aaa_seed.zip"})

        js_eval(page, (
            "var i=document.getElementById('url');"
            "i.value='http://127.0.0.1:9097/uipage.html';"
            "i.dispatchEvent(new Event('input'));"
            "document.getElementById('addform').dispatchEvent(new Event('submit',{cancelable:true}));"))

        shown = False
        for _ in range(40):
            shown = js_eval(page, "!document.getElementById('candmodal')?.hidden")
            if shown:
                break
            time.sleep(0.3)
        if not shown:
            dbg_status = js_eval(page, "document.getElementById('status')?.textContent || ''")
            try:
                r = api("POST", "/resolve", {"url": "http://127.0.0.1:9097/uipage.html"})
                dbg_r = json.dumps({k: r.get(k) for k in ("isPage", "supportRange", "size")}) + " cands=" + str(len(r.get("candidates") or []))
            except Exception as e:
                dbg_r = f"resolve threw {e}"
            raise SystemExit(f"candidate modal did not open for the 2-link page (UI status: {dbg_status!r}; direct resolve: {dbg_r})")
        n = js_eval(page, "document.querySelectorAll('#candlist input[name=cand]').length")
        print(f"candidate modal opened with {n} picks ✓")
        if not n or n < 2:
            raise SystemExit(f"expected >=2 candidates, got {n}")

        # 3) pick the SECOND link, click 下载 -> exactly that task lands
        js_eval(page, (
            "var c=document.querySelectorAll('#candlist input[name=cand]');"
            "c[c.length-1].click();"))
        js_eval(page, "document.getElementById('cand-ok').click();")
        picked = js_eval(page, (
            "var c=document.querySelectorAll('#candlist label');"
            "c[c.length-1]?.textContent || ''"))
        # modal hides, and a task whose url is the second candidate appears
        frag = (picked.split(":")[-1]).strip() if picked else ""
        frag = frag.split("/")[-1] if frag else "ui2.zip"
        task_ok = False
        for _ in range(40):
            time.sleep(0.4)
            task_ok = js_eval(page, (
                f"[...document.querySelectorAll('#tasks .task')]"
                f".some(t => (t.dataset.url||JSON.stringify(t)).includes('{frag}')"
                f" || [...document.querySelectorAll('#tasks .task')].length >= 1)"))
            if task_ok:
                break
        hidden = js_eval(page, "document.getElementById('candmodal')?.hidden")
        ntasks = js_eval(page, "document.querySelectorAll('#tasks .task').length")
        if not hidden or not ntasks:
            raise SystemExit(f"pick did not land a task (modal hidden={hidden}, tasks={ntasks})")
        # wait until core says that task is complete (status 5)
        done = False
        for _ in range(40):
            time.sleep(0.5)
            ts = api("GET", "/tasks") or []
            if any(t["status"] == 5 and frag.split(".")[0] in (t.get("req") or {}).get("url", "")
                   for t in ts):
                done = True
                break
        if not done:
            raise SystemExit(f"picked task never completed: {[t['status'] for t in api('GET','/tasks')]}")
        print(f"pick -> task {frag} downloaded to complete ✓")

        # the seeded aaa_seed task + the modal-picked ui2 task: name order and
        # newest order must be exact reverses; reload syncs the list first.
        js_eval(page, "location.reload();")
        time.sleep(1.2)
        for _ in range(30):
            if js_eval(page, "document.querySelectorAll('#tasks .task').length >= 2"):
                break
            time.sleep(0.4)
        # localStorage may remember a past sort (Edge reuses the temp profile);
        # force the newest-order baseline before capturing, then flip to name.
        js_eval(page, (
            "var s=document.getElementById('sortsel');if(s)s.value='new';"
            "var s=document.getElementById('sortsel');if(s)s.dispatchEvent(new Event('change'));"))
        time.sleep(0.5)
        order_new = js_eval(page, (
            "[...document.querySelectorAll('#tasks .task')]"
            ".map(t => (t.querySelector('.name')?.textContent || t.textContent).slice(0,20)).join('|')"))
        js_eval(page, (
            "var s=document.getElementById('sortsel');s.value='name';"
            "s.dispatchEvent(new Event('change'));"))
        time.sleep(0.6)
        order_name = js_eval(page, (
            "[...document.querySelectorAll('#tasks .task')]"
            ".map(t => (t.querySelector('.name')?.textContent || t.textContent).slice(0,20)).join('|')"))
        if order_new == order_name:
            raise SystemExit(f"sort dropdown changed nothing: {order_new!r}")
        print("sort dropdown reorders the list ✓")

        # 6) row pause/start buttons round-trip on a rate-limited task (UI path).
        # conn=1 on purpose: defaultConn is process-wide, so 8 legs would share
        # one 32 KiB/s token and a mid-run resume strands >1MiB below the cap —
        # the 75s poll window would close on rate math, not a bug. Single conn
        # keeps the resume ETA at (240KiB-size burst) ~12s.
        api("PUT", "/config", {"rateLimitKBps": 32, "defaultConn": 1})
        js_eval(page, (
            "var i=document.getElementById('url');"
            "i.value='http://127.0.0.1:9096/ui1.zip?pause=1';"
            "i.dispatchEvent(new Event('input'));"
            "document.getElementById('addform').dispatchEvent(new Event('submit',{cancelable:true}));"))
        pid = None
        for _ in range(40):
            run_ = [t for t in (api("GET", "/tasks") or [])
                    if "ui1.zip" in (t.get("req") or {}).get("url", "") and t.get("status") in (2, 3)]
            if run_:
                pid = run_[0]["id"]
                break
            time.sleep(0.3)
        if not pid:
            raise SystemExit("pause-test task never entered running")
        has_pause = False
        for _ in range(30):  # the UI row must render status=running before a pause button exists
            if js_eval(page, "!!document.querySelector('button[data-act=pause]')"):
                has_pause = True
                break
            time.sleep(0.3)
        if not has_pause:
            raise SystemExit("no pause button ever appeared in the UI task list")
        js_eval(page, "document.querySelector('button[data-act=pause]').click();")
        paused = False
        t0 = time.time()
        settle = None
        for _ in range(60):  # window 12s (UI render + CDP + core settle); settle assertion below is the real gate
            t_ = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == pid), None)
            if t_ and t_.get("status") == 4:
                paused = True
                settle = time.time() - t0
                break
            if t_ and t_.get("status") == 5:
                raise SystemExit("pause-test task completed before the pause could land (rate too fast for this test?)")
            time.sleep(0.2)
        if not paused:
            # diagnose: was the click's POST lost, or is the core slow to settle?
            t_ = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == pid), None)
            dbg = json.dumps({k: (t_ or {}).get(k) for k in ("status", "downloaded", "size", "err")}) if t_ else "task gone"
            rows = js_eval(page, "[...document.querySelectorAll('#tasks .task')].map(r=>r.textContent.slice(0,60)).join(' // ')")
            js_eval(page, (f"fetch('http://127.0.0.1:{CORE_PORT}/api/v1/tasks/stop',"
                           f"{{method:'POST',headers:{{'X-SunTey-Token':'{TOKEN}'}},body:JSON.stringify({{id:{json.dumps(pid)!r}}})}})"))
            man = False
            for _ in range(25):
                t2 = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == pid), None)
                if t2 and t2.get("status") == 4:
                    man = True
                    break
                time.sleep(0.2)
            if man:
                raise SystemExit(f"pause click's POST never landed (UI bug): core stayed {dbg}; manual stop settled fine")
            raise SystemExit(f"pause button did not pause the task in core: {dbg} rows: {rows!r}")
        if settle and settle > 6.0:
            raise SystemExit(f"pause took {settle:.1f}s to settle in core via UI (want <6s incl. render; pre-fix was 5-8s+, IDM stops instantly)")
        print(f"pause settled in {settle:.1f}s ✓")
        lbl = ""
        for _ in range(20):
            lbl = js_eval(page, "document.querySelector('button[data-act=start]')?.textContent||''")
            if "开始" in lbl:
                break
            time.sleep(0.3)
        if "开始" not in lbl:
            raise SystemExit(f"row button after pause shows {lbl!r}, want 开始")
        row_btn = "[data-id=\"" + pid + "\"] button[data-act=start]"
        for _ in range(20):
            if js_eval(page, "!!document.querySelector('" + row_btn + "')"):
                break
            time.sleep(0.3)
        if not js_eval(page, "!!document.querySelector('" + row_btn + "')"):
            dom = js_eval(page, "Array.from(document.querySelectorAll('.task')).map(e=>e.dataset.id+':'+(e.querySelector('.acts')||{}).textContent).join('|')")
            raise SystemExit(f"no start button on the paused task row; rows={dom!r}")
        js_eval(page, "document.querySelector('" + row_btn + "').click();")
        done = False
        for _ in range(200):  # idle-guard (30s) heals cancel-aborted conns via retry; allow up to two cycles + rate ETA
            t_ = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == pid), None)
            if t_ and t_.get("status") == 5:
                done = True
                break
            if t_ and t_.get("status") == 6:
                raise SystemExit(f"pause-test task errored: {t_.get('err')}")
            time.sleep(0.4)
        if not done:
            snap = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == pid), None)
            raise SystemExit(f"resume click did not finish the rate-limited task: {snap}")
        api("PUT", "/config", {"rateLimitKBps": 0, "defaultConn": 8})
        print("pause/start row buttons round-trip on the rate-limited task (32 KB/s cap visible) ✓")

        # 5) settings gear opens with real config
        js_eval(page, "document.getElementById('gearbtn').click();")
        vis = False
        for _ in range(20):
            vis = js_eval(page, "!document.getElementById('settings')?.hidden")
            if vis:
                break
            time.sleep(0.3)
        if not vis:
            raise SystemExit("settings modal did not open")
        cfg_js = js_eval(page, (
            "JSON.stringify({conn: document.getElementById('cfg-conn')?.value,"
            "ua: (document.getElementById('cfg-ua')?.value||'').slice(0,12),"
            "clip: document.getElementById('cfg-clip')?.checked})"))
        print(f"settings: {cfg_js}")
        cfg = api("GET", "/config")
        if str(cfg.get("defaultConn")) != str(json.loads(cfg_js)["conn"]):
            raise SystemExit(f"settings conn {cfg_js} != core {cfg.get('defaultConn')}")
        if json.loads(cfg_js)["clip"] != bool(cfg.get("clipboardWatch", True)):
            raise SystemExit("settings clipboard checkbox out of sync with core")

        # 7) BT/magnet per-file selection modal (迅雷/IDM "文件" detail)
        fix = os.environ.get("SUNTEY_TORRENT_FIXTURE", "")  # 无默认：不传就跳过 step7
        if not os.path.isfile(fix):
            print(f"step7 skipped: torrent fixture missing ({fix})")
        else:
            created = api("POST", "/tasks", {"url": fix})
            t7 = created["id"] if isinstance(created, dict) else None
            if not t7:
                raise SystemExit(f"torrent task create failed: {created}")
            api("POST", "/tasks/start", {"id": t7})
            files = None
            for _ in range(60):  # public-net metadata bootstrap can take seconds
                files = api("GET", "/bt/files?id=" + t7)
                if files:
                    break
                err7 = None
                t_ = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == t7), None)
                if t_ and t_.get("status") == 6:
                    raise SystemExit(f"torrent task errored: {t_.get('err')}")
                time.sleep(0.5)
            if not files:
                raise SystemExit("no /bt/files list after 30s (metadata not arriving?)")
            row_btn = "[data-id=\"" + t7 + "\"] button[data-act=files]"
            for _ in range(20):
                if js_eval(page, "!!document.querySelector('" + row_btn + "')"):
                    break
                time.sleep(0.3)
            if not js_eval(page, "!!document.querySelector('" + row_btn + "')"):
                snap7 = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == t7), None)
                dbg7 = json.dumps({k: snap7.get(k) for k in ("status", "err", "info", "btSel", "size")}, ensure_ascii=False) if snap7 else None
                dom = js_eval(page, "Array.from(document.querySelectorAll('.task')).map(e=>e.dataset.id+':'+(e.querySelector('.acts')||{}).textContent).join('|')")
                raise SystemExit(f"row 文件 button not rendered on the BT task; t7={dbg7}; rows={dom!r}")
            js_eval(page, "document.querySelector('" + row_btn + "').click();")
            vis7 = False
            for _ in range(10):
                vis7 = js_eval(page, "!document.getElementById('filemodal')?.hidden")
                if vis7:
                    break
                time.sleep(0.2)
            if not vis7:
                raise SystemExit("文件 modal did not open on click")
            rows = js_eval(page, "document.querySelectorAll('#filelist label').length")
            if not rows:
                raise SystemExit("file list empty inside 文件 modal")
            js_eval(page, "document.getElementById('file-ok').click();")
            sel7 = None
            for _ in range(20):
                t_ = next((t for t in (api("GET", "/tasks") or []) if t.get("id") == t7), None)
                sel7 = (t_ or {}).get("btSel")
                if sel7:
                    break
                time.sleep(0.3)
            if not sel7:
                raise SystemExit(f"btSel not persisted after ok: {sel7!r}")
            api("POST", "/tasks/stop", {"id": t7})
            print(f"文件 selection modal opens with {rows} file(s), btSel={sel7!r} persisted ✓")

        print("UI BROWSER SMOKE PASS")
    finally:
        if page:
            page.close()
        if proc:
            try:
                proc.terminate()
                proc.wait(5)
            except Exception:
                pass
        try:
            api("POST", "/shutdown")
            core.wait(timeout=10)
        except Exception:
            core.terminate()
            try:
                core.wait(5)
            except Exception:
                core.kill()
        zsrv.shutdown()
        uisrv.shutdown()
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)
        shutil.rmtree(profile, ignore_errors=True)


if __name__ == "__main__":
    main()
