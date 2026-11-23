import { invoke } from "@tauri-apps/api/core";
import { open as openDialog } from "@tauri-apps/plugin-dialog";

// TaskStatus mirrors Go: 0 Unknown 1 Resolving 2 Ready 3 Running 4 Paused 5 Complete 6 Error
const STATUS_NAMES: Record<number, string> = {
  0: "未知",
  1: "解析中",
  2: "待开始",
  3: "下载中",
  4: "已暂停",
  5: "已完成",
  6: "出错",
};

interface Task {
  id: string;
  name: string;
  size: number;
  downloaded: number;
  status: number;
  savePath: string;
  supportRange: boolean;
  conn: number;
  speed: number;
  req: { url: string };
  err: string;
  info?: string;
  files?: { name: string; size: number }[];
  btSel?: string; // BT kept-file indexes ("0,2"); empty = everything
  hls?: { segUrls?: unknown[]; done?: boolean[] };
  createdAt?: string; // core RFC3339 — lexicographic order == chronological in one zone
}

// Live BT swarm counters, kept client-side: the core streams peers/seeds/upload
// on progress events, and the task row (persisted shape) has no such fields.
interface BTLive {
  peers: number;
  seeds: number;
  uploaded: number;
}
const btLive = new Map<string, BTLive>();

// Level of the last live message, for colouring: transient retries are grey,
// repeated ones yellow, give-ups red. Painting every retry red is what made a
// healthy task look like it was stuck in a failure loop.
type MsgLevel = "info" | "warn" | "err";

interface CoreInfo {
  port: number;
  token: string;
}

let core: CoreInfo | null = null;
let es: EventSource | null = null;
const tasks = new Map<string, Task>();
let filter: "all" | "run" | "done" = "all";

// List ordering (persisted): newest first is the default (迅雷/IDM style);
// "fast" surfaces the active tasks that actually move right now.
type SortMode = "new" | "fast" | "size" | "name";
let sortMode = (localStorage.getItem("suntey.sort") || "new") as SortMode;
function bySort(a: Task, b: Task): number {
  switch (sortMode) {
    case "fast":
      return (b.speed || 0) - (a.speed || 0) || (b.downloaded || 0) - (a.downloaded || 0);
    case "size":
      return (b.size || 0) - (a.size || 0);
    case "name":
      return (a.name || a.req?.url || "").localeCompare(b.name || b.req?.url || "", "zh");
    default: {
      const ca = a.createdAt || "", cb = b.createdAt || "";
      return cb.localeCompare(ca) || (a.id < b.id ? -1 : 1);
    }
  }
}

const $ = <T extends Element>(sel: string) => document.querySelector(sel) as T | null;

function fmtBytes(n: number): string {
  if (n < 0) return "未知大小";
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + " MB";
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(1) + " KB";
  return n + " B";
}

function fmtSpeed(n: number): string {
  if (n <= 0) return "-";
  return fmtBytes(n) + "/s";
}

function setStatus(s: string, bad = false) {
  const el = $<HTMLSpanElement>("#status");
  if (el) {
    el.textContent = s;
    el.style.color = bad ? "var(--err)" : "var(--dim)";
  }
}

async function coreReady(): Promise<CoreInfo> {
  let last = "sidecar starting";
  for (let i = 0; i < 200; i++) {
    try {
      return await invoke<CoreInfo>("sidecar_info");
    } catch (e) {
      last = String(e);
      if (last.includes("内核启动失败")) throw new Error(last); // fatal, show now
      setStatus(last);
      await new Promise((r) => setTimeout(r, 150));
    }
  }
  throw new Error(last);
}

async function api<T>(method: string, path: string, body?: unknown): Promise<T> {
  if (!core) {
    core = await coreReady(); // lazy retry
  }
  const res = await fetch(`http://127.0.0.1:${core.port}/api/v1${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-SunTey-Token": core.token,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const j = (await res.json()) as { ok: boolean; data?: T; error?: string };
  if (!j.ok) throw new Error(j.error || "api error");
  return j.data as T;
}

function pct(t: Task): number {
  if (t.status === 5) return 100;
  if (t.hls?.segUrls?.length) {
    const total = t.hls.segUrls.length;
    const done = (t.hls.done || []).slice(0, total).filter(Boolean).length;
    return Math.min(100, Math.round((done / total) * 100));
  }
  if (t.size <= 0) return t.downloaded > 0 ? 50 : 0;
  return Math.min(100, Math.round((t.downloaded / t.size) * 100));
}

function actionsFor(status: number): string {
  if (status === 3) return "pause|del";
  if (status === 4 || status === 2) return "start|del";
  if (status === 5) return "open|del";
  if (status === 6) return "start|del"; // 出错也可重试：断点进度仍在，点开始即续传
  return "del";
}

// BT file checklist (Thunder/Xunlei-style): the button only appears once the
// torrent metadata has produced a file list and the engine can still act on
// it (ready/running/paused). Completed and errored tasks have no live torrent
// handle, so their "文件" button would only ever 404.
function btFilesFor(t: Task): string {
  if (!isBT(t) || !(t.files?.length ?? 0)) return "";
  if (t.status === 3) return "|files";
  if (t.status === 4 || t.status === 2) return "|files";
  return "";
}

function parseSel(s?: string): number[] | null {
  if (!s) return null;
  const n = s
    .split(",")
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x));
  return n.length ? n : null;
}

// in-place DOM nodes per task; buttons rebuilt only when the action set changes
const nodes = new Map<string, HTMLElement>();

function ensureNode(t: Task): HTMLElement {
  let el = nodes.get(t.id);
  if (!el) {
    el = document.createElement("div");
    el.className = "task";
    el.dataset.id = t.id;
    el.innerHTML = `
      <div class="row1">
        <span class="name"></span>
        <span class="badge"></span>
        <span class="size"></span>
        <span class="acts"></span>
      </div>
      <div class="bar"><i></i></div>
      <div class="meta">
        <span class="pct"></span>
        <span class="dl"></span>
        <span class="sp"></span>
        <span class="bt"></span>
        <span class="msg"></span>
      </div>`;
    nodes.set(t.id, el);
    $<HTMLDivElement>("#tasks")!.appendChild(el);
  }
  return el;
}

// Live message severity per task, driven by the event's Level field.
const msgLv = new Map<string, MsgLevel>();

function isBT(t: Task): boolean {
  const u = (t.req?.url || "").toLowerCase();
  return u.startsWith("magnet:?") || u.endsWith(".torrent") || !!t.files?.length;
}

function visible(t: Task): boolean {
  if (filter === "run") return t.status === 3 || t.status === 1;
  if (filter === "done") return t.status === 5;
  return true;
}

function render() {
  const host = $<HTMLDivElement>("#tasks");
  if (!host) return;
  const empty = $<HTMLDivElement>("#tasks .empty");
  const shown = [...tasks.values()].filter(visible).sort(bySort);
  if (shown.length === 0) {
    nodes.clear();
    host.innerHTML = '<div class="empty">' + (filter === "all" ? "暂无任务" : "该分类下暂无任务") + "</div>";
    return;
  }
  if (empty) empty.remove();
  for (const t of shown) {
    const el = ensureNode(t);
    host.appendChild(el); // keep DOM order in sync with the chosen sort
    el.className = `task${t.status === 5 ? " complete" : t.status === 6 ? " error" : ""}`;
    (el.querySelector(".name") as HTMLElement).textContent = t.name || t.req.url;
    (el.querySelector(".badge") as HTMLElement).textContent = STATUS_NAMES[t.status] ?? String(t.status);
    const segTotal = t.hls?.segUrls?.length ?? 0;
    (el.querySelector(".size") as HTMLElement).textContent = isBT(t)
      ? `BT · ${t.files?.length ? t.files.length + " 个文件 · " : "等待元数据 "}${fmtBytes(t.size)}`
      : segTotal
        ? `HLS 视频 · ${(t.hls!.done || []).filter(Boolean).length}/${segTotal} 分片`
        : (t.supportRange ? `${t.conn} 连接 · ` : "单连接 · ") + fmtBytes(t.size);
    (el.querySelector(".bar > i") as HTMLElement).style.width = pct(t) + "%";
    (el.querySelector(".pct") as HTMLElement).textContent = pct(t) + "%";
    (el.querySelector(".dl") as HTMLElement).textContent = `${fmtBytes(t.downloaded)} / ${fmtBytes(t.size)}`;
    (el.querySelector(".sp") as HTMLElement).textContent = t.status === 3 ? fmtSpeed(t.speed) : "";

    // BT swarm line: peers/seeds are the difference between "0 B/s 卡住" and a
    // healthy swarm — the user must see WHY it is slow (no seeds vs no network).
    const bt = isBT(t) ? btLive.get(t.id) : undefined;
    const btEl = el.querySelector(".bt") as HTMLElement;
    if (bt) {
      btEl.textContent = `节点 ${bt.peers} · 种子 ${bt.seeds} · 上传 ${fmtBytes(bt.uploaded)}`;
      btEl.title = "节点数=已连上的对端；种子数=拥有完整文件的对端（0 表示全网无人有完整文件，只能等）";
    } else {
      btEl.textContent = "";
    }

    const msg = el.querySelector(".msg") as HTMLElement;
    msg.textContent = t.status === 6 ? t.err : (t.info ?? "");
    msg.className = "msg " + (t.status === 6 ? "err" : (msgLv.get(t.id) ?? "info"));

    const acts = el.querySelector(".acts") as HTMLElement;
    const spec = actionsFor(t.status) + btFilesFor(t);
    if (acts.dataset.spec !== spec) {
      acts.dataset.spec = spec;
      acts.innerHTML = spec
        .split("|")
        .map((a) => {
          const label =
            a === "pause" ? "暂停"
            : a === "start" ? "开始"
            : a === "open" ? "打开位置"
            : a === "files" ? "文件"
            : "删除";
          return `<button data-act="${a}">${label}</button>`;
        })
        .join("");
    }
  }
}

async function act(id: string, a: string) {
  try {
    if (a === "open") {
      const t = tasks.get(id);
      if (t?.savePath) await invoke("reveal_path", { path: t.savePath });
      return;
    }
    if (a === "files") {
      await openFiles(id);
      return;
    }
    if (a === "pause") await api("POST", "/tasks/stop", { id });
    else if (a === "start") await api("POST", "/tasks/start", { id });
    else if (a === "del") {
      await api("DELETE", "/tasks", { id });
      tasks.delete(id);
      nodes.get(id)?.remove();
      nodes.delete(id);
    }
    await loadTasks();
    render();
  } catch (e) {
    setStatus(String(e), true);
  }
}

async function loadTasks() {
  const list = await api<Task[]>("GET", "/tasks");
  tasks.clear();
  for (const t of list) tasks.set(t.id, t);
}

function connectEvents() {
  if (!core) return;
  es = new EventSource(`http://127.0.0.1:${core.port}/api/v1/events?token=${core.token}`);
  es.onmessage = (m) => {
    const e = JSON.parse(m.data) as {
      type: string;
      taskId: string;
      status?: string;
      downloaded?: number;
      size?: number;
      speed?: number;
      err?: string;
      level?: string;
      attempt?: number;
      name?: string;
      peers?: number;
      seeds?: number;
      uploaded?: number;
    };
    const t = tasks.get(e.taskId);
    if (!t) {
      loadTasks().then(render).catch(() => {});
      return;
    }
    if (e.type === "progress") {
      t.downloaded = e.downloaded ?? t.downloaded;
      t.speed = e.speed ?? t.speed;
      if (e.peers !== undefined || e.seeds !== undefined || e.uploaded !== undefined) {
        const live = btLive.get(e.taskId) || { peers: 0, seeds: 0, uploaded: 0 };
        live.peers = e.peers ?? live.peers;
        live.seeds = e.seeds ?? live.seeds;
        live.uploaded = e.uploaded ?? live.uploaded;
        btLive.set(e.taskId, live);
      }
    } else if (e.type === "meta") {
      // Magnet metadata landed: the core renamed the task to the real torrent
      // name, filled in size + file list and computed the save path.
      t.name = e.name || t.name;
      if (e.size !== undefined) t.size = e.size;
      t.info = "元数据已获取，开始下载";
      msgLv.set(e.taskId, "info");
      loadTasks().catch(() => {});
    } else if (e.type === "status" || e.type === "done") {
      if (e.type === "done") t.status = 5;
      else
        t.status =
          e.status === "running"
            ? 3
            : e.status === "paused"
              ? 4
              : e.status === "complete"
                ? 5
                : e.status === "error"
                  ? 6
                  : t.status;
      if (e.downloaded !== undefined) t.downloaded = e.downloaded;
      if (e.type === "done") {
        t.info = "";
        msgLv.set(e.taskId, "info");
      }
    } else if (e.type === "error") {
      t.status = 6;
      t.info = "";
      if (e.err) t.err = e.err;
    } else if (e.type === "retry") {
      // Core's bounded auto-retry: show the reason, and colour by severity so a
      // normal quick retry does not look like a failure loop.
      t.info = e.err || "连接中断，正在自动重试";
      msgLv.set(e.taskId, e.level === "warn" ? "warn" : e.level === "error" ? "err" : "info");
    } else if (e.type === "stalled") {
      t.info = e.err || "连接停滞";
      msgLv.set(e.taskId, "warn");
    } else if (e.type === "slow" || e.type === "downgrade") {
      // server lied about Range / throttles per connection: core degraded
      // automatically; surface the reason instead of looking like an error.
      if (e.type === "downgrade") {
        t.status = 3;
        t.conn = 1;
        t.supportRange = false;
      }
      t.info = e.err || "";
      msgLv.set(e.taskId, "warn");
      setStatus(e.err || "");
    }
    render();
  };
  es.onerror = () => {
    setStatus("事件流断开，重连中…");
    es?.close();
    setTimeout(connectEvents, 1500);
  };
}

// M6.5 candidate picker: a pasted web page with several file links lets the
// user choose which file to download (迅雷 batch-download lite). The core
// already probed every link (name/size known); this UI only chooses.
interface Cand {
  url: string;
  name: string;
  size: number;
}

function pickCandidate(cands: Cand[], page: string): Promise<Cand | null> {
  const modal = $<HTMLDivElement>("#candmodal");
  const list = $<HTMLDivElement>("#candlist");
  if (!modal || !list) return Promise.resolve(cands[0] ?? null);
  list.innerHTML = "";
  cands.forEach((c, i) => {
    const lab = document.createElement("label");
    const r = document.createElement("input");
    r.type = "radio";
    r.name = "cand";
    r.value = String(i);
    r.checked = i === 0;
    lab.appendChild(r);
    lab.appendChild(
      document.createTextNode(` ${c.name || "未命名文件"} — ${fmtBytes(c.size)}`),
    );
    list.appendChild(lab);
  });
  const pg = $<HTMLDivElement>("#candpage");
  if (pg) pg.textContent = `来自页面：${page}`;
  modal.hidden = false;
  return new Promise((resolve) => {
    const done = (v: Cand | null) => {
      modal.hidden = true;
      const okb = $<HTMLButtonElement>("#cand-ok");
      const cb = $<HTMLButtonElement>("#cand-cancel");
      if (okb) okb.onclick = null;
      if (cb) cb.onclick = null;
      resolve(v);
    };
    const okb = $<HTMLButtonElement>("#cand-ok");
    const cb = $<HTMLButtonElement>("#cand-cancel");
    if (okb)
      okb.onclick = () => {
        const sel = list.querySelector<HTMLInputElement>('input[name="cand"]:checked');
        done(sel ? cands[Number(sel.value)] : cands[0]);
      };
    if (cb) cb.onclick = () => done(null);
  });
}

// --- BT/magnet per-file selection (迅雷/IDM "files" detail) ---
type FileItem = { index: number; name: string; size: number; selected: boolean };

async function openFiles(id: string) {
  const modal = $<HTMLDivElement>("#filemodal");
  const list = $<HTMLDivElement>("#filelist");
  if (!modal || !list) return;
  const t = tasks.get(id);
  let items: FileItem[];
  try {
    // engine running: authoritative live list (also live selected flags)
    items = await api<FileItem[]>("GET", "/bt/files?id=" + encodeURIComponent(id));
    const hint = $<HTMLDivElement>("#filehint");
    if (hint)
      hint.textContent =
        t && t.status === 3
          ? "运行中：取消勾选立即停止该文件后续下载（已下载片段保留）"
          : "任务未运行：改选将在下次开始时生效";
  } catch {
    // engine not running (ready/paused): fall back to the persisted snapshot
    const sel = parseSel(t?.btSel);
    items = (t?.files ?? []).map((f, i) => ({
      index: i,
      name: f.name,
      size: f.size,
      selected: sel ? sel.includes(i) : true,
    }));
    const hint = $<HTMLDivElement>("#filehint");
    if (hint) hint.textContent = "任务当前未运行：改选将保存，下次开始时生效";
  }
  if (!items.length) {
    setStatus("该任务还没有文件清单（等待元数据）", true);
    return;
  }
  list.innerHTML = "";
  const boxes: HTMLInputElement[] = [];
  for (const it of items) {
    const lab = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = it.selected;
    boxes.push(cb);
    lab.appendChild(cb);
    lab.appendChild(document.createTextNode(` ${it.name} — ${fmtBytes(it.size)}`));
    lab.style.display = "block";
    list.appendChild(lab);
  }
  modal.hidden = false;
  const close = () => {
    modal.hidden = true;
    const okb = $<HTMLButtonElement>("#file-ok");
    const cbtn = $<HTMLButtonElement>("#file-cancel");
    if (okb) okb.onclick = null;
    if (cbtn) cbtn.onclick = null;
  };
  const cbtn = $<HTMLButtonElement>("#file-cancel");
  if (cbtn) cbtn.onclick = () => close();
  const okb = $<HTMLButtonElement>("#file-ok");
  if (okb)
    okb.onclick = async () => {
      const keep: number[] = [];
      for (let i = 0; i < items.length; i++) if (boxes[i].checked) keep.push(items[i].index);
      if (!keep.length) {
        setStatus("至少勾选一个文件", true);
        return;
      }
      try {
        await api("POST", "/bt/selection", { id, files: keep });
      } catch (e) {
        setStatus(String(e), true);
        return;
      }
      close();
      await loadTasks();
      render();
    };
}

async function addTask(url: string) {
  // http(s): resolve first so a page with multiple links asks the user
  // (magnet/ed2k/bt go straight to Create, which resolves internally).
  if (/^https?:/i.test(url)) {
    try {
      const res = await api<{
        isPage?: boolean;
        candidates?: Cand[];
      }>("POST", "/resolve", { url });
      if (res?.isPage && (res.candidates?.length ?? 0) > 1) {
        const pick = await pickCandidate(res.candidates!, url);
        if (!pick) return; // user cancelled: nothing added
        const t = await api<Task>("POST", "/tasks", { url: pick.url, referer: url });
        tasks.set(t.id, t);
        render();
        await api("POST", "/tasks/start", { id: t.id });
        setStatus("");
        return;
      }
    } catch {
      // resolve failed: fall through — Create shows the real error
    }
  }
  const t = await api<Task>("POST", "/tasks", { url });
  tasks.set(t.id, t);
  render();
  await api("POST", "/tasks/start", { id: t.id });
  setStatus("");
}

window.addEventListener("DOMContentLoaded", async () => {
  // event delegation: buttons survive in-place re-renders
  $<HTMLDivElement>("#tasks")!.addEventListener("click", (ev) => {
    const btn = (ev.target as HTMLElement).closest?.("button[data-act]") as HTMLElement | null;
    const card = (ev.target as HTMLElement).closest?.(".task") as HTMLElement | null;
    if (btn && card?.dataset.id) act(card.dataset.id, btn.dataset.act!);
  });

  $<HTMLFormElement>("#addform")!.addEventListener("submit", (ev) => {
    ev.preventDefault();
    const input = $<HTMLInputElement>("#url");
    const url = input?.value.trim() ?? "";
    if (!url) return;
    if (input) input.value = "";
    addTask(url).catch((e) => setStatus(String(e), true));
  });

  try {
    core = await coreReady();
    const badge = $<HTMLSpanElement>("#core-badge");
    if (badge) {
      // The badge used to say "core: 127.0.0.1:port" only, so a new build and a
      // stale one looked identical. Show the core version + build stamp (served
      // by GET /api/v1/version, stamped at compile time) so "我打开的是新版吗"
      // is answerable at a glance.
      const setBadge = (ver?: string) => {
        badge.textContent = ver
          ? `core v${ver} · :${core!.port}`
          : `core: 127.0.0.1:${core!.port}`;
      };
      setBadge();
      badge.title = "点击复制连接信息（浏览器扩展设置用）";
      badge.style.cursor = "pointer";
      badge.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(`${core!.port}|${core!.token}`);
          setStatus("已复制连接信息（端口|令牌），粘贴到浏览器扩展的 Sun_Tey 设置");
        } catch {
          setStatus(`${core!.port}|${core!.token}`, true); // show for manual copy
        }
      });
      api<{ version: string; build: string; features: string[] }>("GET", "/version")
        .then((v) => {
          setBadge(v.version);
          badge.title = `内核 ${v.version}（构建 ${v.build}）\n` +
            (v.features || []).join("\n") +
            "\n\n点击复制连接信息（浏览器扩展设置用）";
          versionInfo = v;
        })
        .catch(() => {}); // old core without the route: keep the plain badge
    }
    await loadTasks();
    render();
    connectEvents();
  } catch (e) {
    setStatus(String(e), true);
    render();
  }
});

// ---------- 设置面板 ----------
const $gear = document.getElementById("gearbtn") as HTMLButtonElement;
const $settings = document.getElementById("settings") as HTMLDivElement;
const $dir = document.getElementById("cfg-dir") as HTMLInputElement;
const $conn = document.getElementById("cfg-conn") as HTMLSelectElement;
const $max = document.getElementById("cfg-max") as HTMLInputElement;
const $buf = document.getElementById("cfg-buf") as HTMLInputElement;
const $rate = document.getElementById("cfg-rate") as HTMLInputElement;
const $ua = document.getElementById("cfg-ua") as HTMLInputElement;
const $clip = document.getElementById("cfg-clip") as HTMLInputElement;
const $btport = document.getElementById("cfg-btport") as HTMLInputElement;
const $btseed = document.getElementById("cfg-btseed") as HTMLInputElement;
const $btnoup = document.getElementById("cfg-btnoup") as HTMLInputElement;
const $btstats = document.getElementById("bt-stats") as HTMLDivElement;
const $extpath = document.getElementById("ext-path") as HTMLDivElement;
const $save = document.getElementById("cfg-save") as HTMLButtonElement;
const $cancel = document.getElementById("cfg-cancel") as HTMLButtonElement;
const $browse = document.getElementById("cfg-browse") as HTMLButtonElement;

let cfgCache: any = null;
let extPath = "";
// Core version/build/features, fetched once at boot (GET /api/v1/version) and
// surfaced in the settings panel; the badge in the header shows the version.
let versionInfo: { version: string; build: string; features: string[] } | null = null;

async function openSettings() {
  try {
    cfgCache = await api("GET", "/config");
  } catch (e: any) {
    setStatus(`读取配置失败：${e?.message ?? e}`, true);
    return;
  }
  $dir.value = cfgCache.downloadDir ?? "";
  $conn.value = String(cfgCache.defaultConn ?? 8);
  $max.value = String(cfgCache.maxRunning ?? 0);
  $buf.value = String(cfgCache.writeBufferKB ?? 128);
  $rate.value = String(cfgCache.rateLimitKBps ?? 0);
  $ua.value = (cfgCache.userAgent || cfgCache.defaultUserAgent || "").trim();
  $clip.checked = cfgCache.clipboardWatch !== false;
  $btport.value = String(cfgCache.btPort ?? 0);
  $btseed.checked = cfgCache.btSeedAfter !== false;
  $btnoup.checked = cfgCache.btNoUpload === true;
  $settings.hidden = false;
  // Version + feature list: makes "我打开的 exe 是新版吗" answerable in one look.
  const verline = $<HTMLDivElement>("#ver-line");
  if (verline) {
    if (versionInfo) {
      verline.textContent = `内核 v${versionInfo.version}（构建 ${versionInfo.build}）：` +
        (versionInfo.features || []).join(" · ");
    } else {
      api<{ version: string; build: string; features: string[] }>("GET", "/version")
        .then((v) => {
          versionInfo = v;
          verline.textContent = `内核 v${v.version}（构建 ${v.build}）：` + (v.features || []).join(" · ");
        })
        .catch(() => { verline.textContent = "内核版本：读取失败（旧内核无 /version 接口）"; });
    }
  }
  // BT + extension info load lazily: neither may block the panel opening.
  api<any>("GET", "/bt/stats")
    .then((s) => {
      $btstats.textContent = s.running
        ? `BT 状态：监听端口 ${s.listenPort} · 任务 ${s.torrents} 个 · 节点 ${s.peers} · 种子 ${s.seeds} · 累计上传 ${fmtBytes(s.uploaded)}`
        : "BT 状态：未启动（首次下载磁力/种子时自动启动）";
    })
    .catch(() => {
      $btstats.textContent = "BT 状态：读取失败";
    });
  try {
    extPath = await invoke<string>("extension_dir");
    $extpath.textContent = extPath;
  } catch (e: any) {
    $extpath.textContent = String(e?.message ?? e);
  }
}

$gear.addEventListener("click", openSettings);
$cancel.addEventListener("click", () => ($settings.hidden = true));
$settings.addEventListener("click", (e) => {
  if ((e.target as HTMLElement).id === "settings") $settings.hidden = true;
});

document.getElementById("ext-open")!.addEventListener("click", async () => {
  if (!extPath) {
    setStatus("扩展目录未定位，请先打开设置面板", true);
    return;
  }
  try {
    await invoke("open_dir", { path: extPath });
  } catch (e: any) {
    setStatus(String(e?.message ?? e), true);
  }
});
document.getElementById("ext-copy")!.addEventListener("click", async () => {
  if (!extPath) return;
  try {
    await navigator.clipboard.writeText(extPath);
    setStatus("已复制扩展文件夹路径");
  } catch {
    setStatus(extPath, true);
  }
});
document.getElementById("ext-cfg")!.addEventListener("click", async () => {
  if (!core) return;
  const s = `${core.port}|${core.token}`;
  try {
    await navigator.clipboard.writeText(s);
    setStatus("已复制「端口|令牌」，粘贴到浏览器扩展的设置里");
  } catch {
    setStatus(s, true);
  }
});

$browse.addEventListener("click", async () => {
  try {
    const picked = await openDialog({ directory: true, defaultPath: $dir.value, title: "选择下载目录" });
    if (picked) $dir.value = picked as string;
  } catch {
    setStatus("目录选择不可用，请直接输入路径", true);
  }
});

$save.addEventListener("click", async () => {
  $save.disabled = true;
  try {
    const body: any = {
      downloadDir: $dir.value.trim(),
      defaultConn: parseInt($conn.value, 10) || 8,
      maxRunning: Math.max(0, parseInt($max.value, 10) || 0),
      writeBufferKB: Math.max(16, parseInt($buf.value, 10) || 128),
      rateLimitKBps: Math.max(0, parseInt($rate.value, 10) || 0),
      userAgent: $ua.value.trim(),
      clipboardWatch: $clip.checked,
      btPort: Math.max(0, parseInt($btport.value, 10) || 0),
      btSeedAfter: $btseed.checked,
      btNoUpload: $btnoup.checked,
    };
    cfgCache = await api("PUT", "/config", body);
    $settings.hidden = true;
    setStatus("设置已保存并即时生效");
  } catch (e: any) {
    setStatus(`保存失败：${e?.message ?? e}`, true);
  } finally {
    $save.disabled = false;
  }
});

// ---------- 种子文件：桌面对话框给出本地路径，内核自己解析 ----------
const $torrent = document.getElementById("torrentbtn") as HTMLButtonElement;
$torrent.addEventListener("click", async () => {
  try {
    const picked = await openDialog({
      multiple: false,
      title: "选择 .torrent 种子文件",
      filters: [{ name: "BitTorrent 种子", extensions: ["torrent"] }],
    });
    if (!picked) return;
    await addTask(String(picked));
  } catch (e: any) {
    setStatus(`种子添加失败：${e?.message ?? e}`, true);
  }
});

// ---------- 列表过滤 ----------
{
  const sel = document.getElementById("sortsel") as HTMLSelectElement | null;
  if (sel) {
    sel.value = sortMode;
    sel.addEventListener("change", () => {
      sortMode = sel.value as SortMode;
      localStorage.setItem("suntey.sort", sortMode);
      render();
    });
  }
}
for (const [id, key] of [
  ["tab-all", "all"],
  ["tab-run", "run"],
  ["tab-done", "done"],
] as const) {
  const b = document.getElementById(id) as HTMLButtonElement;
  b.addEventListener("click", () => {
    filter = key;
    for (const o of ["tab-all", "tab-run", "tab-done"]) {
      document.getElementById(o)!.classList.toggle("on", o === id);
    }
    render();
  });
}
