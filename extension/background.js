// Sun_Tey MV3 background worker: media sniffing + core API proxy (adds cookies).
const MEDIA_RE = /\.(m3u8|mp4|webm|flv|mkv|avi|mov|m4v|mp3|m4a|ogg)([?#]|$)/i;

// ---- right-click "用 Sun_Tey 下载" (links/videos/audio/images/URL selection) ----
const VER = chrome.runtime.getManifest().version;

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.removeAll(() => {
    chrome.contextMenus.create({
      id: "suntey-dl",
      title: "用 Sun_Tey 下载 (v" + VER + ")",
      contexts: ["link", "video", "audio", "image", "selection"],
    });
  });
});

// ---- pairing badge: token 未保存 / 内核连不上 时工具栏图标给提示 ----
async function badge(text, tip) {
  try {
    await chrome.action.setBadgeBackgroundColor({ color: "#e8590c" });
    await chrome.action.setBadgeText({ text });
    if (tip) await chrome.action.setTitle({ title: tip });
  } catch (_) {}
}
function badgeClear() {
  try {
    chrome.action.setBadgeText({ text: "" });
    chrome.action.setTitle({ title: "Sun_Tey 设置" });
  } catch (_) {}
}
async function paired() {
  const { token } = await cfgGet();
  return !!token;
}

// shared start: create task with page cookies/referer, then start it
async function startDownload(url, referer, ua, extra) {
  const header = {};
  if (referer) header.Referer = referer;
  if (ua) header["User-Agent"] = ua;
  if (extra) Object.assign(header, extra);
  try {
    const cookies = await chrome.cookies.getAll({ url });
    if (cookies.length)
      header.Cookie = cookies.map((c) => `${c.name}=${c.value}`).join("; ");
  } catch (_) {}
  const r = await api("/tasks", {
    method: "POST",
    body: JSON.stringify({ url, header }),
  });
  const j = await r.json();
  if (!j.ok) return { ok: false, error: j.error };
  await api("/tasks/start", {
    method: "POST",
    body: JSON.stringify({ id: j.data.id }),
  });
  return { ok: true, id: j.data.id };
}

chrome.contextMenus.onClicked.addListener(async (info, tab) => {
  let url = "";
  if (info.mediaType === "video" || info.mediaType === "audio" || info.mediaType === "image") {
    url = info.srcElement ? info.srcElement.src : "";
  } else if (info.linkUrl) {
    url = info.linkUrl;
  } else if (info.selectionText && /^https?:\/\/\S+$/i.test(info.selectionText.trim())) {
    url = info.selectionText.trim();
  }
  if (!url) return;
  if (!(await paired())) {
    badge(
      "未配对",
      "先在 Sun_Tey 桌面窗口配对：点橙色 core 徽标复制「端口|令牌」，到扩展弹窗粘贴保存"
    );
    return;
  }
  try {
    const r = await startDownload(
      url,
      info.pageUrl || (tab && tab.url) || "",
      (tab && tab.userAgent) || ""
    );
    if (r && r.ok) badgeClear();
    else
      badge("离线", "连不上 Sun_Tey 内核：确认桌面程序已启动，且扩展中保存的端口/令牌有效");
  } catch (_) {
    badge("离线", "连不上 Sun_Tey 内核：确认桌面程序已启动，且扩展中保存的端口/令牌有效");
  }
});

function cfgGet() {
  return chrome.storage.local.get({ port: "17332", token: "" });
}

async function api(path, opts = {}) {
  const { port, token } = await cfgGet();
  opts.headers = Object.assign(
    { "Content-Type": "application/json", "X-SunTey-Token": token },
    opts.headers || {}
  );
  return fetch(`http://127.0.0.1:${port}/api/v1${path}`, opts);
}

async function pushMedia(tabId, item) {
  const key = "m" + tabId;
  const all = await chrome.storage.session.get(key);
  const list = all[key] || [];
  if (!list.some((x) => x.url === item.url)) {
    list.push(item);
    if (list.length > 40) list.shift();
    await chrome.storage.session.set({ [key]: list });
  }
  try {
    chrome.tabs.sendMessage(tabId, { type: "media", items: list.slice(-12) });
  } catch (_) {}
}

chrome.webRequest.onResponseStarted.addListener(
  (d) => {
    if (d.tabId < 0) return;
    let type = null;
    const m = d.url.match(MEDIA_RE);
    if (m) type = m[1].toLowerCase();
    if (!type) {
      const ct = ((d.responseHeaders || []).find(
        (h) => h.name.toLowerCase() === "content-type"
      ) || {}).value || "";
      if (/^video\//i.test(ct) && (d.type === "media" || d.type === "xmlhttprequest"))
        type = ct.split("/")[1].split(";")[0];
    }
    if (!type) return;
    pushMedia(d.tabId, { url: d.url, type, ts: Date.now() });
  },
  { urls: ["<all_urls>"], types: ["media", "xmlhttprequest", "object", "other"] },
  ["responseHeaders"]
);

chrome.tabs.onRemoved.addListener((id) =>
  chrome.storage.session.remove("m" + id)
);

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    if (msg.type === "test") {
      const st = await chrome.storage.local.get({ port: "17332", token: "" });
      if (!st.token) {
        // No token yet: the token-free handshake still distinguishes
        // "core is alive, fill the token" from "nothing on this port".
        try {
          const h = await fetch(`http://127.0.0.1:${st.port}/api/v1/handshake`);
          if (h.ok) {
            sendResponse({ ok: false, alive: true, needToken: true });
            return;
          }
        } catch (e) {}
        sendResponse({ ok: false, error: "127.0.0.1:" + st.port + " 上没有 Sun_Tey 内核" });
        return;
      }
      try {
        const r = await api("/tasks");
        if (r.ok) badgeClear();
        sendResponse({ ok: r.ok, status: r.status });
      } catch (e) {
        sendResponse({ ok: false, error: String(e) });
      }
      return;
    }
    if (msg.type === "media") {
      const tid = sender.tab ? sender.tab.id : msg.tabId;
      const all = await chrome.storage.session.get("m" + tid);
      sendResponse({ items: (all["m" + tid] || []).slice(-12) });
      return;
    }
    if (msg.type === "download") {
      try {
        const extra = msg.origin ? { Origin: msg.origin } : null;
        sendResponse(await startDownload(msg.url, msg.referer, msg.ua, extra));
      } catch (e) {
        sendResponse({ ok: false, error: String(e) });
      }
      return;
    }
  })();
  return true;
});
