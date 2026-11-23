// Sun_Tey page overlay: floating download button over <video>/<audio> + sniff list.
(() => {
  if (window.__suntey) return;
  window.__suntey = true;

  const btn = document.createElement("button");
  btn.className = "suntey-btn";
  btn.textContent = "⬇ 下载";
  btn.style.display = "none";
  document.documentElement.appendChild(btn);

  let cur = null;
  let hideT = null;

  function place(v) {
    const r = v.getBoundingClientRect();
    if (r.width < 100 || r.height < 50) return hide();
    btn.style.top = r.top + 8 + "px";
    btn.style.left = Math.max(8, r.right - 96) + "px";
    const src = v.currentSrc || v.src || "";
    btn.dataset.src = src;
    btn.style.display = "block";
    btn.classList.remove("sent");
    btn.textContent = src && !src.startsWith("blob:") ? "⬇ 下载" : "🔒 MSE";
    cur = v;
  }
  function hide() {
    btn.style.display = "none";
    cur = null;
  }

  document.addEventListener(
    "mouseover",
    (e) => {
      const v = e.target.closest && e.target.closest("video,audio");
      if (v) {
        clearTimeout(hideT);
        place(v);
      }
    },
    true
  );
  document.addEventListener(
    "mouseout",
    (e) => {
      const v = e.target.closest && e.target.closest("video,audio");
      if (v) hideT = setTimeout(hide, 900);
    },
    true
  );
  btn.addEventListener("mouseenter", () => clearTimeout(hideT));
  btn.addEventListener("mouseleave", () => {
    hideT = setTimeout(hide, 400);
  });
  window.addEventListener("scroll", () => cur && place(cur), true);
  window.addEventListener("resize", () => cur && place(cur));

  btn.addEventListener("click", () => {
    const src = btn.dataset.src || "";
    if (!src || src.startsWith("blob:")) {
      toast("这是 MSE/加密流，请用右下角嗅探列表里的 m3u8/mp4 下载");
      return;
    }
    send(src);
  });

  function send(url) {
    chrome.runtime.sendMessage(
      {
        type: "download",
        url,
        referer: location.href,
        origin: location.origin,
        ua: navigator.userAgent,
      },
      (r) => {
        if (chrome.runtime.lastError)
          return toast("扩展通信失败: " + chrome.runtime.lastError.message);
        if (r && r.ok) {
          btn.textContent = "✓ 已发送";
          btn.classList.add("sent");
          toast("已发送到 Sun_Tey ✓ 在桌面窗口看进度");
        } else {
          toast("发送失败: " + (r && r.error));
        }
      }
    );
  }

  function toast(t) {
    const d = document.createElement("div");
    d.className = "suntey-toast";
    d.textContent = t;
    document.documentElement.appendChild(d);
    setTimeout(() => d.remove(), 2400);
  }

  // ---- sniff list panel ----
  const panel = document.createElement("div");
  panel.className = "suntey-panel";
  panel.style.display = "none";
  document.documentElement.appendChild(panel);

  panel.addEventListener("click", (e) => {
    if (e.target.classList.contains("suntey-all")) {
      let n = 0;
      let hls = 0;
      for (const i of lastItems) {
        send(i.url);
        n++;
        if (i.type === "m3u8") hls++;
      }
      e.target.textContent = "已发 " + n;
      toast(
        "已发送 " + n + " 个到 Sun_Tey" + (hls ? "（含 " + hls + " 个 m3u8，内核会下载分片并合成 MP4）" : "")
      );
      return;
    }
    if (e.target.classList.contains("suntey-hd")) {
      panel.classList.toggle("min");
      return;
    }
    const b = e.target.closest(".suntey-dl");
    if (!b) return;
    b.textContent = "…";
    send(b.dataset.url);
  });

  function esc(s) {
    return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
  }
  let lastItems = [];
  function render(items) {
    items = items || [];
    lastItems = items;
    if (!items.length) {
      panel.style.display = "none";
      return;
    }
    panel.style.display = "block";
    const nonHls = items.filter((i) => i.type !== "m3u8").length;
    panel.innerHTML =
      '<div class="suntey-hd">Sun_Tey 嗅探到 ' +
      items.length +
      " 个媒体（点标题收起/展开）" +
      (nonHls
        ? '<button class="suntey-all" title="下载全部媒体（跳过 m3u8 清单）">全部下载 ' +
          nonHls +
          "</button>"
        : "") +
      "</div>" +
      items
        .map((i) => {
          const name = i.url.split("/").pop().split(/[?#]/)[0] || i.url;
          const isHls = i.type === "m3u8";
          const act =
            '<button class="suntey-dl" title="' +
            (isHls
              ? "下载这个 m3u8：内核下载全部分片并合成 MP4（可断点续传）"
              : "发送到 Sun_Tey 下载") +
            '" data-url="' +
            esc(i.url) +
            '">' +
            (isHls ? "下载视频" : "下载") +
            "</button>";
          return (
            '<div class="suntey-row"><span class="suntey-tag">' +
            i.type +
            '</span><span class="suntey-name" title="' +
            esc(i.url) +
            '">' +
            esc(name) +
            "</span>" +
            act +
            "</div>"
          );
        })
        .join("");
  }

  chrome.runtime.onMessage.addListener((m) => {
    if (m.type === "media") render(m.items);
  });
  chrome.runtime.sendMessage({ type: "media" }, (r) => {
    if (r && r.items) render(r.items);
  });
})();
