const $ = (s) => document.querySelector(s);

function parseConn(v) {
  v = v.trim();
  const i = v.lastIndexOf("|");
  if (i < 0) return { port: "17332", token: v };
  return { port: v.slice(0, i).trim() || "17332", token: v.slice(i + 1).trim() };
}

async function test(port, token) {
  const r = await chrome.runtime.sendMessage({ type: "test" });
  return r;
}

chrome.storage.local.get({ port: "17332", token: "" }, (c) => {
  $("#conn").value = c.token ? c.port + "|" + c.token : "";
  if (c.token) probe();
});

async function probe() {
  $("#msg").textContent = "测试连接中…";
  $("#msg").className = "";
  const r = await chrome.runtime.sendMessage({ type: "test" });
  if (r && r.ok) {
    $("#msg").textContent = "✓ 已连接 Sun_Tey 内核";
    $("#msg").className = "ok";
  } else if (r && r.needToken) {
    $("#msg").textContent = "内核已在线 ✓，只需令牌：在 Sun_Tey 界面设置里点「复制」，再点上方「粘贴」";
    $("#msg").className = "warn";
  } else {
    $("#msg").textContent = "✗ 连不上：先打开 Sun_Tey 桌面程序，再检查令牌";
    $("#msg").className = "err";
  }
}

$("#paste").onclick = async () => {
  try {
    $("#conn").value = (await navigator.clipboard.readText()).trim();
  } catch (e) {
    $("#msg").textContent = "剪贴板读取失败，请手动粘贴 (Ctrl+V)";
    $("#msg").className = "err";
  }
};

$("#save").onclick = async () => {
  const { port, token } = parseConn($("#conn").value);
  await chrome.storage.local.set({ port, token });
  probe();
};
