use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{Manager, State};

/// Hosted Go sidecar: process handle + loopback endpoint discovered from its
/// first stdout line {"type":"listening","port":N,"token":"..."}.
/// stderr tail is kept for UI diagnostics (spawn/store-lock failures).
pub struct Sidecar {
    child: Mutex<Option<Child>>,
    port: Mutex<u16>,
    token: Mutex<String>,
    errlog: Arc<Mutex<String>>,
}

fn find_sidecar(app: &tauri::App) -> Option<PathBuf> {
    let mut cands: Vec<PathBuf> = Vec::new();
    if let Ok(dir) = app.path().resource_dir() {
        cands.push(dir.join("binaries").join("suntey-core.exe"));
        cands.push(dir.join("suntey-core.exe"));
    }
    // portable layout: Sun_Tey.exe next to suntey-core.exe
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            cands.push(dir.join("suntey-core.exe"));
        }
    }
    // dev fallback: ../../core/bin/suntey-core.exe relative to src-tauri
    cands.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../core/bin/suntey-core.exe"));
    cands.into_iter().find(|p| p.exists())
}

fn spawn_sidecar(app: &tauri::App) {
    let state = app.state::<Sidecar>();
    let exe = match find_sidecar(app) {
        Some(e) => e,
        None => {
            *state.errlog.lock().unwrap() =
                "suntey-core.exe not found (bundle resources / exe dir / ../../core/bin)".into();
            return;
        }
    };
    // store file lives next to the MAIN exe: portable root (sidecar sits there
    // too) or the install INSTDIR. The installed sidecar lives in binaries/ —
    // a db stranded there also blocks uninstall while the core runs (file
    // lock). Fall back to the sidecar dir when current_exe is unavailable.
    let store = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("suntey.db")))
        .or_else(|| exe.parent().map(|d| d.join("suntey.db")))
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    let mut cmd = Command::new(&exe);
    // default port 17332 (stable for browser extension); busy → core falls back
    cmd.args(["serve"]);
    // anacrolix storage defaults to mmap fileIo; CreateFileMapping fails on some
    // Windows hosts (ERROR_BAD_EXE_FORMAT). The storage package only reads this
    // env at process init, so it must be injected here at spawn time.
    cmd.env("TORRENT_STORAGE_DEFAULT_FILE_IO", "classic");
    if !store.is_empty() {
        cmd.args(["--store", &store]);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            *state.errlog.lock().unwrap() = format!("spawn failed: {e}");
            return;
        }
    };

    // capture stderr tail on a background thread (non-blocking for UI)
    let errlog = state.errlog.clone();
    if let Some(err) = child.stderr.take() {
        std::thread::spawn(move || {
            let mut tail = String::new();
            for line in BufReader::new(err).lines().map_while(Result::ok) {
                tail.push_str(&line);
                tail.push('\n');
                if tail.len() > 4000 {
                    // keep the tail, UTF-8 safe
                    tail = tail.chars().rev().take(3000).collect::<String>();
                    tail = tail.chars().rev().collect();
                }
                *errlog.lock().unwrap() = tail.clone();
            }
        });
    }

    let out = child.stdout.take().unwrap();
    let line = BufReader::new(out)
        .lines()
        .next()
        .and_then(|r| r.ok())
        .unwrap_or_default();
    let v: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
    if v["type"] == "listening" {
        *state.port.lock().unwrap() = v["port"].as_u64().unwrap_or(0) as u16;
        *state.token.lock().unwrap() = v["token"].as_str().unwrap_or_default().to_string();
        println!(
            "[sidecar] listening on 127.0.0.1:{} (exe {})",
            v["port"],
            exe.display()
        );
    } else {
        *state.errlog.lock().unwrap() =
            format!("no listening handshake, first stdout line: {line}");
    }
    *state.child.lock().unwrap() = Some(child);
}

#[tauri::command]
fn sidecar_info(state: State<Sidecar>) -> Result<Value, String> {
    let port = *state.port.lock().unwrap();
    if port == 0 {
        let tail = state.errlog.lock().unwrap().clone();
        let msg = if tail.trim().is_empty() {
            "sidecar starting".to_string()
        } else {
            format!("内核启动失败: {}", tail.trim())
        };
        return Err(msg);
    }
    Ok(json!({ "port": port, "token": *state.token.lock().unwrap() }))
}

/// "打开位置": reveal the downloaded file in Windows Explorer.
/// Deliberately NOT tauri-plugin-opener: its capability scope whitelist
/// rejects custom save dirs (the "Not allowed to open path D:\Download"
/// bug). explorer /select works for any drive and highlights the file.
#[tauri::command]
fn reveal_path(path: String) -> Result<(), String> {
    let p = std::path::Path::new(&path);
    if !p.exists() {
        return Err("文件不存在：可能已被移动或删除".into());
    }
    #[cfg(windows)]
    {
        std::process::Command::new("explorer")
            .arg(format!("/select,{path}"))
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("无法打开资源管理器: {e}"))
    }
    #[cfg(not(windows))]
    {
        Err("暂只支持在 Windows 打开位置".into())
    }
}

/// Location of the bundled browser extension folder, for the 引导 flow
/// (chrome://extensions → 开发者模式 → 加载已解压的扩展程序).
/// Search order mirrors find_sidecar: installed bundle (resources/extension),
/// portable layout (exe dir/extension), then the repo checkout for dev runs.
#[tauri::command]
fn extension_dir(app: tauri::AppHandle) -> Result<String, String> {
    let mut cands: Vec<PathBuf> = Vec::new();
    if let Ok(dir) = app.path().resource_dir() {
        cands.push(dir.join("extension"));
        cands.push(dir.join("binaries").join("extension"));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            cands.push(dir.join("extension"));
            cands.push(dir.join("ext"));
        }
    }
    cands.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ext"));
    for c in &cands {
        if c.join("manifest.json").exists() {
            return Ok(c.to_string_lossy().to_string());
        }
    }
    Err("未找到扩展目录（打包时未捆绑 extension/）".into())
}

/// Open a folder in Explorer (plain open, no /select) — used by the 引导 flow so
/// the user can drag the folder onto chrome://extensions.
#[tauri::command]
fn open_dir(path: String) -> Result<(), String> {
    let p = std::path::Path::new(&path);
    if !p.exists() {
        return Err("目录不存在".into());
    }
    #[cfg(windows)]
    {
        std::process::Command::new("explorer")
            .arg(path)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("无法打开资源管理器: {e}"))
    }
    #[cfg(not(windows))]
    {
        Err("暂只支持 Windows".into())
    }
}

/// Tray icon (menu: show/hide + quit). Quit routes through RunEvent::Exit so
/// the sidecar gets its graceful snapshot+stop before the process tree dies.
fn build_tray(app: &tauri::App) -> tauri::Result<()> {
    let show = MenuItem::with_id(app, "show", "显示 / 隐藏 主界面", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "退出 Sun_Tey", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&show, &quit])?;
    let _tray = TrayIconBuilder::with_id("suntey-tray")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, ev| match ev.id.0.as_str() {
            "show" => toggle_main_window(app),
            "quit" => app.exit(0),
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if let tauri::tray::TrayIconEvent::Click {
                button: tauri::tray::MouseButton::Left,
                button_state: tauri::tray::MouseButtonState::Up,
                ..
            } = event
            {
                toggle_main_window(tray.app_handle());
            }
        })
        .build(app)?;
    Ok(())
}

fn toggle_main_window(app: &tauri::AppHandle) {
    if let Some(w) = app.get_webview_window("main") {
        if w.is_visible().unwrap_or(true) {
            let _ = w.hide();
        } else {
            let _ = w.show();
            let _ = w.set_focus();
        }
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            // second launch: bring the existing window back for real. The old
            // window may be hidden (closed-to-tray) or minimised, and
            // set_focus alone does neither — the user would click the exe and
            // see nothing. unminimise → show → focus, like Thunder/IDM.
            use tauri::Manager;
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.unminimize();
                let _ = w.show();
                let _ = w.set_focus();
            }
        }))
        .plugin(tauri_plugin_dialog::init())
        .manage(Sidecar {
            child: Mutex::new(None),
            port: Mutex::new(0),
            token: Mutex::new(String::new()),
            errlog: Arc::new(Mutex::new(String::new())),
        })
        .setup(|app| {
            spawn_sidecar(app);
            build_tray(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            // Close-to-tray (IDM/迅雷 style): X hides the window, downloads keep
            // running in background; tray menu "退出 Sun_Tey" exits for real and
            // routes through RunEvent::Exit → graceful_shutdown (snapshot+stop).
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![sidecar_info, reveal_path, extension_dir, open_dir])
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(|app_handle, event| {
        if let tauri::RunEvent::Exit = event {
            if let Some(state) = app_handle.try_state::<Sidecar>() {
                graceful_shutdown(&state);
                if let Some(mut c) = state.child.lock().unwrap().take() {
                    let _ = c.kill(); // fallback if the core ignored shutdown
                }
            }
        }
    });
}

/// Ask the core to pause all running tasks (persisting chunk progress for
/// resume) before the window goes away. Raw TcpStream — no HTTP client dep.
/// Must complete fast: the process tree dies seconds after Exit.
fn graceful_shutdown(state: &Sidecar) -> bool {
    let port = *state.port.lock().unwrap();
    let token = state.token.lock().unwrap().clone();
    if port == 0 {
        return false;
    }
    let req = format!(
        "POST /api/v1/shutdown HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-SunTey-Token: {token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    use std::io::{Read, Write};
    use std::time::Duration;
    let ok = std::net::TcpStream::connect(("127.0.0.1", port))
        .and_then(|mut s| {
            s.set_write_timeout(Some(Duration::from_millis(400)))?;
            s.set_read_timeout(Some(Duration::from_millis(900)))?;
            s.write_all(req.as_bytes())?;
            s.flush()?;
            let mut buf = [0u8; 128];
            let _ = s.read(&mut buf); // ack means every running task was cancelled
            Ok(())
        })
        .is_ok();
    if ok {
        // The core persists each task's chunk snapshots shortly after the ack
        // and self-exits ~1.2s later. Outlive that so the fallback kill()
        // cannot race the snapshots to disk.
        std::thread::sleep(Duration::from_millis(1500));
    }
    ok
}
