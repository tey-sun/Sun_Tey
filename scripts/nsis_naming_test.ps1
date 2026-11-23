$ErrorActionPreference = 'Continue'
$root = if ($env:SUNTEY_REPO) { $env:SUNTEY_REPO } else { Split-Path -Parent $PSScriptRoot }
$setup = if ($env:SUNTEY_SETUP) { $env:SUNTEY_SETUP } else { Join-Path $root "app\src-tauri\target\release\bundle\nsis\Sun_Tey_0.1.0_x64-setup.exe" }
$dir = "$env:LOCALAPPDATA\Sun_Tey"

# 1) install (silent, per-user)
$p = Start-Process -FilePath $setup -ArgumentList '/S' -Wait -PassThru
"INSTALL_EXIT=" + $p.ExitCode
Get-ChildItem $dir | ForEach-Object { "  " + $_.Name + " " + $_.Length }

# 2) smoke-run installed Sun_Tey.exe
$exe = Join-Path $dir "Sun_Tey.exe"
if (-not (Test-Path $exe)) { "SMOKE_FAIL no Sun_Tey.exe in install dir"; exit 1 }
$proc = Start-Process -FilePath $exe -PassThru
Start-Sleep -Seconds 9
$code = (curl.exe -s -o NUL -w "%{http_code}" http://127.0.0.1:17332/api/v1/tasks)
"HEALTH_CODE=" + $code
Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
Get-Process suntey-core -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

# 2b) db placement: must be at INSTDIR root, not inside binaries/
"DB_AT_ROOT=" + (Test-Path (Join-Path $dir "suntey.db"))
"DB_IN_BINARIES=" + (Test-Path (Join-Path $dir "binaries\suntey.db"))

# 3) uninstall (silent)
$un = Join-Path $dir "uninstall.exe"
$u = Start-Process -FilePath $un -ArgumentList '/S' -Wait -PassThru
"UNINSTALL_EXIT=" + $u.ExitCode
Start-Sleep -Seconds 3
"DIR_EXISTS_AFTER=" + (Test-Path $dir)
if (Test-Path $dir) { Get-ChildItem $dir -Recurse | ForEach-Object { "  LEFT: " + $_.FullName.Substring($dir.Length) } }

# 4) cleanup leftovers (user data dir EBWebView etc. intentionally kept = user data)
if (Test-Path $dir) { Remove-Item $dir -Recurse -Force -ErrorAction SilentlyContinue; "FORCE_CLEANED" }
"REMAINING=" + (Test-Path $dir)
