<#
Single-command installer/upgrader untuk node-agent di Windows.
Usage (PowerShell biasa, bukan admin):
  $env:NODE_AGENT_TOKEN = "<token-yang-sama-dengan-VPS>"
  .\scripts\install-windows.ps1
Optional overrides:
  $env:NODE_AGENT_SERVER = "http://100.64.0.1:8788"  (default)
  $env:NODE_AGENT_ID     = "windows"                     (default)
#>

$ErrorActionPreference = "Stop"

$Server = if ($env:NODE_AGENT_SERVER) { $env:NODE_AGENT_SERVER } else { "http://100.64.0.1:8788" }
if (-not $env:NODE_AGENT_TOKEN) {
    Write-Error "Set `$env:NODE_AGENT_TOKEN dulu sebelum run (harus sama dengan token di VPS)."
    exit 1
}
$Token  = $env:NODE_AGENT_TOKEN
$NodeId = if ($env:NODE_AGENT_ID) { $env:NODE_AGENT_ID } else { "windows" }

$InstallDir  = Join-Path $env:USERPROFILE ".hermes\bin"
$LogDir      = Join-Path $env:USERPROFILE ".hermes"
$TaskName    = "NodeAgent"
$ExePath     = Join-Path $InstallDir "node-agent.exe"
$WrapperPath = Join-Path $InstallDir "node-agent-supervisor.ps1"

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Write-Host "==> Menarik binary node-agent dari $Server (via tailscale)"
$headers = @{ "X-Node-Agent-Token" = $Token }
Invoke-WebRequest -Uri "$Server/dl/windows" -Headers $headers -OutFile "$ExePath.new" -UseBasicParsing
# exe lock saat masih jalan — kill dulu, supervisor loop restart pakai binary baru
Get-Process node-agent -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
Move-Item -Force "$ExePath.new" $ExePath

Write-Host "==> Menyimpan environment variable (persist antar sesi login)"
[System.Environment]::SetEnvironmentVariable("NODE_AGENT_SERVER", $Server, "User")
[System.Environment]::SetEnvironmentVariable("NODE_AGENT_TOKEN", $Token, "User")
[System.Environment]::SetEnvironmentVariable("NODE_AGENT_ID", $NodeId, "User")

Write-Host "==> Menulis supervisor script (auto-restart kalau exe crash — setara launchd KeepAlive)"
@"
`$env:HOME = `$env:USERPROFILE
`$env:NODE_AGENT_SERVER = "$Server"
`$env:NODE_AGENT_TOKEN  = "$Token"
`$env:NODE_AGENT_ID     = "$NodeId"
while (`$true) {
    & "$ExePath" *>> "$LogDir\node-agent.log"
    Start-Sleep -Seconds 3
}
"@ | Set-Content -Path $WrapperPath -Encoding UTF8

Write-Host "==> Mendaftarkan Scheduled Task '$TaskName' (trigger: saat logon, restart on-crash via loop)"
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { schtasks /Delete /TN $TaskName /F | Out-Null }
$action = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$WrapperPath`""
schtasks /Create /TN $TaskName /TR $action /SC ONLOGON /RL HIGHEST /F | Out-Null

Write-Host "==> Menjalankan sekarang (nggak perlu logoff/logon dulu)"
schtasks /Run /TN $TaskName | Out-Null

Start-Sleep -Seconds 3
Write-Host "==> Verifikasi registrasi..."
try {
    $resp = Invoke-RestMethod -Uri "$Server/api/nodes" -Headers $headers
    if ($resp | Where-Object { $_.NodeID -eq $NodeId }) {
        Write-Host "OK — node '$NodeId' terdaftar di server."
    } else {
        Write-Host "Belum kelihatan terdaftar — cek log: $LogDir\node-agent.log"
    }
} catch {
    Write-Host "Gagal cek /api/nodes: $_"
}

Write-Host "Selesai. Log: $LogDir\node-agent.log"
Write-Host "Uninstall: schtasks /Delete /TN $TaskName /F"
