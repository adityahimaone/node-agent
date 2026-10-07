# One-command node-agent updater for Windows.
#
# Reads the token and the server URL from the persisted
# User environment variables, so an update never asks for
# the token again. Usage:
#   powershell -NoProfile -Command "iex (irm http://<vps>:8788/update/windows)"
$ErrorActionPreference = "Stop"

$Token  = [System.Environment]::GetEnvironmentVariable("NODE_AGENT_TOKEN", "User")
$Server = [System.Environment]::GetEnvironmentVariable("NODE_AGENT_SERVER", "User")
if (-not $Token -or -not $Server) {
    Write-Error "node-agent is not installed (no NODE_AGENT_* user environment variables) — run install-windows.ps1 first"
    exit 1
}

$Dir = Join-Path $env:USERPROFILE ".hermes\bin"
Write-Host "==> Fetching node-agent from $Server"
Invoke-WebRequest -Uri "$Server/dl/windows" -Headers @{ "X-Node-Agent-Token" = $Token } -OutFile "$Dir\node-agent.exe.new" -UseBasicParsing
# The exe is locked while running — kill first; the
# supervisor loop (~/.hermes/bin/node-agent-supervisor.ps1)
# restarts the new binary within a few seconds.
Get-Process node-agent -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
Move-Item -Force "$Dir\node-agent.exe.new" "$Dir\node-agent.exe"
Write-Host "==> node-agent updated; supervisor restarts it within a few seconds"
