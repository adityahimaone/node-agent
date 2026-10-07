# One-command node-agent updater for Windows.
#
# Reads the token and the server URL from the persisted
# User environment variables, so an update never asks for
# the token again. The binary comes from the node-agent
# server first; when that is unreachable it falls back to
# a release URL (GitHub) — the NODE_AGENT_RELEASE_URL user
# environment variable, or NODE_AGENT_GITHUB_RELEASE
# baked in by the server. Usage:
#   powershell -NoProfile -Command "iex (irm http://<vps>:8788/update/windows)"
$ErrorActionPreference = "Stop"

$Token  = [System.Environment]::GetEnvironmentVariable("NODE_AGENT_TOKEN", "User")
$Server = [System.Environment]::GetEnvironmentVariable("NODE_AGENT_SERVER", "User")
if (-not $Token -or -not $Server) {
    Write-Error "node-agent is not installed (no NODE_AGENT_* user environment variables) — run install-windows.ps1 first"
    exit 1
}

# Fallback download base (e.g. a GitHub release download
# directory). A NODE_AGENT_RELEASE_URL user variable wins;
# otherwise the server-baked default applies. A leftover
# placeholder means no fallback is configured.
$ReleaseBase = [System.Environment]::GetEnvironmentVariable("NODE_AGENT_RELEASE_URL", "User")
if ([string]::IsNullOrEmpty($ReleaseBase)) { $ReleaseBase = "__NODE_AGENT_RELEASE_URL__" }
if ($ReleaseBase -like "__*") { $ReleaseBase = "" }

$Dir = Join-Path $env:USERPROFILE ".hermes\bin"
$Destination = "$Dir\node-agent.exe.new"
Write-Host "==> Fetching node-agent"
$Fetched = $false
try {
    Invoke-WebRequest -Uri "$Server/dl/windows" -Headers @{ "X-Node-Agent-Token" = $Token } -OutFile $Destination -UseBasicParsing
    $Fetched = $true
} catch {
    Write-Host "    server unreachable — trying the release URL"
    if ($ReleaseBase) {
        try {
            Invoke-WebRequest -Uri "$($ReleaseBase.TrimEnd('/'))/node-agent-windows-amd64.exe" -OutFile $Destination -UseBasicParsing
            $Fetched = $true
        } catch { }
    }
}
if (-not $Fetched) {
    if (Test-Path $Destination) { Remove-Item $Destination -Force }
    Write-Error "update failed: neither the server nor the release URL is reachable"
    exit 1
}
# The exe is locked while running — kill first; the
# supervisor loop (~/.hermes/bin/node-agent-supervisor.ps1)
# restarts the new binary within a few seconds.
Get-Process node-agent -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
Move-Item -Force $Destination (Join-Path $Dir "node-agent.exe")
Write-Host "==> node-agent updated; supervisor restarts it within a few seconds"
