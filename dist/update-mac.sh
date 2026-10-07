#!/bin/bash
# One-command node-agent updater for macOS.
#
# Reads the token and the server URL from the existing
# LaunchAgent config, so an update never asks for the
# token again. The binary comes from the node-agent server
# first; when that is unreachable it falls back to a
# release URL (GitHub) — NODE_AGENT_RELEASE_URL on the
# worker, or NODE_AGENT_GITHUB_RELEASE baked in by the
# server. Usage:
#   curl -fsSL http://<vps>:8788/update/mac | bash
set -euo pipefail

PLIST="$HOME/Library/LaunchAgents/com.adit.node-agent.plist"
plist_value() {
  /usr/libexec/PlistBuddy -c "Print :EnvironmentVariables:$1" "$PLIST" 2>/dev/null
}

TOKEN=$(plist_value NODE_AGENT_TOKEN)
SERVER=$(plist_value NODE_AGENT_SERVER)
if [[ -z "$TOKEN" || -z "$SERVER" ]]; then
  echo "node-agent is not installed (no LaunchAgent config at $PLIST) — run install-mac.sh first" >&2
  exit 1
fi

# Fallback download base (e.g. a GitHub release download
# directory). The worker's NODE_AGENT_RELEASE_URL wins;
# otherwise the server-baked default applies. A leftover
# placeholder means no fallback is configured.
RELEASE_BASE="${NODE_AGENT_RELEASE_URL:-__NODE_AGENT_RELEASE_URL__}"
if [[ "$RELEASE_BASE" == __* ]]; then RELEASE_BASE=""; fi

BIN="$HOME/.hermes/bin/node-agent"
mkdir -p "$(dirname "$BIN")"
echo "==> Fetching node-agent"
if ! curl -fsSL -H "X-Node-Agent-Token: $TOKEN" "$SERVER/dl/mac" -o "$BIN.new"; then
  echo "    server unreachable — trying the release URL" >&2
  if [[ -z "$RELEASE_BASE" ]] || ! curl -fsSL "${RELEASE_BASE%/}/node-agent-darwin-arm64" -o "$BIN.new"; then
    rm -f "$BIN.new"
    echo "update failed: neither the server nor the release URL is reachable" >&2
    exit 1
  fi
fi
chmod +x "$BIN.new"
# Rename over the running binary: the running process keeps
# its inode, so this is safe, and the kickstart below
# launches the new file.
mv -f "$BIN.new" "$BIN"
if ! launchctl kickstart -k "gui/$(id -u)/com.adit.node-agent" 2>/dev/null; then
  # Agent not loaded (fresh boot before first login) — load it.
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load "$PLIST"
fi
echo "==> node-agent updated and restarted"
