#!/bin/bash
# One-command node-agent updater for macOS.
#
# Reads the token and the server URL from the existing
# LaunchAgent config, so an update never asks for the
# token again. Usage:
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

BIN="$HOME/.hermes/bin/node-agent"
mkdir -p "$(dirname "$BIN")"
echo "==> Fetching node-agent from $SERVER"
curl -fsSL -H "X-Node-Agent-Token: $TOKEN" "$SERVER/dl/mac" -o "$BIN.new"
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
