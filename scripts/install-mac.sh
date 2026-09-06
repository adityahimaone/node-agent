#!/usr/bin/env bash
# Single-command installer/upgrader untuk node-agent di Mac.
# Usage:
#   NODE_AGENT_TOKEN=<token> ./scripts/install-mac.sh
# Optional overrides:
#   NODE_AGENT_SERVER=http://100.80.220.71:8788  (default, tailscale IP VPS)
#   NODE_AGENT_ID=mac                             (default)
set -euo pipefail

NODE_AGENT_SERVER="${NODE_AGENT_SERVER:-http://100.80.220.71:8788}"
NODE_AGENT_TOKEN="${NODE_AGENT_TOKEN:?set NODE_AGENT_TOKEN before running (harus sama dengan token di VPS)}"
NODE_AGENT_ID="${NODE_AGENT_ID:-mac}"
INSTALL_DIR="$HOME/.hermes/bin"
PLIST_LABEL="com.adit.node-agent"
PLIST_PATH="$HOME/Library/LaunchAgents/${PLIST_LABEL}.plist"
LOG_DIR="$HOME/.hermes"

mkdir -p "$INSTALL_DIR" "$LOG_DIR"

echo "==> Menarik binary node-agent dari $NODE_AGENT_SERVER (via tailscale)"
curl -fsSL -H "X-Node-Agent-Token: $NODE_AGENT_TOKEN" \
  "$NODE_AGENT_SERVER/dl/mac" -o "$INSTALL_DIR/node-agent.new"
chmod +x "$INSTALL_DIR/node-agent.new"
mv "$INSTALL_DIR/node-agent.new" "$INSTALL_DIR/node-agent"

echo "==> Menulis launchd plist di $PLIST_PATH"
cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${PLIST_LABEL}</string>
  <key>ProgramArguments</key>
  <array><string>${INSTALL_DIR}/node-agent</string></array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>NODE_AGENT_SERVER</key><string>${NODE_AGENT_SERVER}</string>
    <key>NODE_AGENT_TOKEN</key><string>${NODE_AGENT_TOKEN}</string>
    <key>NODE_AGENT_ID</key><string>${NODE_AGENT_ID}</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>${LOG_DIR}/node-agent.out.log</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/node-agent.err.log</string>
</dict>
</plist>
PLIST

echo "==> (Re)loading launchd service"
launchctl unload "$PLIST_PATH" 2>/dev/null || true
launchctl load "$PLIST_PATH"

echo "==> Menunggu registrasi..."
sleep 2
if curl -fsS -H "X-Node-Agent-Token: $NODE_AGENT_TOKEN" "$NODE_AGENT_SERVER/api/nodes" 2>/dev/null | grep -q "\"$NODE_AGENT_ID\""; then
  echo "OK — node '$NODE_AGENT_ID' terdaftar di server."
else
  echo "Belum kelihatan terdaftar — cek log: $LOG_DIR/node-agent.err.log"
fi

echo "Selesai. Logs: $LOG_DIR/node-agent.out.log (stdout) / node-agent.err.log (stderr)"
echo "Uninstall: launchctl unload $PLIST_PATH && rm $PLIST_PATH"
