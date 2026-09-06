#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/node-agent-server"
DIST="$ROOT/dist"
PID="$HOME/.hermes/node-agent.pid"
LOG="$HOME/.hermes/node-agent.log"
ENV_FILE="$HOME/.hermes/node-agent.env"
mkdir -p "$(dirname "$PID")" "$DIST"
# Persisted token (chmod 600) so restarts keep auth without shell exports
[[ -f "$ENV_FILE" ]] && . "$ENV_FILE"
case "${1:-}" in
  build) GOMAXPROCS=1 GOGC=20 go build -o "$BIN" ./cmd/server && echo "built $BIN";;
  build-mac) GOOS=darwin GOARCH=arm64 GOMAXPROCS=1 GOGC=20 go build -o "$DIST/node-agent-darwin-arm64" ./cmd/agent && echo "built $DIST/node-agent-darwin-arm64";;
  build-windows) GOOS=windows GOARCH=amd64 GOMAXPROCS=1 GOGC=20 go build -o "$DIST/node-agent-windows-amd64.exe" ./cmd/agent && echo "built $DIST/node-agent-windows-amd64.exe";;
  start) [[ -x "$BIN" ]] || { echo "binary missing: $BIN, run $0 build"; exit 1; }
         if [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null; then echo "already running PID=$(cat "$PID")"; exit 0; fi
         : "${NODE_AGENT_TOKEN:?set NODE_AGENT_TOKEN in $ENV_FILE (openssl rand -hex 32). Pass NODE_AGENT_TOKEN= explicitly to run without auth.}"
         NODE_AGENT_DIST_DIR="$DIST" nohup env NODE_AGENT_TOKEN="$NODE_AGENT_TOKEN" "$BIN" >>"$LOG" 2>&1 & echo $! > "$PID"; echo "started PID=$(cat "$PID") log=$LOG (auth=$([[ -n "$NODE_AGENT_TOKEN" ]] && echo on || echo OFF))";;
  stop)  if [[ -f "$PID" ]]; then kill "$(cat "$PID")" 2>/dev/null || true; rm -f "$PID"; echo "stopped"; else echo "not running"; fi;;
  restart) "$0" stop; sleep 1; "$0" start;;
  status) if [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null; then echo "running PID=$(cat "$PID")"; curl -s -H "X-Node-Agent-Token: ${NODE_AGENT_TOKEN:-}" http://127.0.0.1:8788/health | python3 -m json.tool | head -30; else echo "stopped"; fi;;
  logs) tail -n 100 "$LOG" 2>&1 | head -100;;
  *) echo "Usage: $0 {build|build-mac|build-windows|start|stop|restart|status|logs}"; exit 2;;
esac
