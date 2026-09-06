#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/node-agent-server"
PID="$HOME/.hermes/node-agent.pid"
LOG="$HOME/.hermes/node-agent.log"
mkdir -p "$(dirname "$PID")"
case "${1:-}" in
  build) GOMAXPROCS=1 GOGC=20 go build -o "$BIN" ./cmd/server && echo "built $BIN";;
  start) [[ -x "$BIN" ]] || { echo "binary missing: $BIN, run $0 build"; exit 1; }
         if [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null; then echo "already running PID=$(cat "$PID")"; exit 0; fi
         nohup "$BIN" >>"$LOG" 2>&1 & echo $! > "$PID"; echo "started PID=$(cat "$PID") log=$LOG";;
  stop)  if [[ -f "$PID" ]]; then kill "$(cat "$PID")" 2>/dev/null || true; rm -f "$PID"; echo "stopped"; else echo "not running"; fi;;
  restart) "$0" stop; sleep 1; "$0" start;;
  status) if [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null; then echo "running PID=$(cat "$PID")"; curl -s http://127.0.0.1:8788/health | python3 -m json.tool | head -30; else echo "stopped"; fi;;
  logs) tail -n 100 "$LOG" 2>&1 | head -100;;
  *) echo "Usage: $0 {build|start|stop|restart|status|logs}"; exit 2;;
esac
