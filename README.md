# node-agent — Otak di VPS, tangan di Mac/Windows

Persistent transport for Hermes. Orchestrator + kanban stay on VPS, execution runs on local Mac/Windows via Tailscale. Diagram rev3 agentic-flow-adit.

```
VPS (otak)  --HTTP long-poll-->  Mac node-agent  --hermes/codex/shell-->  workspace
    |                                | heartbeat 15s + register
    +-- tailscale tailnet (relay/direct) --+
    +-- SSH fallback (file ops) ------------+
```

Server routes `POST /api/dispatch` by workspace prefix; agent long-polls `GET /api/nodes/{id}/poll`.

## Quick start

VPS:
```sh
cd ~/apps/node-agent
./ctl.sh build
./ctl.sh start   # pm2 node-agent :8788
curl http://127.0.0.1:8788/health
```

Mac (launchd KeepAlive):
```sh
scp ~/apps/node-agent/node-agent adityahimawan@mac:.hermes/bin/node-agent
ssh mac 'launchctl load ~/Library/LaunchAgents/com.adit.node-agent.plist'
```

Dispatch:
```sh
curl -X POST http://127.0.0.1:8788/api/dispatch -H 'Content-Type: application/json' \
  -d '{"task_id":"t1","board":"f8-saas","message":"git status","workspace":"/Users/adityahimawan/Development/saas"}'
curl http://127.0.0.1:8788/api/results/t1
```

## Workspaces

`~/.hermes/workspaces.json` (VPS+Mac synced) binds hermes kanban boards to luvus workspaces:

- `root` → `/Users/adityahimawan/Development`
- `saas` → `/Users/.../saas` (apps: gadjian/app, portal-hadirr, baktiku-portal)
- `bisadaya` → `/Users/.../bisadaya-monorepo`

Helper `ws` CLI (`~/.hermes/bin/ws`):

- `ws list` — ID | STATUS connected/disconnected | ROUTE relay/direct | LUVUS | PATH
- `ws ping [id]` / `ws open <id>` / `ws status --json`

## runJob heuristic

Shell meta (`;|&&`) or CLI prefix `git /ls/cat/echo/pwd/grep/find` → `bash -lc` fast (~50-100ms).
Otherwise `hermes chat -q` → `codex exec` fallback (~25-60s).

## Build

`go vet ./... && GOMAXPROCS=1 GOGC=20 go build -o node-agent-server ./cmd/server` (VPS)
`GOOS=darwin GOARCH=arm64 go build -o node-agent ./cmd/agent` (Mac) — `ctl.sh` wraps it.

## Stack

Go 1.23, chi, HTTP JSON long-poll (no protoc). Tailscale tailnet, SSH alias `mac-tailscale` for file ops. pm2 on VPS, launchd on Mac.

## Manifest (rev3)

hermes-agent, holographic-memory, 9router, luvus, ponytail, caveman, codegraph verified. Skipped: tanvesh01/issue-workflow (empty), gRPC (YAGNI → HTTP).

