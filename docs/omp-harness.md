# omp Executor — Worker Contract

> What the node-agent worker must do to run the `omp` executor
> ([oh-my-pi](https://omp.sh)) for Switchyard, and how to verify it.

Control-plane counterpart: [kanban-board docs/features/omp-executor.md](https://github.com/adityahimaone/switchyard/blob/master/docs/features/omp-executor.md).
Compare with the CommandCode contract in `README.md` and the DSH contract in
[dsh-harness.md](dsh-harness.md).

---

## 1. Invocation

```sh
# first run — no --resume, so omp mints a real session
omp -p --auto-approve --mode json "<prompt>"

# continuation — resume the exact bound session
omp -p --auto-approve --mode json --resume <session-id> "<prompt>"
```

| Flag | Why |
|---|---|
| `-p` | Headless one-shot. Without it the CLI opens a TUI and blocks. |
| `--auto-approve` | A headless worker cannot answer an interactive approval prompt, so tool calls would hang. It permits file edits and shell commands — trusted nodes only. |
| `--mode json` | Emits NDJSON so the worker can read the session id out of the stream. |
| `--resume <id>` | Continuation only. omp matches by session-id prefix, so a prefix is enough. |

The prompt is appended as the final positional argument (`ompArgs` in
`cmd/agent/main.go`). The binary is resolved by `ompBin()` → `findBin("omp")`,
which checks `$HOME/.local/bin`, `/opt/homebrew/bin`, `/usr/local/bin`, then
`exec.LookPath`. Windows resolves `omp.exe` through the same path.

## 2. Session identity

omp reports its session id on whichever NDJSON frame resolves it, not on a single
dedicated terminal frame. `parseOmpSessionID` therefore scans every JSON line and
accepts a `sessionId` at the top level **or** nested under `data`:

```json
{"type":"session_info_update","sessionId":"01JZ8Y2A..."}
{"id":"req_1","type":"response","command":"open_session","success":true,"data":{"resumed":true,"sessionId":"01JZ8Y2A..."}}
```

The parser is deliberately tolerant. A future omp release that reshapes its
frames must degrade to "unknown session" — which fails loudly — rather than
crash or silently bind a wrong id. Non-JSON lines (the `provenance` header and
`EXECUTOR_PROOF` trailer the worker adds) are skipped, and a truncated trailing
line from a partial stream is ignored.

### Rules

1. When `session_continuation` is false, the worker sends an empty session id and
   omp starts and saves a new session.
2. When `session_continuation` is true, the worker **must** resume the dispatched
   session. If `omp_session_id` is empty the run is refused with
   `omp_session_missing: continuation requested but no session id was dispatched`
   rather than silently downgraded to a cold start — a cold start would re-apply
   the same feedback to files the previous turn already edited.
3. The resolved session id is written to the provenance line as
   `omp_session_id=<id>`, returned in `ResultRequest.OMPSessionID`, and the
   control plane rejects a session that differs from the dispatched one
   (`omp_identity_rejected`, card goes to `blocked`).
4. A **failed** run may legitimately return no session id; that is accepted. Only
   a success must prove identity, which is why the worker fails the run with
   `omp_session_missing: headless --mode json returned no session id` when a
   success produced none.
5. omp emits no turn sequence, so `LastTurnSeq` stays nil and the control plane
   relies on its `current_run_id` ownership fence. Do not synthesize one.

## 3. Provenance

Every result is prefixed with:

```
provenance executor=omp requested=omp bin=/Users/x/.local/bin/omp args=["-p" "--auto-approve" "--mode" "json" "--resume" "01JZ..."] ws=/Users/x/Development/app omp_session_id=01JZ...
```

and suffixed with `EXECUTOR_PROOF=omp` plus a repeated provenance line. The
control plane parses `omp_session_id` from this line when the structured field is
absent.

## 4. Install

The worker probes the binary; it never installs it. Both host types had it
installed on 2026-09-28.

```sh
# macOS / Linux — installs to ~/.local/bin/omp
curl -fsSL https://omp.sh/install | sh
```

```powershell
# Windows — installs to %LOCALAPPDATA%\omp\omp.exe and adds it to the user PATH
irm https://omp.sh/install.ps1 | iex
```

Alpine/musl hosts additionally need `apk add libstdc++ libgcc`, because the
prebuilt musl binary links them dynamically.

### The `bun` version trap

Both installers prefer a source install through `bun` and abort when `bun` is
older than **1.3.13 → 1.3.14**. A host with an older `bun` — the Windows box
here ships 1.3.13 bundled with Kiro-Cli — reports
`Bun 1.3.14 or newer is required. Current version: 1.3.13` and installs
nothing. Force the prebuilt binary instead:

```powershell
& ([scriptblock]::Create((irm https://omp.sh/install.ps1))) -Binary
```

```sh
curl -fsSL https://omp.sh/install | sh -s -- --binary
```

After installing, restart the worker so `detectExecutors` re-registers the node
with `omp` in `executors` and `versions.omp` in the register frame.

## 5. Credentials

omp needs a provider on the host. Without one it exits early — after the
executor was already resolved and the process spawned — and prints:

```
No models available. Use /login or set an API key environment variable.
```

omp resolves credentials from its **own** store, not from the node-agent
environment, so a worker with working `dsh` and `codex` executors can still
have none usable for omp. Verified on the Mac: `~/Library/LaunchAgents/com.adit.node-agent.plist`
carries only `NODE_AGENT_*` variables, with no provider key.

Set it up once as the service user:

```sh
omp setup            # interactive onboarding
```

or export a supported provider key (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GEMINI_API_KEY`, …) into the service environment, then restart the worker:

```sh
# launchd: add the key to EnvironmentVariables in
# ~/Library/LaunchAgents/com.adit.node-agent.plist, then
launchctl unload ~/Library/LaunchAgents/com.adit.node-agent.plist
launchctl load   ~/Library/LaunchAgents/com.adit.node-agent.plist
```

Note the worker does **not** pass `--model`, so the board-level `model` field is
ignored for this executor and omp resolves the model from the host's own config
and `/model` assignment.

## 6. Verification

```sh
# 1. binary present and runnable as the service user
omp --version                       # e.g. omp/18.4.0

# 2. worker advertises the capability (register frame, not the 15s heartbeat)
curl -s -H "X-Node-Agent-Token: $NODE_AGENT_TOKEN" localhost:8788/health
#    expect "omp" in executors and versions.omp present

# 3. end-to-end: a task with executor=omp should produce a provenance line
#    carrying omp_session_id, then land in review (never straight to done)
```

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `omp_unavailable: omp not installed or not on PATH` | Binary missing for the service user | Install it, then restart the worker |
| Run exits with `No models available` | No provider credential on the host | `omp setup` as the service user, or set a provider API key in the worker env |
| `omp_session_missing: continuation requested but no session id was dispatched` | Worker predates the `omp_session_id` wire field | Deploy a current worker to every host |
| `omp_session_missing: headless --mode json returned no session id` | Run succeeded but emitted no session frame | Check the worker log; confirm `--mode json` reached the CLI |
| `omp_identity_rejected:` from the control plane | Worker returned a different session than dispatched | Inspect the run log for the `--resume` id actually used |
| Nothing appears in `versions` | Worker not restarted after install | Restart node-agent so capabilities re-register |

Worker log: `$TMPDIR/node-agent-<task_id>/run.log`.

## 8. Tests

`cmd/agent/omp_test.go` covers the contract without spawning a real binary:

- first run omits `--resume` and always requests `--mode json` / `--auto-approve`
- continuation resumes the bound (whitespace-trimmed) session
- the parser reads a top-level `sessionId` and a nested `data.sessionId`
- the parser ignores provenance/proof lines and a truncated tail
- the result carries the session id and leaves `LastTurnSeq` nil

```sh
go test ./cmd/agent -run OMP
```
