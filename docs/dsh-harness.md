# DeepSeek Harness (`dsh`) executor

How node-agent runs DeepSeek Harness headless sessions, and the two rules that
keep them reliable: **isolated home** and **stable session identity**.

## Invocation

| Item | Value |
|---|---|
| Binary | `dsh` |
| Args | `--profile headless --json` |
| Continuation args | `--profile headless --json --session-id <id>` |
| Env | `PATH=/opt/homebrew/bin:/usr/local/bin:$PATH` plus `DSH_HOME` |
| Model override | `DSH_MODEL` when the dispatch carries `model` |

First run omits `--session-id`. DSH creates the session and emits a `session`
event; the worker reads `sessionId` and `cwd` from it. Passing a placeholder ID
on the first run fails with
`session "..." does not exist; omit --session-id to start a new Session`.

`dsh` is a Node launcher. launchd starts the worker with a minimal `PATH`, so
every DSH invocation — including the `--version` health check — must use the
Homebrew `PATH`. The worker appends it; do not rely on the service `PATH` alone.

`--json` is required. Without the session event the worker returns
`dsh_session_missing: headless --json returned no session event` rather than
silently reporting success.

## Session identity rules

The control plane binds result identity to the dispatched session. Therefore:

1. **Never clear `dsh_session_id`.** A continuation that cannot resume fails; it
   is not downgraded to a cold run.
2. **Never create a new session to escape a failure.** A new ID is rejected
   downstream as `dsh_identity_rejected`.
3. **Session `cwd` must match the dispatched workspace.** A mismatch fails with
   `dsh_workspace_mismatch`.

### Write-handle conflicts

DSH takes an OS `flock(2)` write handle on `session.lock` for the life of a
session owner, and the installed build never expires or renews the lease. Any
other process holding that handle blocks the run:

```text
dsh_session_conflict: existing session is owned by an active write handle
```

The worker retries the **same** session (default 30 attempts, 1 s apart) and
never clears the ID. Tuning retries does not fix the underlying cause — see
below.

## Isolated home

`dsh web` resolves its home through `resolveDshHome()` → `$HOME/.dsh` unless
`DSH_HOME` is set, and holds its own locks there for its whole process lifetime.
Two consequences drove the design:

- agent runs sharing `~/.dsh` contend with the daemon's non-expiring locks;
- agent runs isolated from `~/.dsh` are invisible in the `dsh web` UI.

The worker resolves a private home and points DSH at it:

| Home | Used by |
|---|---|
| `~/.dsh-nodeagent` | agent-dispatched headless runs (default isolated home) |
| `~/.dsh` | `dsh web`, interactive use |

`dshIsolatedHome()` creates the directory mode `0700` and symlinks
`profiles`, `settings.yaml`, `.credentials.yaml`, and `.anonymous-user-id` from
`~/.dsh`, so credentials and profiles stay shared while session state does not.
All on-disk DSH state resolves through `dshHome()` /
`dshWorkspaceRegistryPath()` / `dshSessionDirName()` — never a hardcoded
`~/.dsh`.

If the isolated home cannot be created the worker logs the failure and falls
back to the shared home rather than failing the dispatch.

### Legacy session adoption

Cards created before the isolated home existed point at sessions under
`~/.dsh`. On a continuation the worker **copies** such a session into the
isolated home (`adoptLegacyDSHSession`) before invoking DSH. The copy is
deliberate: copying gives the isolated home an independent lock inode, whereas
symlinking would hand the daemon's `flock(2)` straight back. Adoption never
overwrites an existing isolated copy.

### Publishing back for UI visibility

Mirroring the transcript is **not** enough for the web UI. `dsh web` enumerates
sessions from `$DSH_HOME/storages/workspace.json`
(`tables.workspaces[*].sessionIds`), not from the sessions directory. A session
with a valid transcript but no registry entry stays invisible.

After a successful run the worker copies the session directory into `~/.dsh`
**and** registers it in the legacy workspace registry
(`registerDSHWorkspaceInRegistry`). Three rules keep this safe:

- **Never copy `session.lock`.** Sharing that inode hands the daemon's
  non-expiring lock back to the agent and reintroduces the original conflict.
  The destination lock file is removed after copying.
- **Compare the newest file mtime, not the directory mtime.** Continuations
  append to the same transcript in place, leaving the parent directory mtime
  unchanged; a directory-mtime check makes the first publish look permanently
  current and later loops never refresh.
- **Never overwrite a strictly newer published transcript.** The daemon may
  have advanced the session in the web UI.

Session directory naming follows DSH: the canonical cwd with separators
flattened and fenced by `--`, e.g.
`/Users/x/Development/blog` → `--Users-x-Development-blog--`.

## Workspace ID

`dshWorkspaceID(workspacePath)` looks the canonical workspace up in the isolated
home's workspace registry and returns its ID, or `""` when the workspace is not
registered yet. Before a dsh run spawns, the worker calls
`ensureDSHWorkspace(workspacePath)`, which **creates the durable workspace
record (canonical path, title, empty session list) when absent and reuses any
existing record — including one the `dsh web` GUI created — by canonical path**.
This pre-create is what makes a brand-new execution land in the workspace
(grouped) on its very first run instead of appearing under Ungrouped until a
continuation attaches it. Same-canonical-path runs therefore keep one workspace
id across the isolated and legacy homes, so GUI-created workspaces like
`bisadaya-monorepo` receive agent sessions without duplicates. The ID is
returned in the result so the control plane can confirm the run touched the
intended workspace.

## Result provenance

Every DSH result starts with a provenance header and ends with an executor
proof:

```text
provenance executor=dsh requested=dsh bin=/opt/homebrew/bin/dsh args=["--profile" "headless" "--json"] ws=/Users/x/Development/blog dsh_session_id=session-... dsh_session_cwd=/Users/x/Development/blog
EXECUTOR_PROOF=dsh
```

The worker does not commit or push. Results land in `review`.

## Configuration

| Variable | Default | Effect |
|---|---|---|
| `NODE_AGENT_DSH_HOME` | `$HOME/.dsh-nodeagent` | Isolated home for agent runs |
| `NODE_AGENT_DSH_WORKSPACE_REGISTRATION` | enabled | `0` skips workspace registry registration |
| `NODE_AGENT_DSH_PUBLISH` | enabled | `0` skips mirroring sessions into `~/.dsh` |
| `NODE_AGENT_DSH_WEB_URL` | `http://127.0.0.1:3080/` | Probe used to tolerate a slow `--version` |
| `NODE_AGENT_DSH_CONFLICT_RETRIES` | `30` | Write-handle conflict retries |
| `NODE_AGENT_DSH_CONFLICT_DELAY_MS` | `1000` | Delay between conflict retries |

An empty `NODE_AGENT_DSH_HOME` value falls back to the default isolated home;
set it to an explicit path to relocate, not to disable isolation.

## Verification

```sh
# Session must advance in the isolated home.
W=--Users-x-Development-blog--; S=session-<id>
stat -f '%z bytes %Sm' ~/.dsh-nodeagent/sessions/$W/$S/session.v3.jsonl.zstd

# Published copy must match the isolated transcript byte for byte.
stat -f '%z' ~/.dsh/sessions/$W/$S/session.v3.jsonl.zstd

# The published session must be listed by the UI registry.
python3 -c "import json;d=json.load(open('$HOME/.dsh/storages/workspace.json'));print([len(w['sessionIds']) for w in d['tables']['workspaces'].values()])"

# The two homes must not share a lock inode.
stat -f '%i' ~/.dsh-nodeagent/sessions/$W/$S/session.lock
```

A multi-loop continuity test with real cards is recorded in
[test-case-dsh-isolated-home-three-loop.md](test-case-dsh-isolated-home-three-loop.md).

## Troubleshooting

### `dsh_unavailable: DeepSeek Harness (dsh) not installed or not on PATH`

Install `@deepseek-ai/dsh` and restart the worker so capabilities refresh. Run
`dsh --version` under the same minimal environment launchd uses.

### `binary health check timed out`

DSH may boot its plugin tree even for `--version`. If the web profile answers at
`NODE_AGENT_DSH_WEB_URL` the worker proceeds and lets the bounded headless run
decide; otherwise the check fails.

### `dsh_session_conflict`

Something else owns the session's write handle — normally `dsh web` on the same
home. Confirm the agent is using the isolated home
(`lsof -p <pid> | grep session.lock`), not that retries need tuning.

### Sessions complete but are missing from `dsh web`

Check that `NODE_AGENT_DSH_PUBLISH` is not `0` and that the worker log shows
`dsh session publish: mirrored session ...`. A mirrored transcript without a
registry entry still does not appear.

### `dsh_workspace_mismatch`

DSH resumed a session whose `cwd` differs from the dispatched workspace. Fix the
card's workspace path rather than retrying; the session belongs to another
repository.
