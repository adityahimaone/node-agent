# Test case: remote DSH three-loop continuation (node-agent transport)

Regression test for `dsh_session_conflict` on remote macOS workspaces.

## Root cause

`dsh web` (long-lived daemon) holds a `flock(2)` on `session.lock` for every
session it opens, for the entire life of its write handle. The lock has **no
expiry by design** (`dsh-session-persistence-jsonl`). A headless CLI
continuation of the same session therefore always fails with
`already owned by an active write handle`. No retry budget can win — the holder
is a daemon that never exits.

Observed: PID 198 (`dsh web`, port 3080) held **18** `session.lock` handles,
including the exact session the kanban card was continuing.

Retry tuning and per-session serialization were both insufficient.

## Fix (node-agent only)

1. `dshIsolatedHome()` — agent-dispatched runs use `~/.dsh-nodeagent`, with
   user DSH config (`profiles`, `settings.yaml`, `.credentials.yaml`,
   `.anonymous-user-id`) symlinked in so credentials stay in sync. Sessions,
   storages and locks are private. Override: `NODE_AGENT_DSH_HOME`.
2. `dshHome()` / `dshWorkspaceRegistryPath()` — all DSH on-disk state resolves
   through the isolated home, never hardcoded `~/.dsh`.
3. `adoptLegacyDSHSession()` — a session created before isolation is lazily
   **copied** into the isolated home on first continuation. The copy gets a new
   lock inode, so the daemon's handle on the original is irrelevant. Existing
   cards keep working; no cold start.

## Preconditions

- Mac worker runs the isolated-home binary (sha256 in step 0).
- The legacy session exists under `~/.dsh/sessions/<workspace>/`.
- The card carries `executor=dsh`, `workspace_transport=node-agent`,
  `workspace_ssh_target=mac-tailscale`, and a non-empty persisted
  `dsh_session_id` (i.e. this is a continuation, not an initial run).
- `dsh web` is left running — that is the point of the test.

## Steps

Loop 1 = unblock (or first dispatch), loops 2 and 3 = requeue a continuation.

```sh
# 0. binary identity
shasum -a 256 ~/.hermes/bin/node-agent          # on mac-tailscale

# Loop 1
hermes kanban --board default unblock t_02ccffe9 --reason "loop 1 of 3"

# Loop 2 (review -> ready, continuation)
hermes kanban --board default reopen-review t_02ccffe9 --reason "@default loop 2 of 3"

# Loop 3
hermes kanban --board default reopen-review t_02ccffe9 --reason "@default loop 3 of 3"
```

Wait for each run to reach `review` before issuing the next requeue.
Poll: `select status from tasks where id='<card>'`.

## Pass criteria

| check | expected |
|---|---|
| loops that reach `review` | 3 / 3 |
| `completed` events with `"error":""` | 3 |
| `dsh_session_conflict` / `write handle` occurrences after fix | **0** |
| `dsh_session_id` at end | identical to the value before loop 1 |
| session id reported by each run | all three identical |
| `consecutive_failures` | 0 |
| `last_failure_error` | empty |
| isolated-home lock inode | differs from legacy lock inode |
| legacy lock still held by `dsh web` | yes (proves isolation, not luck) |

## Actual result

Card `t_02ccffe9` ("Pull Bisadaya"), board `default`, workspace
`/Users/adityahimawan/Development/bisadaya-monorepo`.

```
loop 1  remote_dispatched 1790218219 -> completed 1790218365  error=""
loop 2  remote_dispatched 1790218457 -> completed 1790218566  error=""
loop 3  remote_dispatched 1790218627 -> completed 1790218735  error=""
```

- session reported by all three runs: `session-d902d9c9-bfe3-453f-a223-2dc2c6b498fd`
- same as the pre-fix `dsh_session_id` → continuation preserved, no cold start
- `write handle` occurrences after the fix: **0** (19 before)
- `consecutive_failures=0`, `last_failure_error=''`
- Mac: isolated `session.lock` inode `64937701` vs legacy `64820218`;
  `dsh web` PID 198 still held 18 legacy locks throughout
- `workspace_transport=node-agent` (not downgraded to `ssh`)

## Four-loop result (card `t_54e3ba28`)

Second run against a fresh card, four loops, same isolated-home binary.

Board `f8-bisadaya`, workspace
`/Users/adityahimawan/Development/bisadaya-monorepo`, `executor=dsh`,
`workspace_transport=node-agent` (auto-routed), `created_by=user`.

```
run 1  --profile headless --json
       remote_dispatched 1790219772 -> completed 1790219788  error=""
run 2  ... --session-id session-a456789d-3f60-4534-a479-1d735ba85700
       remote_dispatched 1790219819 -> completed 1790219835  error=""
run 3  ... --session-id session-a456789d-...
       remote_dispatched 1790219865 -> completed 1790219897  error=""
run 4  ... --session-id session-a456789d-...
       remote_dispatched 1790219928 -> completed 1790219939  error=""
```

- one session for all four runs: `session-a456789d-3f60-4534-a479-1d735ba85700`
- run 1 omits `--session-id` (initial); runs 2-4 pass it (continuations)
- `write handle` / `dsh_session_conflict` events: **0**
- `consecutive_failures=0`, `last_failure_error=''`, status `review`
- Mac: isolated transcript grew to 64,376 bytes; session absent from legacy
  `~/.dsh` (agent-only, as intended)

Creating the card: the CLI has no `--executor` flag and the board API needs a
session cookie, so create through the board's own production `CreateTask` and
let it derive transport from the workspace path. Do not hand-write the row.

## Notes

- One transient `409: executor unavailable on node: dsh` fires between the Mac
  worker restart and its first heartbeat registration. It clears on the next
  dispatch tick and is not a lock failure. Allow one retry after a worker
  restart before judging a run.
- The board-UI comment path reopens `review`/`blocked` cards by setting
  `status='todo'` (`internal/kanban/comments.go`). The CLI equivalent is
  `hermes kanban reopen-review`.
