# Claude Code CLI (`claude`) executor

The node-agent runs Anthropic's Claude Code CLI in non-interactive headless mode to execute Kanban tasks. Claude is an **explicit-only** executor: it is never selected by `auto`, and it requires a registered, trusted worker host.

## Invocation

| Item | Value |
|---|---|
| Binary | `claude` |
| Args (first run) | `-p`, `--output-format json`, `--permission-mode bypassPermissions` |
| Args (continuation) | `-p`, `--output-format json`, `--permission-mode bypassPermissions`, `--resume <session-id>` |
| Prompt source | `prompt` from dispatch |
| JSON metadata | terminal result must include `session_id` |

Example:

```sh
claude -p --output-format json --permission-mode bypassPermissions "Continue implementation for TASK-123"
claude -p --output-format json --permission-mode bypassPermissions --resume 66f8e8b6-... "Apply review feedback"
```

The prompt passed to `-p` is the full task description/intent sent by Switchyard. The workspace working directory is set to the task's workspace path for the child process.

## Installation and authentication

Claude Code must be installed and authenticated on the same OS account that runs node-agent. The node-agent installers only deploy the worker; they do not install `claude` or provision Anthropic credentials.

1. Install Claude Code natively (curl/Homebrew/winget/apt) using the [official installation instructions](https://code.claude.com/docs/en/overview).
2. Authenticate: run `claude` interactively once under that account, complete login/API key setup, and confirm `claude --version` works.
3. Restart node-agent so capability detection picks up `claude` and its version (`versions.claude` in `/api/nodes`).
4. Only enable `claude` on trusted worker hosts.

## Non-interactive permissions

`--permission-mode bypassPermissions` allows Claude Code to proceed without interactive approval (edits, reads, and permitted shell commands under the configured workspace). This is necessary for unattended headless execution. Do not run `claude` with this mode on untrusted or shared-hosting accounts. Review your Claude Code permission settings for the worker account.

## Session continuity

Kanban binds one Claude session per card in `harness_bindings` (`harness_kind='claude'`). The identity field is `claude_session_id`.

- **First run**: omit `--resume`, run with the full prompt, and require that the terminal JSON result includes `session_id`. The worker uses that returned ID to bind the card.
- **Continuation**: pass `--resume <bound_session_id>` and must never create a new session to recover from a failure.
- **No workspace/turn identity**: Claude does not return a workspace id or turn sequence. Only the run-ownership fence (`current_run_id`) prevents replay of a stale result. Switchyard rejects any result whose `claude_session_id` differs from the dispatched one (`claude_identity_rejected`).
- **Deterministic failure on missing session**: if a successful run produces no `session_id`, the worker surfaces `claude_session_missing:` and does not retry; the card becomes `blocked`.

## Result parsing

Claude's terminal JSON result (when using `--output-format json`) contains:

```json
{
  "session_id": "...",
  "result": "..."
}
```

The worker takes `session_id` as the continuity ID and treats `result` as the task output text. Provenance records `executor=claude` and `claude_session_id`. The worker does not commit or push; results land in `review`.

## Capability advertisement

When probed, node-agent reports `claude` in `executors` and `versions.claude` with `claude --version` output. Binary detection accepts `claude` on `PATH`. Health checks fail with `claude_unavailable: Claude Code CLI (claude) not installed or not on PATH` if missing.

## Troubleshooting

- **`claude_unavailable`**: install `claude` and restart node-agent. Verify `which claude && claude --version` under the worker account.
- **`claude_session_missing`**: Claude returned a successful JSON run but omitted `session_id`. Re-run explicitly; this is not retried automatically.
- **Permissions**: if Claude pauses for approval despite `--permission-mode bypassPermissions`, check the account's Claude Code settings/permissions for that host.
- **Authentication**: ensure the account has a valid Claude login/API key. Claude Code sessions persist on-disk under the worker account's configuration, so re-auth can invalidate old bindings.

## See also

- [Switchyard continuity contract](../../switchyard/docs/features/claude-code-executor.md)
- [Execution flow](../../switchyard/docs/execution-flow.md)