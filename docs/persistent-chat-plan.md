# Implement persistent chat in node-agent

Goal: allow Switchyard to maintain a persistent agent-side conversation per task or per workspace over node-agent, instead of launching a fresh `hermes chat -q` per dispatch.

## Why
- Short prompts lose context between tasks; long prompts explode tokens.
- Persistent conversation keeps the local executor (hermes/codex/commandcode) alive and reuses its context.
- Matches the agentic-flow vision: orchestrator delegates to a stable local agent, not a one-shot CLI per task.

## Constraints
- Keep HTTP long-poll fallback working for environments that do not support gRPC.
- Use session manager for delivery IDs; do not duplicate session semantics.
- Security: require `NODE_AGENT_TOKEN` on gRPC metadata and HTTP headers.
- Worker binary runs on Mac/Windows; server on VPS. Cross-compile correctly.

## Design

### 1) Conversation store
Add `internal/conversation/store.go`:
- Backed by local SQLite file per node (e.g. `$HOME/.node-agent/chat.db`).
- Schema:
  - `conversations(id TEXT PK, workspace TEXT, executor TEXT, created_at INT, last_active INT)`
  - `messages(id TEXT PK, conversation_id TEXT, role TEXT, content TEXT, created_at INT)`
- Methods: `EnsureConversation(workspace, executor) (id, error)`, `AppendMessage(convID, role, content)`, `GetContext(convID, limit) []Message`.
- ponytail: no ORM; stdlib `database/sql` with modernc.org/sqlite is enough.

### 2) Job schema extensions
Add fields to `transport.DispatchRequest`:
- `conversation_id` (optional). If empty, agent auto-creates or reuses workspace conversation.
- `append_only` (bool). When true, agent only appends user message; when false, it resets conversation.
- `context_window` (int). Max past messages to include when building prompt context.

### 3) Agent executor changes
In `cmd/agent/main.go:runJob()`:
- If executor is `hermes`:
  - Load conversation store.
  - Load or create conversation for workspace.
  - Build prompt from conversation context + new message.
  - Execute via hermes CLI `chat -q` with constructed prompt.
  - Append user and assistant messages to store.
- If executor is `codex`/`commandcode`/`shell`:
  - For now, continue one-shot. Later, keep a persistent process per conversation if supported.

### 4) gRPC session integration
In `cmd/server/grpc.go:Connect()`:
- On `DispatchJob`, include conversation context in the frame (as additional metadata).
- Worker acknowledges with `JobAck` and can request more context via `JobProgress{Phase:"context_request"}` (future).
- Server can send `ServerNotice{Code:"context_hint", Message: convID}` after register to hint the worker about conversation continuity.

### 5) API changes
- `POST /api/dispatch` accepts `conversation_id`, `append_only`, `context_window`.
- `GET /api/results/{task_id}` response includes `conversation_id` used.
- `GET /api/conversations` (new) lists active conversations on the node (via node-agent health endpoint).
- `POST /api/conversations/{id}/reset` clears conversation history.

## Implementation steps

1. Add conversation store with SQLite.
2. Add conversation fields to transport types.
3. Implement conversation load/save in agent executor.
4. Extend gRPC frame to carry conversation metadata.
5. Extend HTTP dispatch API to accept conversation params.
6. Update README with persistent chat docs.
7. Add tests for store and executor integration.
8. Cross-compile and test on Mac.

## Verification
- `go vet ./...` and `go test ./...` pass.
- Dispatch with `conversation_id` reuses context.
- Dispatch without `conversation_id` auto-creates per workspace.
- Reset endpoint clears history.
- gRPC and HTTP transports both work.
