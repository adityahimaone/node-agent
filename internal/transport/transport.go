package transport

import (
	"encoding/json"
	"net/http"
	"time"

	"node-agent/internal/heartbeat"
)

// Request types — plain HTTP JSON, no grpc codegen needed.

// Register — agent dials in.
type RegisterRequest struct {
	NodeID     string               `json:"node_id"` // "mac" / "windows"
	Hostname   string               `json:"hostname"`
	Version    string               `json:"version"`
	Workspaces []string             `json:"workspaces"`          // paths from ~/.hermes/workspaces.json
	Executors  []string             `json:"executors,omitempty"` // installed runners on this node
	Versions   map[string]string    `json:"versions,omitempty"`
	DSHHealth  *heartbeat.DSHHealth `json:"dsh_health,omitempty"`
	Transports []string             `json:"transports,omitempty"`
}

// Heartbeat
type HeartbeatRequest struct {
	NodeID    string               `json:"node_id"`
	Status    string               `json:"status"` // idle|busy
	DSHHealth *heartbeat.DSHHealth `json:"dsh_health,omitempty"`
}

// Dispatch — server -> agent
type DispatchRequest struct {
	TaskID        string `json:"task_id"`
	Board         string `json:"board"`
	Message       string `json:"message"`
	Workspace     string `json:"workspace"` // absolute path on agent
	Model         string `json:"model"`
	Provider      string `json:"provider"`
	Executor      string `json:"executor,omitempty"`       // auto|hermes|codex|commandcode|omp|shell
	Command       string `json:"command,omitempty"`        // only used by shell executor
	ExecutionMode string `json:"execution_mode,omitempty"` // direct|agentic
	NoRTK         bool   `json:"no_rtk,omitempty"`         // preserve machine-readable command output
	MaxIterations int    `json:"max_iterations,omitempty"`
	Acceptance    string `json:"acceptance,omitempty"`
	// PrequestNote is the workspace prequest (project prerequisites) injected
	// by the server from workspaces.json Note on first runs, not continuations.
	PrequestNote string `json:"prequest_note,omitempty"`
	// DSHSessionID resumes same DeepSeek Harness session for task comments.
	DSHSessionID   string `json:"dsh_session_id,omitempty"`
	DSHWorkspaceID string `json:"dsh_workspace_id,omitempty"`
	// HarnessKind names the continuity harness ("dsh", "commandcode" or "omp").
	HarnessKind string `json:"harness_kind,omitempty"`
	// CommandCodeSessionID resumes the same Command Code session for task comments.
	CommandCodeSessionID string `json:"commandcode_session_id,omitempty"`
	// DSHPermissionMode is the sandbox/approval preset for a dsh run
	// (read-only|workspace-write|danger-full-access). It maps to the
	// DSH_PERMISSION_MODE env the dsh base patch reads. Empty leaves the dsh
	// profile default (workspace-write).
	DSHPermissionMode string `json:"dsh_permission_mode,omitempty"`
	// CommandCodeMode selects the Command Code permission mode
	// (standard|plan|accept-edits|yolo). Empty keeps the historical --yolo.
	CommandCodeMode string `json:"commandcode_mode,omitempty"`
	// OMPSessionID resumes the same omp session for task comments.
	OMPSessionID string `json:"omp_session_id,omitempty"`
	// ClaudeSessionID resumes the same Claude Code CLI session for task comments.
	ClaudeSessionID     string `json:"claude_session_id,omitempty"`
	LastTurnSeq         *int64 `json:"last_turn_seq,omitempty"`
	LastCommentID       *int64 `json:"last_comment_id,omitempty"`
	RunID               string `json:"run_id,omitempty"`
	SessionContinuation bool   `json:"session_continuation,omitempty"`
	// Persistent chat. ConversationID empty => server/agent auto-resolves per
	// workspace. AppendOnly=false means reset context before this message.
	ConversationID string `json:"conversation_id,omitempty"`
	AppendOnly     bool   `json:"append_only,omitempty"`
	ContextWindow  int    `json:"context_window,omitempty"`
	// TimeoutS overrides the job timeout for this dispatch, in seconds.
	//
	// It exists because one class of job legitimately outlasts the default: a
	// visual suite signs in, walks every page in both themes and runs a full axe
	// scan, which can run well past 600s on a cold start. Raising the global
	// default instead would make every hung job wait an hour to be noticed.
	TimeoutS int `json:"timeout_s,omitempty"`
	// ArtifactDir is a per-job directory the agent uploads to before posting its
	// result. Empty for every job that produces no artifacts.
	ArtifactDir string `json:"artifact_dir,omitempty"`
}

// Progress — agent -> server. Sent while executor is still running.
type ProgressRequest struct {
	TaskID string `json:"task_id"`
	Chunk  string `json:"chunk"`
}

// Result — agent -> server
type ResultRequest struct {
	TaskID         string `json:"task_id"`
	Success        bool   `json:"success"`
	Output         string `json:"output"`
	Error          string `json:"error,omitempty"`
	DurationMs     int64  `json:"duration_ms"`
	DSHSessionID   string `json:"dsh_session_id,omitempty"`
	DSHWorkspaceID string `json:"dsh_workspace_id,omitempty"`
	// CommandCodeSessionID is the Command Code session this run belongs to.
	CommandCodeSessionID string `json:"commandcode_session_id,omitempty"`
	// OMPSessionID is the omp session this run belongs to.
	OMPSessionID string `json:"omp_session_id,omitempty"`
	// ClaudeSessionID is the Claude Code CLI session this run belongs to.
	ClaudeSessionID string `json:"claude_session_id,omitempty"`
	LastTurnSeq     *int64 `json:"last_turn_seq,omitempty"`
	// Artifacts are files the run produced, already stored on this node. Only
	// the metadata rides along here; the control plane pulls the bytes over the
	// artifact endpoint. See cmd/server/artifacts.go.
	Artifacts []Artifact `json:"artifacts,omitempty"`
}

// Artifact is one file stored for a task, as reported in a result.
//
// Name is the generated on-disk filename and is the only handle a client gets —
// Path is informational. Together they are what lets the control plane fetch a
// file without ever naming a path of its own.
type Artifact struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	MIME   string `json:"mime"`
}

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func ReadJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

var _ = time.Now
