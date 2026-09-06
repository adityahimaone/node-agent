package transport

import (
	"encoding/json"
	"net/http"
	"time"
)

// Request types — plain HTTP JSON, no grpc codegen needed.

// Register — agent dials in.
type RegisterRequest struct {
	NodeID   string `json:"node_id"`   // "mac" / "windows"
	Hostname string `json:"hostname"`
	Version  string `json:"version"`
	Workspaces []string `json:"workspaces"` // paths from ~/.hermes/workspaces.json
}

// Heartbeat
type HeartbeatRequest struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"` // idle|busy
}

// Dispatch — server -> agent
type DispatchRequest struct {
	TaskID    string `json:"task_id"`
	Board     string `json:"board"`
	Message   string `json:"message"`
	Workspace string `json:"workspace"` // absolute path on agent
	Model     string `json:"model"`
	Provider  string `json:"provider"`
}

// Result — agent -> server
type ResultRequest struct {
	TaskID  string `json:"task_id"`
	Success bool   `json:"success"`
	Output  string `json:"output"`
	Error   string `json:"error,omitempty"`
	DurationMs int64 `json:"duration_ms"`
}

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func ReadJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}
var _ = time.Now
