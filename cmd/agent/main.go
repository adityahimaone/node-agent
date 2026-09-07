package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"node-agent/internal/transport"
)

// httpClient is shared by register/heartbeat/result posts. The old code used
// http.Post (= http.DefaultClient, no timeout) — if the tailnet stalls
// (Mac sleep/wake, wifi switch) those calls could hang forever with no
// retry, silently killing the heartbeat goroutine or the result post.
var httpClient = &http.Client{Timeout: 10 * time.Second}

// pollClient is separate because long-poll intentionally blocks up to ~25s
// server-side; give it a bit of headroom instead of sharing the 10s client.
var pollClient = &http.Client{Timeout: 30 * time.Second}

var agentToken = os.Getenv("NODE_AGENT_TOKEN")

// busy tracks real job state. The previous heartbeatLoop hardcoded
// Status:"idle" on every tick regardless of whether a job was running,
// so a long-running hermes/codex job could get reported as idle mid-flight.
var busy int32

func main() {
	server := os.Getenv("NODE_AGENT_SERVER")
	if server == "" {
		server = "http://100.64.0.1:8788"
	}
	nodeID := os.Getenv("NODE_AGENT_ID")
	if nodeID == "" {
		h, _ := os.Hostname()
		if h == "" {
			h = "mac"
		}
		// normalize: adityas-macbook-pro -> mac, aditya-rtx -> windows
		if len(h) > 3 && h[:6] == "aditya" {
			if contains(h, "macbook") {
				nodeID = "mac"
			} else {
				nodeID = "windows"
			}
		} else {
			nodeID = h
		}
	}
	if agentToken == "" {
		log.Printf("WARNING: NODE_AGENT_TOKEN is not set — requests to %s will be unauthenticated", server)
	}
	fmt.Printf("node-agent %s -> %s\n", nodeID, server)

	wsPaths := loadWorkspaces()

	register := func() error {
		return postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID: nodeID, Hostname: hostname(), Version: "0.2.0", Workspaces: loadWorkspaces()})
	}
	if err := postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID: nodeID, Hostname: hostname(), Version: "0.2.0", Workspaces: wsPaths}); err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered %s workspaces=%v", nodeID, wsPaths)

	go heartbeatLoop(server, nodeID)

	for {
		req, _ := http.NewRequest("GET", server+"/api/nodes/"+nodeID+"/poll", nil)
		if agentToken != "" {
			req.Header.Set("X-Node-Agent-Token", agentToken)
		}
		resp, err := pollClient.Do(req)
		if err != nil {
			log.Printf("poll err: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode == 204 {
			resp.Body.Close()
			continue
		}
		if resp.StatusCode == 404 {
			// server restarted / lost state — re-register then keep polling
			resp.Body.Close()
			if rerr := register(); rerr != nil {
				log.Printf("re-register: %v", rerr)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		if resp.StatusCode != 200 {
			b := readAll(resp)
			log.Printf("poll %d: %s", resp.StatusCode, string(b))
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		var job transport.DispatchRequest
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			log.Printf("decode: %v", err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		log.Printf("job %s ws=%s msg=%.80s", job.TaskID, job.Workspace, job.Message)

		atomic.StoreInt32(&busy, 1)
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID: nodeID, Status: "busy"})
		start := time.Now()
		output, ok, errStr := runJob(job)
		dur := time.Since(start).Milliseconds()
		atomic.StoreInt32(&busy, 0)

		res := transport.ResultRequest{TaskID: job.TaskID, Success: ok, Output: output, Error: errStr, DurationMs: dur}
		if err := postJSON(server+"/api/nodes/"+nodeID+"/result", res); err != nil {
			log.Printf("result post err: %v", err)
		}
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID: nodeID, Status: "idle"})
	}
}

func heartbeatLoop(server, nodeID string) {
	for {
		time.Sleep(15 * time.Second)
		status := "idle"
		if atomic.LoadInt32(&busy) == 1 {
			status = "busy"
		}
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID: nodeID, Status: status})
	}
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }
func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "unknown"
	}
	return h
}
func loadWorkspaces() []string {
	f, err := os.ReadFile(os.ExpandEnv("$HOME/.hermes/workspaces.json"))
	if err != nil {
		return nil
	}
	var data struct {
		Workspaces []struct {
			Path string `json:"path"`
		} `json:"workspaces"`
	}
	if err := json.Unmarshal(f, &data); err != nil {
		return nil
	}
	out := make([]string, 0, len(data.Workspaces))
	for _, w := range data.Workspaces {
		out = append(out, w.Path)
	}
	return out
}
func postJSON(url string, v any) error {
	b, _ := json.Marshal(v)
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if agentToken != "" {
		req.Header.Set("X-Node-Agent-Token", agentToken)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		bb := readAll(resp)
		return fmt.Errorf("%d %s", resp.StatusCode, string(bb))
	}
	return nil
}
func readAll(r *http.Response) []byte { defer r.Body.Close(); b, _ := bytesReadAll(r); return b }
func bytesReadAll(r *http.Response) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, e := buf.ReadFrom(r.Body)
	return buf.Bytes(), e
}

// jobTimeout caps how long a single dispatched job may run. Previously
// exec.Command had no deadline at all: a hung `hermes chat` or `codex exec`
// call would wedge the agent's single poll loop forever (no new jobs, no
// heartbeat status change) with no way to recover except killing the process
// by hand. Override with NODE_AGENT_JOB_TIMEOUT (seconds).
func jobTimeout() time.Duration {
	secs := 600
	if v := os.Getenv("NODE_AGENT_JOB_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	return time.Duration(secs) * time.Second
}

func runJob(job transport.DispatchRequest) (output string, ok bool, errStr string) {
	ws := job.Workspace
	if ws == "" {
		ws = os.ExpandEnv("$HOME")
	}
	if _, err := os.Stat(ws); err != nil {
		return "", false, fmt.Sprintf("workspace not found: %s", ws)
	}
	tmpDir := filepath.Join(os.TempDir(), "node-agent-"+job.TaskID)
	_ = os.MkdirAll(tmpDir, 0755)
	logFile := filepath.Join(tmpDir, "run.log")

	// Context build: codegraph ensure + project prerequisites, then prompt.
	cgStatus := ensureCodegraph(ws)
	prequest := readPrequest(ws, job.PrequestNote)
	prompt := job.Message
	if prequest != "" || cgStatus != "" {
		prompt = prequest + "\n\n[" + cgStatus + "]\n\nTask:\n" + job.Message
	}

	// Heuristic: shell meta or common CLI prefix → run shell directly (no LLM).
	// Otherwise treat as hermes/codex task prompt.
	shellPrefixes := []string{"git ", "ls ", "cat ", "echo ", "pwd", "cd ", "grep ", "find ", "head ", "tail ", "curl ", "node ", "npm ", "pnpm ", "python "}
	isShell := bytes.Contains([]byte(job.Message), []byte(";")) || bytes.Contains([]byte(job.Message), []byte("&&")) || bytes.Contains([]byte(job.Message), []byte("|"))
	for _, p := range shellPrefixes {
		if bytes.HasPrefix([]byte(job.Message), []byte(p)) {
			isShell = true
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout())
	defer cancel()

	// Windows has no bash — WSL's bash lacks most coreutils and breaks
	// chdir to Windows paths. Use cmd /c there, bash -lc elsewhere.
	shell, shellFlag := "bash", "-lc"
	if runtime.GOOS == "windows" {
		shell, shellFlag = "cmd", "/c"
	}

	var cmd *exec.Cmd
	if isShell {
		cmd = exec.CommandContext(ctx, shell, shellFlag, rewriteShellCmd(job.Message))
		cmd.Dir = ws
	} else {
		hermesBin := findBin("hermes")
		codexBin := findBin("codex")
		if hermesBin != "" {
			cmd = exec.CommandContext(ctx, hermesBin, "chat", "-q", prompt)
			cmd.Dir = ws
			cmd.Env = append(os.Environ(), "HERMES_WORKSPACE="+ws)
		} else if codexBin != "" {
			cmd = exec.CommandContext(ctx, codexBin, "exec", "--full-auto", prompt)
			cmd.Dir = ws
		} else {
			cmd = exec.CommandContext(ctx, shell, shellFlag, job.Message)
			cmd.Dir = ws
		}
	}
	out, err := cmd.CombinedOutput()
	_ = os.WriteFile(logFile, out, 0644)
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), false, fmt.Sprintf("job timed out after %s", jobTimeout())
	}
	if err != nil {
		return string(out), false, err.Error()
	}
	return string(out), true, ""
}

// ensureCodegraph makes sure the workspace has a codegraph index. Non-fatal:
// any failure just means the agent works without graph context.
// - .codegraph/ exists  -> skip (codegraph auto-syncs from here)
// - binary exists       -> codegraph init (60s cap)
// - no binary           -> skip (no auto-install; install manually once)
func ensureCodegraph(ws string) string {
	if _, err := os.Stat(filepath.Join(ws, ".codegraph")); err == nil {
		return "codegraph: index exists (auto-sync active)"
	}
	bin := findBin("codegraph")
	if bin == "" {
		return "codegraph: not installed, skipped (fallback: FILE_INDEX/AGENTS.md)"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "init")
	cmd.Dir = ws
	_, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return "codegraph init: timed out after 60s (continuing without)"
	}
	if err != nil {
		return fmt.Sprintf("codegraph init failed (continuing): %v", err)
	}
	return "codegraph init: ok"
}

// readPrequest returns the project prerequisites text: the server-injected
// Note from workspaces.json wins, else AGENTS.md head, else README.md head.
func readPrequest(ws, note string) string {
	if note != "" {
		return "Project prerequisites (workspaces note):\n" + note
	}
	for _, name := range []string{"AGENTS.md", "README.md"} {
		b, err := os.ReadFile(filepath.Join(ws, name))
		if err != nil {
			continue
		}
		lines := strings.Split(string(b), "\n")
		if len(lines) > 100 {
			lines = lines[:100]
		}
		return fmt.Sprintf("Project prerequisites (%s head):\n%s", name, strings.Join(lines, "\n"))
	}
	return ""
}

// rewriteShellCmd tries rtk rewrite via `rtk hook check` then `rtk rewrite`.
// Single source of truth for hermes/Claude hooks. If rtk knows a compact
// form, return rewritten; otherwise raw. 800ms cap so broken rtk never stalls.
// ponytail: `rtk rewrite` exit code is unstable across versions (observed 3
// with valid output); rely on non-empty output not starting with "No rewrite".
func rewriteShellCmd(raw string) string {
	if os.Getenv("NODE_AGENT_NO_RTK") == "1" {
		return raw
	}
	bin := findBin("rtk")
	if bin == "" {
		if p, err := exec.LookPath("rtk"); err == nil {
			bin = p
		} else {
			return raw
		}
	}
	try := func(args ...string) (string, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, args...)
		out, _ := cmd.CombinedOutput()
		s := strings.TrimSpace(string(out))
		if s == "" || strings.HasPrefix(s, "No rewrite for:") {
			return "", false
		}
		return s, true
	}
	if s, ok := try("hook", "check", raw); ok {
		return s
	}
	if s, ok := try("rewrite", raw); ok {
		return s
	}
	return raw
}

func findBin(name string) string {
	for _, p := range []string{os.ExpandEnv("$HOME/.local/bin/" + name), "/opt/homebrew/bin/" + name, "/usr/local/bin/" + name} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if b, err := exec.LookPath(name); err == nil {
		return b
	}
	return ""
}
