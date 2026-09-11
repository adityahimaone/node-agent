package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
	executors, versions := detectExecutors()

	register := func() error {
		return postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID: nodeID, Hostname: hostname(), Version: "0.3.0", Workspaces: loadWorkspaces(), Executors: executors, Versions: versions})
	}
	transportMode := strings.ToLower(strings.TrimSpace(os.Getenv("NODE_AGENT_TRANSPORT")))
	if transportMode == "" {
		transportMode = "auto"
	}
	grpcTarget := os.Getenv("NODE_AGENT_GRPC_TARGET")
	if transportMode == "grpc" || (transportMode == "auto" && grpcTarget != "") {
		if err := runGRPC(server, grpcTarget, nodeID, wsPaths, executors, versions); err == nil {
			return
		} else if transportMode == "grpc" {
			log.Fatalf("grpc transport: %v", err)
		} else {
			log.Printf("grpc unavailable, falling back to HTTP: %v", err)
		}
	}
	if err := postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID: nodeID, Hostname: hostname(), Version: "0.3.0", Workspaces: wsPaths, Executors: executors, Versions: versions, Transports: []string{"http"}}); err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered %s transport=http workspaces=%v", nodeID, wsPaths)

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

func runGRPC(server, target, nodeID string, wsPaths, executors []string, versions map[string]string) error {
	if target == "" {
		return fmt.Errorf("NODE_AGENT_GRPC_TARGET is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(transport.Codec{})))
	if err != nil {
		return fmt.Errorf("grpc connect: %w", err)
	}
	defer conn.Close()
	streamCtx := context.Background()
	if agentToken != "" {
		streamCtx = metadata.NewOutgoingContext(streamCtx, metadata.Pairs("x-node-agent-token", agentToken))
	}
	stream, err := transport.NewNodeAgentServiceClient(conn).Connect(streamCtx)
	if err != nil {
		return fmt.Errorf("grpc stream: %w", err)
	}
	var sendMu sync.Mutex
	send := func(frame *transport.WorkerFrame) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(frame)
	}
	if err := send(&transport.WorkerFrame{Register: &transport.RegisterFrame{NodeID: nodeID, Hostname: hostname(), Version: "0.3.0", Workspaces: wsPaths, Executors: executors, Versions: versions, Transports: []string{"grpc", "http"}}}); err != nil {
		return fmt.Errorf("grpc register: %w", err)
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := send(&transport.WorkerFrame{Heartbeat: &transport.HeartbeatFrame{NodeID: nodeID, Status: func() string {
				if atomic.LoadInt32(&busy) == 1 {
					return "busy"
				}
				return "idle"
			}()}}); err != nil {
				return
			}
		}
	}()
	ack, err := stream.Recv()
	if err != nil || ack.RegisterAck == nil {
		return fmt.Errorf("grpc register ack: %w", err)
	}
	log.Printf("registered %s transport=grpc session=%s target=%s (http fallback=%s)", nodeID, ack.RegisterAck.SessionID, target, server)
	for {
		frame, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("grpc receive: %w", err)
		}
		if frame.DispatchJob == nil {
			continue
		}
		job := frame.DispatchJob
		atomic.StoreInt32(&busy, 1)
		_ = stream.Send(&transport.WorkerFrame{Heartbeat: &transport.HeartbeatFrame{NodeID: nodeID, Status: "busy"}})
		if err := stream.Send(&transport.WorkerFrame{JobAck: &transport.JobAck{DeliveryID: job.DeliveryID, Accepted: true}}); err != nil {
			return err
		}
		start := time.Now()
		output, ok, errStr := runJob(transport.DispatchRequest{TaskID: job.TaskID, Board: job.Board, Message: job.Message, Workspace: job.Workspace, Model: job.Model, Provider: job.Provider, Executor: job.Executor, Command: job.Command, PrequestNote: job.PrequestNote})
		dur := time.Since(start).Milliseconds()
		atomic.StoreInt32(&busy, 0)
		if err := stream.Send(&transport.WorkerFrame{JobResult: &transport.JobResult{DeliveryID: job.DeliveryID, TaskID: job.TaskID, Success: ok, Output: output, Error: errStr, DurationMs: dur}}); err != nil {
			return err
		}
		_ = stream.Send(&transport.WorkerFrame{Heartbeat: &transport.HeartbeatFrame{NodeID: nodeID, Status: "idle"}})
	}
}
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

	// AI executors receive project context. Shell is a direct terminal fast path:
	// no CodeGraph init, README/AGENTS injection, or Hermes session preamble.
	executor := strings.ToLower(strings.TrimSpace(job.Executor))
	if executor == "" {
		executor = "auto"
	}
	prompt := job.Message
	if executor != "shell" {
		cgStatus := ensureCodegraph(ws)
		prequest := readPrequest(ws, job.PrequestNote)
		if prequest != "" || cgStatus != "" {
			prompt = prequest + "\n\n[" + cgStatus + "]\n\nTask:\n" + job.Message
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
	var resolvedBin string
	var resolvedArgs []string
	var resolvedEnv []string

	switch executor {
	case "shell":
		command := job.Command
		if strings.TrimSpace(command) == "" {
			return "", false, "shell executor requires command"
		}
		resolvedBin = shell
		resolvedArgs = []string{shellFlag, rewriteShellCmd(command)}
		cmd = exec.CommandContext(ctx, resolvedBin, resolvedArgs...)
	case "hermes":
		bin := findBin("hermes")
		if bin == "" {
			return "", false, "executor_unavailable: hermes"
		}
		resolvedBin = bin
		resolvedArgs = []string{"chat", "-q"}
		resolvedEnv = append(os.Environ(), "HERMES_WORKSPACE="+ws)
		cmd = exec.CommandContext(ctx, bin, "chat", "-q", prompt)
		cmd.Env = resolvedEnv
	case "codex":
		bin := findBin("codex")
		if bin == "" {
			return "", false, "executor_unavailable: codex"
		}
		resolvedBin = bin
		resolvedArgs = []string{"exec", "--full-auto"}
		cmd = exec.CommandContext(ctx, bin, "exec", "--full-auto", prompt)
	case "commandcode":
		bin := commandCodeBin()
		if bin == "" {
			return "", false, "executor_unavailable: commandcode (cmd/cmdc)"
		}
		resolvedBin = bin
		resolvedArgs = []string{"-p", "--yolo", "--skip-onboarding", "--output-format", "text"}
		cmd = exec.CommandContext(ctx, bin, "-p", prompt, "--yolo", "--skip-onboarding", "--output-format", "text")
	case "auto":
		// Auto preserves the historical preference but remains explicit in the result.
		if findBin("hermes") != "" {
			job.Executor = "hermes"
			return runJob(job)
		}
		if findBin("codex") != "" {
			job.Executor = "codex"
			return runJob(job)
		}
		if commandCodeBin() != "" {
			job.Executor = "commandcode"
			return runJob(job)
		}
		return "", false, "executor_unavailable: auto found no AI executor"
	default:
		return "", false, fmt.Sprintf("unknown executor %q", executor)
	}
	cmd.Dir = ws
	out, err := cmd.CombinedOutput()
	// Provenance header: first line of every result proves which binary ran.
	provenance := fmt.Sprintf("provenance executor=%s requested=%s bin=%s args=%q ws=%s",
		executor, strings.ToLower(strings.TrimSpace(job.Executor)), resolvedBin, resolvedArgs, ws)
	proof := fmt.Sprintf("EXECUTOR_PROOF=%s", executor)
	out = append([]byte(provenance+"\n"), out...)
	out = append(out, []byte("\n"+proof+"\n"+provenance+"\n")...)

	_ = os.WriteFile(logFile, out, 0644)
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), false, fmt.Sprintf("job timed out after %s", jobTimeout())
	}
	if err != nil {
		return string(out), false, err.Error()
	}
	return string(out), true, ""
}

func commandCodeBin() string {
	if runtime.GOOS == "windows" {
		return findBinAny("cmdc", "command-code")
	}
	return findBinAny("cmd", "command-code")
}

func findBinAny(names ...string) string {
	for _, name := range names {
		if p := findBin(name); p != "" {
			return p
		}
	}
	return ""
}

func detectExecutors() ([]string, map[string]string) {
	checks := []struct {
		name string
		bins []string
	}{
		{"hermes", []string{"hermes"}}, {"codex", []string{"codex"}},
		{"commandcode", []string{"cmd", "cmdc", "command-code"}},
	}
	var out []string
	versions := map[string]string{}
	for _, c := range checks {
		if bin := findBinAny(c.bins...); bin != "" {
			out = append(out, c.name)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			b, _ := exec.CommandContext(ctx, bin, "--version").CombinedOutput()
			cancel()
			versions[c.name] = strings.TrimSpace(string(b))
		}
	}
	out = append(out, "shell")
	return out, versions
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
