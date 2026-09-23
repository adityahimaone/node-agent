package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"node-agent/internal/conversation"
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

var dshWorkspaceMu sync.Mutex

type dshWorkspaceStorage struct {
	Unit   map[string]any `json:"unit"`
	Global map[string]any `json:"global"`
	Tables struct {
		Workspaces map[string]dshWorkspace `json:"workspaces"`
	} `json:"tables"`
}

type dshWorkspace struct {
	Path       string   `json:"path"`
	Title      string   `json:"title"`
	SessionIDs []string `json:"sessionIds"`
	CreatedAt  string   `json:"createdAt"`
	UpdatedAt  string   `json:"updatedAt"`
}

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
		output, ok, errStr := runJobWithProgress(job, func(phase, message string) {
			marker, _ := json.Marshal(map[string]string{"phase": phase, "label": message})
			_ = postJSON(server+"/api/nodes/progress", transport.ProgressRequest{TaskID: job.TaskID, Chunk: "HERMES_EVENT: " + string(marker) + "\n"})
		})
		dur := time.Since(start).Milliseconds()
		atomic.StoreInt32(&busy, 0)

		res := dshResult(job, output, ok, errStr, dur)
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
		output, ok, errStr := runJobWithProgress(transport.DispatchRequest{TaskID: job.TaskID, Board: job.Board, Message: job.Message, Workspace: job.Workspace, Model: job.Model, Provider: job.Provider, Executor: job.Executor, Command: job.Command, ExecutionMode: job.ExecutionMode, NoRTK: job.NoRTK, MaxIterations: job.MaxIterations, Acceptance: job.Acceptance, PrequestNote: job.PrequestNote, DSHSessionID: job.DSHSessionID, DSHWorkspaceID: job.DSHWorkspaceID, LastTurnSeq: job.LastTurnSeq, LastCommentID: job.LastCommentID, RunID: job.RunID, SessionContinuation: job.SessionContinuation, ConversationID: job.ConversationID, AppendOnly: job.AppendOnly, ContextWindow: job.ContextWindow}, func(phase, message string) {
			if err := send(&transport.WorkerFrame{JobProgress: &transport.JobProgress{DeliveryID: job.DeliveryID, TaskID: job.TaskID, Phase: phase, Message: message}}); err != nil {
				log.Printf("progress send: %v", err)
			}
		})
		dur := time.Since(start).Milliseconds()
		atomic.StoreInt32(&busy, 0)
		res := dshResult(transport.DispatchRequest{TaskID: job.TaskID, DSHSessionID: job.DSHSessionID, DSHWorkspaceID: job.DSHWorkspaceID, LastTurnSeq: job.LastTurnSeq}, output, ok, errStr, dur)
		if err := stream.Send(&transport.WorkerFrame{JobResult: &transport.JobResult{DeliveryID: job.DeliveryID, TaskID: res.TaskID, Success: res.Success, Output: res.Output, Error: res.Error, DurationMs: res.DurationMs, DSHSessionID: res.DSHSessionID, DSHWorkspaceID: res.DSHWorkspaceID, LastTurnSeq: res.LastTurnSeq}}); err != nil {
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
	// Keep this in sync with the control plane's RemoteJobTimeout default.
	secs := 600
	if v := os.Getenv("NODE_AGENT_JOB_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	return time.Duration(secs) * time.Second
}

func agenticJobTimeout() time.Duration {
	secs := 1200
	if v := os.Getenv("NODE_AGENT_SHELL_AGENTIC_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	return time.Duration(secs) * time.Second
}

var convStore *conversation.Store

func initConvStore() error {
	if convStore != nil {
		return nil
	}
	home := os.Getenv("HOME")
	if home == "" {
		home = os.TempDir()
	}
	s, err := conversation.NewStore(home)
	if err != nil {
		return err
	}
	convStore = s
	return nil
}

// resolveConversation returns the conversation id to use for this job. If the
// job carries an explicit ConversationID, it is used. Otherwise the store picks
// the most recent conversation for the workspace+executor, creating one when
// none exists. When AppendOnly is false, the conversation history is cleared
// before this message so the prompt starts fresh.
func resolveConversation(job transport.DispatchRequest) (string, error) {
	if err := initConvStore(); err != nil {
		return "", err
	}
	if job.ConversationID != "" {
		if !job.AppendOnly {
			if err := convStore.ResetConversation(job.ConversationID); err != nil {
				return "", err
			}
		}
		return job.ConversationID, nil
	}
	exec := strings.ToLower(strings.TrimSpace(job.Executor))
	if exec == "" || exec == "auto" {
		exec = "hermes"
	}
	id, err := convStore.EnsureConversation(job.Workspace, exec)
	if err != nil {
		return "", err
	}
	// Empty ConversationID means persistent workspace chat. Keep history by
	// default; reset is explicit through a supplied ConversationID plus
	// append_only=false.
	return id, nil
}

func runJob(job transport.DispatchRequest) (output string, ok bool, errStr string) {
	return runJobWithProgress(job, nil)
}

func runJobWithProgress(job transport.DispatchRequest, onProgress func(string, string)) (output string, ok bool, errStr string) {
	maxDSHConflictRetries := 15
	if raw := strings.TrimSpace(os.Getenv("NODE_AGENT_DSH_CONFLICT_RETRIES")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			maxDSHConflictRetries = parsed
		}
	}
	for attempt := 0; ; attempt++ {
		output, ok, errStr = runJobWithProgressAttempt(job, onProgress)
		if !strings.HasPrefix(errStr, "dsh_session_conflict_retry:") {
			return output, ok, errStr
		}
		if attempt >= maxDSHConflictRetries {
			return output, false, "dsh_session_conflict: " + strings.TrimPrefix(errStr, "dsh_session_conflict_retry: ")
		}
		// DSH write handles can outlive a short-lived CLI process. Retry same
		// session only; never clear ID or create a new session.
		time.Sleep(750 * time.Millisecond)
	}
}

func runJobWithProgressAttempt(job transport.DispatchRequest, onProgress func(string, string)) (output string, ok bool, errStr string) {
	emit := func(phase, message string) {
		if onProgress != nil {
			onProgress(phase, message)
		}
	}
	emit("job_started", "Starting remote agent")
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

	// Persistent chat: build prompt from conversation history when applicable.
	convID, convErr := resolveConversation(job)
	if convErr != nil {
		log.Printf("conversation resolve: %v (proceeding stateless)", convErr)
	}

	// AI executors receive project context. Shell is a direct terminal fast path:
	// no CodeGraph init, README/AGENTS injection, or Hermes session preamble.
	executor := strings.ToLower(strings.TrimSpace(job.Executor))
	if executor == "" {
		executor = "auto"
	}
	prompt := job.Message
	agentic := executor == "shell" && strings.EqualFold(strings.TrimSpace(job.ExecutionMode), "agentic")
	useShellPreflight := executor == "shell" && (agentic || os.Getenv("NODE_AGENT_SHELL_PREFLIGHT") == "1")
	var shellContext []string
	if executor != "shell" || useShellPreflight {
		cgStatus := ensureCodegraph(ws)
		emit("codegraph_preflight", "Checking workspace structure")
		prequest := ""
		if strings.TrimSpace(job.DSHSessionID) == "" {
			prequest = readPrequest(ws, job.PrequestNote)
		}
		if executor != "shell" && (prequest != "" || cgStatus != "") {
			prompt = prequest + "\n\n[" + cgStatus + "]\n\nTask:\n" + job.Message
		}
		if useShellPreflight {
			// Shell receives context through environment, never by mutating command text.
			// ponytail: expose via NODE_AGENT_* env; add when shell tasks need repo graph without prompt injection cost.
			shellContext = []string{"NODE_AGENT_CODEGRAPH_STATUS=" + cgStatus}
			if prequest != "" {
				shellContext = append(shellContext, "NODE_AGENT_PREQUEST="+prequest)
			}
			if agentic {
				prompt = prequest + "\n\n[" + cgStatus + "]\n\nTask:\n" + job.Message
			}
		}
	}

	jobDeadline := jobTimeout()
	if agentic {
		jobDeadline = agenticJobTimeout()
		job.Message = prompt
	}
	ctx, cancel := context.WithTimeout(context.Background(), jobDeadline)
	defer cancel()
	if executor == "shell" && strings.EqualFold(strings.TrimSpace(job.ExecutionMode), "agentic") {
		return runAgenticShellJob(job, ws, ctx, emit)
	}

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
		if job.NoRTK {
			resolvedArgs = []string{shellFlag, command}
		} else {
			resolvedArgs = []string{shellFlag, rewriteShellCmd(command)}
		}
		cmd = exec.CommandContext(ctx, resolvedBin, resolvedArgs...)
		if useShellPreflight {
			cmd.Env = append(os.Environ(), shellContext...)
		}
	case "hermes":
		bin := findBin("hermes")
		if bin == "" {
			return "", false, "executor_unavailable: hermes"
		}
		resolvedBin = bin
		resolvedArgs = []string{"chat", "-q"}
		resolvedEnv = append(os.Environ(), "HERMES_WORKSPACE="+ws)
		hermesPrompt := prompt
		if convErr == nil && convID != "" {
			ctxMsgs, err := convStore.GetContext(convID, job.ContextWindow)
			if err == nil && len(ctxMsgs) > 0 {
				hermesPrompt = conversation.BuildPrompt(ctxMsgs, prompt)
			}
		}
		cmd = exec.CommandContext(ctx, bin, "chat", "-q", hermesPrompt)
		cmd.Env = resolvedEnv
	case "codex":
		bin := findBin("codex")
		if bin == "" {
			return "", false, "executor_unavailable: codex"
		}
		resolvedBin = bin
		resolvedBin = bin
		resolvedArgs = []string{"exec", "--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--color", "never"}
		cmd = exec.CommandContext(ctx, bin, append(resolvedArgs, prompt)...)
	case "dsh":
		bin := findBin("dsh")
		if bin == "" {
			return "", false, "dsh_unavailable: DeepSeek Harness (dsh) not installed or not on PATH"
		}
		if err := validateDSHBinary(bin); err != nil {
			return "", false, "dsh_unavailable: " + err.Error()
		}
		resolvedBin = bin
		resolvedArgs = deepSeekHarnessArgs(job.DSHSessionID)
		cmd = exec.CommandContext(ctx, bin, append(resolvedArgs, prompt)...)
		// launchd PATH omits Homebrew; dsh's shebang resolves node through env.
		cmd.Env = dshCommandEnv()
		if strings.TrimSpace(job.Model) != "" {
			cmd.Env = append(cmd.Env, "DSH_MODEL="+strings.TrimSpace(job.Model))
		}
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
			return runJobWithProgress(job, onProgress)
		}
		if findBin("codex") != "" {
			job.Executor = "codex"
			return runJobWithProgress(job, onProgress)
		}
		if commandCodeBin() != "" {
			job.Executor = "commandcode"
			return runJobWithProgress(job, onProgress)
		}
		return "", false, "executor_unavailable: auto found no AI executor"
	default:
		return "", false, fmt.Sprintf("unknown executor %q", executor)
	}
	emit("executor_resolved", "Resolved executor: "+executor)
	cmd.Dir = ws
	emit("process_spawned", "Starting agent process")
	out, err := streamCommand(cmd, job.TaskID)
	emit("process_exited", "Agent process finished")
	if executor == "dsh" && strings.TrimSpace(job.DSHSessionID) != "" && dshSessionWriteHandleConflict(out) {
		// Never downgrade a continuation to a cold session. The control plane
		// binds result identity to the dispatched session and must reject a new ID.
		return string(out), false, "dsh_session_conflict_retry: existing session is owned by an active write handle"
	}
	// ponytail: shell caveman gated behind NODE_AGENT_SHELL_CAVEMAN=1; compress tail only when payload >8k and LLM path available
	if executor == "shell" && os.Getenv("NODE_AGENT_SHELL_CAVEMAN") == "1" {
		out = maybeCompressShellOutput(out)
	}

	// streamCommand persists every chunk before process completion. Keep final
	// provenance/result formatting unchanged; live UI reads persisted chunks.
	sessionID, sessionCWD := parseDeepSeekHarnessSessionEvent(out)
	if executor == "dsh" && sessionID != "" && os.Getenv("NODE_AGENT_DSH_WORKSPACE_REGISTRATION") != "0" {
		if registrationErr := registerDeepSeekHarnessWorkspace(ws, sessionID); registrationErr != nil {
			return string(out), false, "dsh_workspace_registration_failed: " + registrationErr.Error()
		}
	}

	// Provenance header: first line of every result proves which binary ran.
	provenance := fmt.Sprintf("provenance executor=%s requested=%s bin=%s args=%q ws=%s",
		executor, strings.ToLower(strings.TrimSpace(job.Executor)), resolvedBin, resolvedArgs, ws)
	if sessionID != "" {
		provenance += fmt.Sprintf(" dsh_session_id=%s dsh_session_cwd=%s", sessionID, sessionCWD)
	}
	if executor == "dsh" && sessionID == "" && err == nil {
		return string(out), false, "dsh_session_missing: headless --json returned no session event"
	}
	if executor == "dsh" && sessionCWD != "" {
		canonicalWS, canonicalErr := filepath.EvalSymlinks(ws)
		canonicalCWD, cwdErr := filepath.EvalSymlinks(sessionCWD)
		if canonicalErr == nil && cwdErr == nil && canonicalWS != canonicalCWD {
			return string(out), false, fmt.Sprintf("dsh_workspace_mismatch: session cwd=%s workspace=%s", canonicalCWD, canonicalWS)
		}
	}
	proof := fmt.Sprintf("EXECUTOR_PROOF=%s", executor)
	out = append([]byte(provenance+"\n"), out...)
	out = append(out, []byte("\n"+proof+"\n"+provenance+"\n")...)

	_ = os.WriteFile(logFile, out, 0644)
	// Persist assistant response to conversation store when applicable.
	if convErr == nil && convID != "" {
		_ = convStore.AppendMessage(convID, "user", prompt)
		_ = convStore.AppendMessage(convID, "assistant", string(out))
	}
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), false, fmt.Sprintf("node_agent_job_timeout: job timed out after %s", jobTimeout())
	}
	if err != nil {
		return string(out), false, err.Error()
	}
	return string(out), true, ""
}

func dshResult(job transport.DispatchRequest, output string, ok bool, errStr string, durationMs int64) transport.ResultRequest {
	result := transport.ResultRequest{TaskID: job.TaskID, Success: ok, Output: output, Error: errStr, DurationMs: durationMs, DSHSessionID: job.DSHSessionID, DSHWorkspaceID: job.DSHWorkspaceID, LastTurnSeq: job.LastTurnSeq}
	if result.DSHWorkspaceID == "" && job.Executor == "dsh" {
		result.DSHWorkspaceID = dshWorkspaceID(job.Workspace)
	}
	if job.Executor != "dsh" {
		return result
	}
	if sessionID, _ := parseDeepSeekHarnessSessionEvent([]byte(output)); sessionID != "" {
		result.DSHSessionID = sessionID
	}
	if result.LastTurnSeq == nil {
		seq := int64(0)
		result.LastTurnSeq = &seq
	} else if ok {
		seq := *result.LastTurnSeq + 1
		result.LastTurnSeq = &seq
	}
	return result
}

func dshSessionWriteHandleConflict(output []byte) bool {
	text := strings.ToLower(string(output))
	return strings.Contains(text, "already owned by an active write handle") ||
		strings.Contains(text, "active write handle")
}

type shellPlan struct {
	Action  string `json:"action"` // run|complete|blocked|edit|create
	Command string `json:"command,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Phase   string `json:"phase,omitempty"` // discover|edit|verify
	Path    string `json:"path,omitempty"`
	OldStr  string `json:"old_str,omitempty"`
	NewStr  string `json:"new_str,omitempty"`
	Content string `json:"content,omitempty"`
}

const maxEditSize = 20 * 1024 // 20KB cap per str_replace field

// resolveInWorkspace joins a relative path to the workspace root and verifies
// it doesn't escape. Shared by edit/create actions and any future path ops.
func resolveInWorkspace(ws, relPath string) (string, error) {
	if filepath.IsAbs(relPath) {
		return "", fmt.Errorf("edit path must be relative to workspace, got absolute: %s", relPath)
	}
	absWs, err := filepath.Abs(ws)
	if err != nil {
		return "", fmt.Errorf("invalid workspace: %w", err)
	}
	target := filepath.Join(absWs, relPath)
	clean := filepath.Clean(target)
	if !strings.HasPrefix(clean, absWs+string(os.PathSeparator)) && clean != absWs {
		return "", fmt.Errorf("path escapes workspace: %s", relPath)
	}
	return clean, nil
}

func applyEdit(ws, relPath, oldStr, newStr string) (string, error) {
	if len(oldStr) > maxEditSize || len(newStr) > maxEditSize {
		return "", fmt.Errorf("old_str/new_str exceeds %d byte cap", maxEditSize)
	}
	absPath, err := resolveInWorkspace(ws, relPath)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", relPath, err)
	}
	content := string(raw)
	if newStr == "" && oldStr == content {
		return "", fmt.Errorf("refusing whole-file delete through edit action for %s", relPath)
	}

	// Detect dominant line ending; normalize for matching if CRLF.
	lineEnding := "\n"
	if strings.Count(content, "\r\n") > strings.Count(content, "\n")-strings.Count(content, "\r\n") {
		lineEnding = "\r\n"
		matchContent := strings.ReplaceAll(content, "\r\n", "\n")
		matchOld := strings.ReplaceAll(oldStr, "\r\n", "\n")
		matchNew := strings.ReplaceAll(newStr, "\r\n", "\n")
		count := strings.Count(matchContent, matchOld)
		if count == 0 {
			return "", fmt.Errorf("old_str not found in %s — file may have changed since discovery, or whitespace/line-endings don't match exactly", relPath)
		}
		if count > 1 {
			return "", fmt.Errorf("old_str matches %d times in %s — must be unique; add more surrounding context", count, relPath)
		}
		updated := strings.Replace(matchContent, matchOld, matchNew, 1)
		updated = strings.ReplaceAll(updated, "\n", lineEnding)
		if err := os.WriteFile(absPath, []byte(updated), 0644); err != nil {
			return "", fmt.Errorf("write %s: %w", relPath, err)
		}
		return fmt.Sprintf("edited %s (%d bytes -> %d bytes)", relPath, len(content), len(updated)), nil
	}

	count := strings.Count(content, oldStr)
	if count == 0 {
		return "", fmt.Errorf("old_str not found in %s — file may have changed since discovery, or whitespace/line-endings don't match exactly", relPath)
	}
	if count > 1 {
		return "", fmt.Errorf("old_str matches %d times in %s — must be unique; add more surrounding context", count, relPath)
	}
	updated := strings.Replace(content, oldStr, newStr, 1)
	if err := os.WriteFile(absPath, []byte(updated), 0644); err != nil {
		return "", fmt.Errorf("write %s: %w", relPath, err)
	}
	return fmt.Sprintf("edited %s (%d bytes -> %d bytes)", relPath, len(content), len(updated)), nil
}

func applyCreate(ws, relPath, content string) (string, error) {
	if len(content) > maxEditSize {
		return "", fmt.Errorf("content exceeds %d byte cap", maxEditSize)
	}
	absPath, err := resolveInWorkspace(ws, relPath)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(absPath); err == nil {
		return "", fmt.Errorf("file already exists: %s — use edit action with old_str for existing files", relPath)
	}
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dir, err)
	}
	if err := os.WriteFile(absPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write %s: %w", relPath, err)
	}
	return fmt.Sprintf("created %s (%d bytes)", relPath, len(content)), nil
}

const maxShellAgentIterations = 24

// runAgenticShellJob gives the planner read-only workspace access and keeps all
// mutations on the explicit shell path. This preserves shell provenance while
// allowing the orchestrator to inspect, edit, test, and recover iteratively.
func runAgenticShellJob(job transport.DispatchRequest, ws string, ctx context.Context, emit func(string, string)) (string, bool, string) {
	max := job.MaxIterations
	if max <= 0 {
		max = 6
	}
	if max > maxShellAgentIterations {
		max = maxShellAgentIterations
	}
	iterations := 0
	discoveryBudget := max / 4
	if discoveryBudget < 3 {
		discoveryBudget = 3
	}
	lastCommand := ""
	repeatedCommandCount := 0
	commandCounts := map[string]int{}
	finish := func(out string, ok bool, errText string) (string, bool, string) {
		proof := fmt.Sprintf("provenance executor=shell requested=shell mode=agentic ws=%s iterations=%d", ws, iterations)
		if out == "" {
			out = proof
		} else {
			out = proof + "\n" + out
		}
		return out, ok, errText
	}
	transcript := strings.Builder{}
	transcript.WriteString("Task intent:\n" + job.Message + "\n")
	if job.Acceptance != "" {
		transcript.WriteString("Acceptance:\n" + job.Acceptance + "\n")
	}
	var lastOutput string
	for i := 1; i <= max; i++ {
		iterations = i
		if err := ctx.Err(); err != nil {
			return finish(lastOutput, false, "agentic shell timeout")
		}
		emit("shell_planning", fmt.Sprintf("Planning shell iteration %d/%d", i, max))
		directive := fmt.Sprintf("Execution controller: discovery has a budget of %d iteration(s). After discovery, move to edit and then verify. Current iteration is %d/%d.", discoveryBudget, i, max)
		if i > discoveryBudget {
			directive += " Discovery budget is exhausted: the next command MUST inspect a target file, edit the requested behavior, or run a focused verification command; do not repeat repository discovery."
		}
		if lastCommand != "" {
			directive += " The immediately previous command was already executed; choose a different command that advances the task: " + lastCommand
		}
		plan, err := planShellCommand(ctx, ws, transcript.String()+"\n"+directive, lastOutput)
		if err != nil {
			return finish(lastOutput, false, "shell planner: "+err.Error())
		}
		plan.Action = strings.ToLower(strings.TrimSpace(plan.Action))
		if plan.Action == "complete" {
			if i == 1 {
				return finish(lastOutput, false, "shell planner completed without executing a command")
			}
			transcript.WriteString("Decision: complete — " + plan.Reason + "\n")
			return finish(lastOutput, true, "")
		}
		if plan.Action == "blocked" {
			return finish(lastOutput, false, "shell planner blocked: "+plan.Reason)
		}
		phase := plan.Phase
		if phase == "" {
			phase = "discover"
		}
		var out string
		var ok bool
		var errText string
		actionLabel := plan.Action
		switch plan.Action {
		case "run":
			if strings.TrimSpace(plan.Command) == "" {
				return finish(lastOutput, false, "shell planner returned an invalid run decision")
			}
			if err := validateShellCommand(plan.Command, ws); err != nil {
				emit("shell_blocked", fmt.Sprintf("Iteration %d blocked: %v", i, err))
				return finish(lastOutput, false, "shell planner proposed a blocked command: "+err.Error())
			}
			if strings.TrimSpace(plan.Command) == lastCommand {
				repeatedCommandCount++
			} else {
				repeatedCommandCount = 0
			}
			lastCommand = strings.TrimSpace(plan.Command)
			commandCounts[lastCommand]++
			if repeatedCommandCount >= 2 || commandCounts[lastCommand] >= 3 {
				return finish(lastOutput, false, fmt.Sprintf("shell planner repeated command without progress: %s", plan.Command))
			}
			if i > discoveryBudget && phase == "discover" && looksLikeDiscoveryCommand(plan.Command) {
				return finish(lastOutput, false, fmt.Sprintf("planner_discovery_budget_exhausted: discovery exceeded %d iterations; next step must be edit or verify", discoveryBudget))
			}
			emit("shell_command", fmt.Sprintf("Iteration %d [%s]: %s", i, phase, plan.Command))
			out, ok, errText = executeAgenticShell(ctx, job, ws, plan.Command, emit)
		case "edit":
			emit("shell_edit", fmt.Sprintf("Iteration %d [edit]: %s", i, plan.Path))
			result, err := applyEdit(ws, plan.Path, plan.OldStr, plan.NewStr)
			out, ok, errText = result, err == nil, ""
			if err != nil {
				errText = err.Error()
				out = "edit failed: " + errText
			}
		case "create":
			emit("shell_create", fmt.Sprintf("Iteration %d [create]: %s", i, plan.Path))
			result, err := applyCreate(ws, plan.Path, plan.Content)
			out, ok, errText = result, err == nil, ""
			if err != nil {
				errText = err.Error()
				out = "create failed: " + errText
			}
		default:
			return finish(lastOutput, false, "shell planner returned an invalid decision")
		}
		lastOutput = out
		plannerOutput := maybeCompressShellOutput([]byte(out))
		transcript.WriteString(fmt.Sprintf("Iteration %d phase: %s\nAction: %s\nCommand: %s\nPath: %s\nExit success: %t\nOutput:\n%s\n", i, phase, actionLabel, plan.Command, plan.Path, ok, trimPlannerOutput(string(plannerOutput))))
		// Keep raw worker output in streamCommand/log history; only planner context gets compacted.
		if errText != "" {
			transcript.WriteString("Error: " + errText + "\n")
		}
		if !ok && i == max {
			return finish(out, false, "shell agent exhausted iterations: "+errText)
		}
	}
	return finish(lastOutput, false, "shell agent exhausted iterations")
}

func looksLikeDiscoveryCommand(command string) bool {
	first := strings.ToLower(strings.TrimSpace(command))
	for _, name := range []string{"rg ", "rg\t", "find ", "fd ", "ls ", "sed ", "head ", "tail ", "awk "} {
		if strings.HasPrefix(first, name) {
			return true
		}
	}
	return first == "rg" || first == "find" || first == "fd" || first == "ls" || first == "sed" || first == "head" || first == "tail" || first == "awk"
}

func planShellCommand(ctx context.Context, ws, transcript, lastOutput string) (shellPlan, error) {
	prompt := `You are a shell task planner. Do not execute commands yourself or use tools. Choose the next single safe action for the worker. The workspace is the command working directory; do not inspect sibling projects or parent-repository paths. Keep discovery bounded: exclude .git, vendor, node_modules, build, dist, cache, and generated directories, and cap listings/searches with head or a focused path. Once a target file has been found, STOP using rg, find, fd, ls, or repository-wide searches.

For FILE EDITS, prefer the native "edit" action over sed/awk/shell — it avoids cross-platform quoting issues entirely. For NEW FILES, use "create". For discovery, tests, and verification, use "run".

Return ONLY one valid JSON object matching one of these shapes:
{"action":"run","command":"...","reason":"...","phase":"discover|edit|verify"}
{"action":"edit","path":"relative/path.ext","old_str":"exact text to find","new_str":"replacement text","reason":"..."}
{"action":"create","path":"relative/path.ext","content":"full file content","reason":"..."}
{"action":"complete","reason":"..."}
{"action":"blocked","reason":"..."}

Rules:
- edit: path must be relative to workspace. old_str must match exactly once in the file (include enough surrounding context for uniqueness). No regex, no shell escaping.
- create: path must be relative. Fails if file already exists.
- run: use only for discovery, testing, and verification — never for file edits.
- complete: only when acceptance criteria are demonstrably satisfied with evidence.
- blocked: only when no safe next step exists.
` + transcript
	if lastOutput != "" {
		prompt += "\nLatest worker output:\n" + trimPlannerOutput(lastOutput)
	}
	bin := findBin("codex")
	if bin == "" {
		return shellPlan{}, fmt.Errorf("read-only planner unavailable: codex not found")
	}
	cmd := exec.CommandContext(ctx, bin, "exec", "--sandbox", "read-only", "--skip-git-repo-check", "--ignore-user-config", "--ignore-rules", "--ephemeral", "--color", "never", prompt)
	cmd.Dir = ws
	out, err := cmd.CombinedOutput()
	if err != nil {
		// CombinedOutput keeps the real planner failure (quota, model,
		// auth) instead of a bare "exit status 1" retried forever.
		msg := strings.TrimSpace(string(out))
		if len(msg) > 400 {
			msg = msg[len(msg)-400:]
		}
		if msg == "" {
			msg = err.Error()
		}
		return shellPlan{}, fmt.Errorf("codex planner failed: %s", msg)
	}
	plan, err := parseShellPlan(string(out))
	if err == nil {
		return plan, nil
	}
	// One bounded repair call prevents malformed formatting from consuming the
	// entire execution budget while keeping the primary planner prompt small.
	repairPrompt := fmt.Sprintf("Return only one valid JSON object for this shell plan. No Markdown and no backslashes before JSON quotes. The action value MUST be exactly one of run, complete, or blocked (never the literal string run|complete|blocked). The phase value MUST be exactly one of discover, edit, or verify. For example: {\"action\":\"run\",\"command\":\"sed -n '1,120p' app/file.php\",\"reason\":\"inspect target\",\"phase\":\"discover\"}. Parse error: %s", err)
	repair := exec.CommandContext(ctx, bin, "exec", "--sandbox", "read-only", "--skip-git-repo-check", "--ignore-user-config", "--ignore-rules", "--ephemeral", "--color", "never", repairPrompt+"\nOriginal response:\n"+trimPlannerOutput(string(out)))
	repair.Dir = ws
	repaired, repairErr := repair.Output()
	if repairErr != nil {
		return shellPlan{}, fmt.Errorf("planner_json_invalid: %v", err)
	}
	plan, parseErr := parseShellPlan(string(repaired))
	if parseErr != nil {
		return shellPlan{}, fmt.Errorf("planner_json_invalid: %v", parseErr)
	}
	return plan, nil
}

func parseShellPlan(raw string) (shellPlan, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "```") {
		lines := strings.Split(raw, "\n")
		if len(lines) >= 2 {
			raw = strings.TrimSpace(strings.Join(lines[1:len(lines)-1], "\n"))
		}
	}
	object, err := firstJSONObject(raw)
	if err != nil {
		// Some models echo an escaped JSON object. Normalize only after the
		// strict parse path fails; command contents remain schema-validated.
		object, err = firstJSONObject(strings.ReplaceAll(raw, `\"`, `"`))
		if err != nil {
			return shellPlan{}, err
		}
	}
	var plan shellPlan
	if err := json.Unmarshal([]byte(object), &plan); err != nil {
		return shellPlan{}, err
	}
	plan.Action = strings.ToLower(strings.TrimSpace(plan.Action))
	plan.Phase = strings.ToLower(strings.TrimSpace(plan.Phase))
	if plan.Action != "run" && plan.Action != "complete" && plan.Action != "blocked" && plan.Action != "edit" && plan.Action != "create" {
		return shellPlan{}, fmt.Errorf("invalid planner action %q", plan.Action)
	}
	if plan.Action == "run" && strings.TrimSpace(plan.Command) == "" {
		return shellPlan{}, fmt.Errorf("run action requires command")
	}
	if plan.Action == "edit" {
		if strings.TrimSpace(plan.Path) == "" {
			return shellPlan{}, fmt.Errorf("edit action requires path")
		}
		if plan.OldStr == "" {
			return shellPlan{}, fmt.Errorf("edit action requires old_str")
		}
	}
	if plan.Action == "create" {
		if strings.TrimSpace(plan.Path) == "" {
			return shellPlan{}, fmt.Errorf("create action requires path")
		}
		if plan.Content == "" {
			return shellPlan{}, fmt.Errorf("create action requires content")
		}
	}
	if plan.Phase != "" && plan.Phase != "discover" && plan.Phase != "edit" && plan.Phase != "verify" {
		return shellPlan{}, fmt.Errorf("invalid planner phase %q", plan.Phase)
	}
	return plan, nil
}

func firstJSONObject(raw string) (string, error) {
	start, depth := -1, 0
	quoted, escaped := false, false
	for i, r := range raw {
		if quoted {
			if escaped {
				escaped = false
			} else if r == '\\' {
				escaped = true
			} else if r == '"' {
				quoted = false
			}
			continue
		}
		if r == '"' {
			quoted = true
			continue
		}
		if r == '{' {
			if depth == 0 {
				start = i
			}
			depth++
		} else if r == '}' && depth > 0 {
			depth--
			if depth == 0 {
				return raw[start : i+1], nil
			}
		}
	}
	return "", fmt.Errorf("planner did not return a complete JSON object")
}

func executeAgenticShell(ctx context.Context, job transport.DispatchRequest, ws, command string, emit func(string, string)) (string, bool, string) {
	shell, flag := "bash", "-lc"
	if runtime.GOOS == "windows" {
		shell, flag = "cmd", "/c"
	}
	rewritten := rewriteShellCmd(command)
	cmd := exec.CommandContext(ctx, shell, flag, rewritten)
	cmd.Dir = ws
	out, err := streamCommand(cmd, job.TaskID)
	if err != nil {
		return string(out), false, err.Error()
	}
	return string(out), true, ""
}

func trimPlannerOutput(raw string) string {
	if len(raw) > 4096 {
		// Keep both declarations/context at the top and payload/save logic at
		// the bottom. Keeping only the head caused the planner to repeat the
		// same sed command because it never saw the relevant lower section.
		const half = 2048
		return raw[:half] + "\n…[middle truncated]…\n" + raw[len(raw)-half:]
	}
	return raw
}

func unsafeShellCommand(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	for _, needle := range []string{"rm -rf", "git reset --hard", "git clean -fd", "git push --force", "git push -f", "sudo ", "chmod -r 777", "mkfs", "dd if=", "drop database", "| sh", "| bash"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func validateShellCommand(command, workspace string) error {
	if unsafeShellCommand(command) {
		return fmt.Errorf("destructive command blocked")
	}
	abs, err := filepath.Abs(workspace)
	if err != nil {
		return fmt.Errorf("invalid workspace: %w", err)
	}
	for _, token := range strings.Fields(command) {
		token = strings.Trim(token, "'\"()[]{};,:")
		// Slash-delimited search/awk regexes such as /item|description|unit/
		// are expressions, not filesystem paths.
		if strings.ContainsAny(token, "|{}") {
			continue
		}
		// Slash-delimited search/awk regexes such as /[.]js$/ or /^api\// are
		// expressions, not filesystem paths. Abs check would treat them as
		// absolute paths and block them. Real paths never contain regex chars.
		if strings.HasPrefix(token, "/") && strings.ContainsAny(token, "[]$*+?^\\") {
			continue
		}
		if token == ".." || strings.HasPrefix(token, "../") || strings.Contains(token, "/../") {
			return fmt.Errorf("path traversal blocked: %s", token)
		}
		if !filepath.IsAbs(token) || strings.HasPrefix(token, abs+string(os.PathSeparator)) || token == abs {
			continue
		}
		if strings.HasPrefix(token, "/usr/") || strings.HasPrefix(token, "/bin/") || strings.HasPrefix(token, "/opt/") {
			continue
		}
		return fmt.Errorf("path outside workspace: %s", token)
	}
	return nil
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
		{"dsh", []string{"dsh"}},
		{"commandcode", []string{"cmd", "cmdc", "command-code"}},
	}
	var out []string
	versions := map[string]string{}
	for _, c := range checks {
		if bin := findBinAny(c.bins...); bin != "" {
			out = append(out, c.name)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			cmd := exec.CommandContext(ctx, bin, "--version")
			if c.name == "dsh" {
				cmd.Env = dshCommandEnv()
			}
			b, _ := cmd.CombinedOutput()
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
func maybeCompressShellOutput(raw []byte) []byte {
	if len(raw) <= 8192 {
		return raw
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "rtk", "pipe", "--ultra-compact")
	cmd.Stdin = bytes.NewReader(raw)
	compressed, err := cmd.Output()
	if err != nil || ctx.Err() != nil || len(compressed) == 0 || len(compressed) >= len(raw) {
		return raw
	}
	return compressed
}

func rewriteShellCmd(raw string) string {
	if os.Getenv("NODE_AGENT_NO_RTK") == "1" {
		return raw
	}
	// RTK's generic rewrite can turn discovery commands such as `rg --files`
	// into incompatible `grep` invocations on macOS. Preserve their semantics;
	// their output is bounded separately before it reaches planner context.
	trimmed := strings.TrimSpace(raw)
	for _, prefix := range []string{"rg", "fd", "find"} {
		if trimmed == prefix || strings.HasPrefix(trimmed, prefix+" ") {
			return raw
		}
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

func validateDSHBinary(bin string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	// launchd starts the worker with a minimal PATH. dsh is a Node launcher,
	// so its health check must use the same Homebrew PATH as the real run.
	cmd.Env = dshCommandEnv()
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		if dshWebAvailable() {
			// Some DSH versions boot the plugin tree even for --version and can
			// exceed this probe timeout while the already-running web profile is
			// healthy. Let the real bounded headless invocation decide instead.
			return nil
		}
		return fmt.Errorf("binary health check timed out (DSH Web/Harness may be unavailable)")
	}
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("binary health check failed: %s", msg)
	}
	return nil
}

func dshWebAvailable() bool {
	url := strings.TrimSpace(os.Getenv("NODE_AGENT_DSH_WEB_URL"))
	if url == "" {
		url = "http://127.0.0.1:3080/"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Timeout: 1500 * time.Millisecond}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 500
}

func dshCommandEnv() []string {
	return append(os.Environ(), "PATH=/opt/homebrew/bin:/usr/local/bin:"+os.Getenv("PATH"))
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

func streamCommand(cmd *exec.Cmd, taskID string) ([]byte, error) {
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stdoutCh := make(chan []byte, 64)
	stderrCh := make(chan []byte, 64)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				stdoutCh <- b
			}
			if err != nil {
				close(stdoutCh)
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				stderrCh <- b
			}
			if err != nil {
				close(stderrCh)
				return
			}
		}
	}()
	var out []byte
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	var pending []byte
	flush := func() {
		if len(pending) == 0 {
			return
		}
		_ = postJSON(nodeAgentBase()+"/api/nodes/progress", transport.ProgressRequest{TaskID: taskID, Chunk: string(pending)})
		pending = nil
	}
	stdoutOpen, stderrOpen := true, true
	for stdoutOpen || stderrOpen || pending != nil {
		select {
		case chunk, ok := <-stdoutCh:
			if !ok {
				stdoutOpen = false
				continue
			}
			out = append(out, chunk...)
			pending = append(pending, chunk...)
		case chunk, ok := <-stderrCh:
			if !ok {
				stderrOpen = false
				continue
			}
			out = append(out, chunk...)
			pending = append(pending, chunk...)
		case <-ticker.C:
			flush()
		}
	}
	flush()
	waitErr := cmd.Wait()
	return out, waitErr
}

func dshWorkspaceID(workspacePath string) string {
	canonicalPath, err := filepath.EvalSymlinks(workspacePath)
	if err != nil {
		return ""
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(home, ".dsh", "storages", "workspace.json"))
	if err != nil {
		return ""
	}
	var storage dshWorkspaceStorage
	if json.Unmarshal(raw, &storage) != nil {
		return ""
	}
	for id, workspace := range storage.Tables.Workspaces {
		storedPath, pathErr := filepath.EvalSymlinks(workspace.Path)
		if pathErr == nil && storedPath == canonicalPath {
			return id
		}
	}
	return ""
}

func registerDeepSeekHarnessWorkspace(workspacePath, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(workspacePath) == "" {
		return fmt.Errorf("dsh workspace registration requires workspace and session")
	}
	canonicalPath, err := filepath.EvalSymlinks(workspacePath)
	if err != nil {
		return fmt.Errorf("canonicalize workspace: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	path := filepath.Join(home, ".dsh", "storages", "workspace.json")
	dshWorkspaceMu.Lock()
	defer dshWorkspaceMu.Unlock()
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read workspace registry: %w", err)
	}
	var storage dshWorkspaceStorage
	if err := json.Unmarshal(raw, &storage); err != nil {
		return fmt.Errorf("decode workspace registry: %w", err)
	}
	if storage.Tables.Workspaces == nil {
		storage.Tables.Workspaces = map[string]dshWorkspace{}
	}
	var workspaceID string
	for id, workspace := range storage.Tables.Workspaces {
		storedPath, pathErr := filepath.EvalSymlinks(workspace.Path)
		if pathErr == nil && storedPath == canonicalPath {
			workspaceID = id
			if !containsString(workspace.SessionIDs, sessionID) {
				workspace.SessionIDs = append([]string{sessionID}, workspace.SessionIDs...)
			}
			workspace.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			storage.Tables.Workspaces[id] = workspace
			break
		}
	}
	if workspaceID == "" {
		workspaceID, err = newUUID()
		if err != nil {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		storage.Tables.Workspaces[workspaceID] = dshWorkspace{
			Path: canonicalPath, Title: filepath.Base(canonicalPath),
			SessionIDs: []string{sessionID}, CreatedAt: now, UpdatedAt: now,
		}
		ids, _ := storage.Global["workspaceIds"].([]any)
		storage.Global["workspaceIds"] = append([]any{workspaceID}, ids...)
	}
	updated, err := json.MarshalIndent(storage, "", "  ")
	if err != nil {
		return fmt.Errorf("encode workspace registry: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(updated, '\n'), 0644); err != nil {
		return fmt.Errorf("write workspace registry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit workspace registry: %w", err)
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16])), nil
}

func deepSeekHarnessArgs(sessionID string) []string {
	args := []string{"--profile", "headless", "--json"}
	if strings.TrimSpace(sessionID) != "" {
		args = append(args, "--session-id", strings.TrimSpace(sessionID))
	}
	return args
}

var dshSessionEventRE = regexp.MustCompile(`\"type\"\s*:\s*\"session\"[^{\n}]*\"sessionId\"\s*:\s*\"([^\"]+)\"(?:[^{}\n]*\"cwd\"\s*:\s*\"([^\"]*)\")?`)

func parseDeepSeekHarnessSessionEvent(raw []byte) (string, string) {
	text := string(raw)
	for _, line := range strings.Split(text, "\n") {
		var event struct {
			Type      string `json:"type"`
			SessionID string `json:"sessionId"`
			CWD       string `json:"cwd"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(line)), &event) == nil && event.Type == "session" && event.SessionID != "" {
			return event.SessionID, event.CWD
		}
	}
	if match := dshSessionEventRE.FindStringSubmatch(text); len(match) > 1 {
		return match[1], match[2]
	}
	return "", ""
}

func nodeAgentBase() string {
	if v := os.Getenv("NODE_AGENT_SERVER"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://100.64.0.1:8788"
}
