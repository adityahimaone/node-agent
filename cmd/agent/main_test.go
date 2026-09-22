package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"node-agent/internal/transport"
)

func mustTempDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "probe.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

// TDD red: shell executor must be provenance-verifiable and must NOT run hermes-style preflight.
func TestDeepSeekHarnessArgsCreateJSONSession(t *testing.T) {
	args := deepSeekHarnessArgs("")
	want := []string{"--profile", "headless", "--json"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("args=%v want=%v", args, want)
	}
	args = deepSeekHarnessArgs("session-existing")
	want = []string{"--profile", "headless", "--json", "--session-id", "session-existing"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("resume args=%v want=%v", args, want)
	}
}

func TestParseDeepSeekHarnessSessionEvent(t *testing.T) {
	id, cwd := parseDeepSeekHarnessSessionEvent([]byte(`{"type":"session","sessionId":"session-123","cwd":"/workspace"}`))
	if id != "session-123" || cwd != "/workspace" {
		t.Fatalf("got id=%q cwd=%q", id, cwd)
	}
}

func TestRegisterDeepSeekHarnessWorkspaceCreatesAndReusesByCanonicalPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	ws := mustTempDir(t)
	registry := filepath.Join(home, ".dsh", "storages")
	if err := os.MkdirAll(registry, 0o755); err != nil {
		t.Fatal(err)
	}
	initial := `{"unit":{"name":"workspace","version":2},"global":{"initialized":true,"workspaceIds":[],"archivedSessionIds":[]},"tables":{"workspaces":{}}}`
	if err := os.WriteFile(filepath.Join(registry, "workspace.json"), []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := registerDeepSeekHarnessWorkspace(ws, "session-1"); err != nil {
		t.Fatal(err)
	}
	if err := registerDeepSeekHarnessWorkspace(filepath.Join(ws, "."), "session-2"); err != nil {
		t.Fatal(err)
	}
	var storage dshWorkspaceStorage
	raw, err := os.ReadFile(filepath.Join(registry, "workspace.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &storage); err != nil {
		t.Fatal(err)
	}
	if len(storage.Tables.Workspaces) != 1 {
		t.Fatalf("workspace count=%d", len(storage.Tables.Workspaces))
	}
	for _, workspace := range storage.Tables.Workspaces {
		if workspace.Title != filepath.Base(ws) || len(workspace.SessionIDs) != 2 {
			t.Fatalf("workspace=%+v", workspace)
		}
	}
}

func TestRegisterDeepSeekHarnessWorkspaceRejectsMissingRegistry(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := registerDeepSeekHarnessWorkspace(mustTempDir(t), "session-1"); err == nil {
		t.Fatal("expected missing registry error")
	}
}

func TestValidateDSHBinary(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "dsh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho dsh-test-version\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateDSHBinary(bin); err != nil {
		t.Fatalf("expected healthy dsh binary: %v", err)
	}
}

func TestValidateDSHBinaryRejectsFailingBinary(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "dsh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho dsh-web-off >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateDSHBinary(bin); err == nil || !strings.Contains(err.Error(), "binary health check failed") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestRunJobDeepSeekHarnessResumesExistingSession(t *testing.T) {
	ws := mustTempDir(t)
	binDir := t.TempDir()
	bin := filepath.Join(binDir, "dsh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '{\"type\":\"session\",\"sessionId\":\"session-existing\",\"cwd\":\"%s\"}\\n' \"$PWD\"\nprintf 'ARGS=%s\\n' \"$*\""), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	out, ok, errStr := runJob(transport.DispatchRequest{TaskID: "t-dsh-resume", Workspace: ws, Executor: "dsh", DSHSessionID: "session-existing", Message: "continue task"})
	if !ok {
		t.Fatalf("dsh resume failed err=%q out=%.500s", errStr, out)
	}
	if !strings.Contains(out, "ARGS=--profile headless --json --session-id session-existing") {
		t.Fatalf("existing session was not passed to dsh, out=%.500s", out)
	}
}

func TestRunJobDeepSeekHarnessUsesHeadlessProfile(t *testing.T) {
	ws := mustTempDir(t)
	binDir := t.TempDir()
	bin := filepath.Join(binDir, "dsh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '{\"type\":\"session\",\"sessionId\":\"session-test\",\"cwd\":\"%s\"}\\n' \"$PWD\"\nprintf 'DSH_EXECUTOR_PROOF=%s\\n' \"$1 $2\""), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	out, ok, errStr := runJob(transport.DispatchRequest{TaskID: "t-dsh-proof", Workspace: ws, Executor: "dsh", Message: "test task"})
	if !ok {
		t.Fatalf("dsh failed err=%q out=%.500s", errStr, out)
	}
	if !strings.Contains(out, "args=[\"--profile\" \"headless\" \"--json\"]") {
		t.Fatalf("dsh profile args missing, out=%.500s", out)
	}
	if !strings.Contains(out, "provenance executor=dsh") {
		t.Fatalf("dsh provenance missing, out=%.500s", out)
	}
}

func TestRunJobDeepSeekHarnessPassesSelectedModel(t *testing.T) {
	ws := mustTempDir(t)
	binDir := t.TempDir()
	bin := filepath.Join(binDir, "dsh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '{\"type\":\"session\",\"sessionId\":\"session-model\",\"cwd\":\"%s\"}\\n' \"$PWD\"\nprintf 'MODEL=%s\\n' \"$DSH_MODEL\""), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	out, ok, errStr := runJob(transport.DispatchRequest{TaskID: "t-dsh-model", Workspace: ws, Executor: "dsh", Model: "nvidia/deepseek-ai/deepseek-v4-flash", Message: "test task"})
	if !ok {
		t.Fatalf("dsh failed err=%q out=%.500s", errStr, out)
	}
	if !strings.Contains(out, "MODEL=nvidia/deepseek-ai/deepseek-v4-flash") {
		t.Fatalf("selected model missing, out=%.500s", out)
	}
}

func TestRunJobShellProvenance(t *testing.T) {
	ws := mustTempDir(t)
	_ = os.WriteFile(filepath.Join(ws, "AGENTS.md"), []byte("agentish preamble that must not leak into shell"), 0o644)
	out, ok, errStr := runJob(transport.DispatchRequest{
		TaskID:    "t-shell-proof",
		Board:     "default",
		Workspace: ws,
		Executor:  "shell",
		Command:   "printf 'EXECUTOR_PROOF=shell ws='; pwd",
	})
	if !ok {
		t.Fatalf("shell failed err=%q out=%.500s", errStr, out)
	}
	if !strings.Contains(out, "EXECUTOR_PROOF=shell") {
		t.Fatalf("shell provenance missing, out=%.500s", out)
	}
	if strings.Contains(out, "agentish preamble") {
		t.Fatalf("shell leaked AGENTS.md preamble")
	}
	if !strings.Contains(out, ws) {
		t.Fatalf("shell not run in workspace dir, out=%.500s", out)
	}
	if !strings.Contains(out, "provenance executor=shell") {
		t.Fatalf("shell audit header missing, out=%.500s", out)
	}
}

func TestRunJobShellFastPathDoesNotLeakHermesPreamble(t *testing.T) {
	ws := mustTempDir(t)
	out, ok, errStr := runJob(transport.DispatchRequest{
		TaskID:       "t-shell-prequest",
		Board:        "default",
		Workspace:    ws,
		Executor:     "shell",
		Command:      `echo EXECUTOR_PROOF=shell`,
		PrequestNote: "this must not appear in shell output",
	})
	if !ok {
		t.Fatalf("shell prequest test failed err=%q out=%.500s", errStr, out)
	}
	if strings.Contains(out, "this must not appear") {
		t.Fatalf("leaked prequest into shell")
	}
}

func TestRunJobShellPreflightUsesEnvironment(t *testing.T) {
	ws := mustTempDir(t)
	_ = os.WriteFile(filepath.Join(ws, "README.md"), []byte("shell preflight context"), 0o644)
	t.Setenv("NODE_AGENT_SHELL_PREFLIGHT", "1")
	t.Setenv("NODE_AGENT_NO_RTK", "1")
	out, ok, errStr := runJob(transport.DispatchRequest{
		TaskID:    "t-shell-preflight",
		Workspace: ws,
		Executor:  "shell",
		Command:   "printf '%s\\n' \"$NODE_AGENT_CODEGRAPH_STATUS\" \"$NODE_AGENT_PREQUEST\"",
	})
	if !ok {
		t.Fatalf("shell preflight failed err=%q out=%.500s", errStr, out)
	}
	if !strings.Contains(out, "shell preflight context") || !strings.Contains(strings.ToLower(out), "codegraph") {
		t.Fatalf("shell preflight env missing, out=%.500s", out)
	}
}

func TestRewriteShellCmd(t *testing.T) {
	cases := []struct{ name, in string }{
		{"known git", "git status"},
		{"known compound", "git add -A && git commit -m x"},
		{"known pipe", "git status | head -5"},
		{"no equivalent", "echo hello"},
		{"multiline", "for i in 1 2 3\ndo echo $i\ndone"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := rewriteShellCmd(c.in)
			if got == "" {
				t.Fatalf("rewriteShellCmd(%q) = empty, want non-empty", c.in)
			}
			if c.name == "known git" && got != "rtk git status" {
				t.Fatalf("known git: got %q, want %q", got, "rtk git status")
			}
			if c.name == "no equivalent" && got != c.in {
				t.Fatalf("no equivalent: got %q, want raw %q", got, c.in)
			}
		})
	}
}

func TestRewriteShellCmdDisabled(t *testing.T) {
	t.Setenv("NODE_AGENT_NO_RTK", "1")
	if got := rewriteShellCmd("git status"); got != "git status" {
		t.Fatalf("disabled: got %q, want raw", got)
	}
}

func TestRewriteShellCmdPreservesDiscoverySemantics(t *testing.T) {
	for _, command := range []string{"rg --files", "fd -t f", "find . -type f"} {
		if got := rewriteShellCmd(command); got != command {
			t.Fatalf("rewriteShellCmd(%q) = %q, want original command", command, got)
		}
	}
}

func TestParseShellPlan(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"normal", `{"action":"run","command":"printf ok","reason":"inspect","phase":"discover"}`, "run"},
		{"fenced", "```json\n{\"action\":\"complete\",\"reason\":\"tests pass\",\"phase\":\"verify\"}\n```", "complete"},
		{"prose", `Here is the plan: {"action":"run","command":"go test ./...","reason":"verify","phase":"verify"}`, "run"},
		{"escaped", `{\"action\":\"run\",\"command\":\"printf ok\",\"reason\":\"inspect\",\"phase\":\"discover\"}`, "run"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseShellPlan(tt.raw)
			if err != nil {
				t.Fatalf("parseShellPlan() error = %v", err)
			}
			if got.Action != tt.want {
				t.Fatalf("action = %q, want %q", got.Action, tt.want)
			}
		})
	}
}

func TestParseShellPlanRejectsInvalidAction(t *testing.T) {
	if _, err := parseShellPlan(`{"action":"delete","command":"echo no","reason":"bad","phase":"edit"}`); err == nil {
		t.Fatal("expected invalid action error")
	}
}

func TestValidateShellCommand(t *testing.T) {
	ws := "/tmp/workspace"
	if err := validateShellCommand("rm -rf .", ws); err == nil {
		t.Fatal("expected destructive command rejection")
	}
	if err := validateShellCommand("cat /etc/passwd", ws); err == nil {
		t.Fatal("expected outside-workspace rejection")
	}
	if err := validateShellCommand("printf ok > /tmp/workspace/out.txt", ws); err != nil {
		t.Fatalf("workspace command rejected: %v", err)
	}
	// Regex patterns like /[.]js$/ are expressions, not filesystem paths.
	if err := validateShellCommand("rg /[.]js$/ src/", ws); err != nil {
		t.Fatalf("regex pattern rejected as path: %v", err)
	}
	if err := validateShellCommand("find . -name /^api\\//", ws); err != nil {
		t.Fatalf("regex pattern with caret rejected: %v", err)
	}
}

func TestLooksLikeDiscoveryCommand(t *testing.T) {
	if !looksLikeDiscoveryCommand("sed -n '1,80p' app/file.php") {
		t.Fatal("sed inspection should count as discovery")
	}
	if looksLikeDiscoveryCommand("python3 -c 'print(1)'") {
		t.Fatal("edit/verification command should not count as discovery")
	}
}

func TestApplyEditZeroMatchesFails(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("hello world"), 0o644)
	_, err := applyEdit(ws, "f.txt", "does not exist", "x")
	if err == nil || !strings.Contains(err.Error(), "old_str not found") {
		t.Fatalf("want old_str not found error, got %v", err)
	}
	raw, _ := os.ReadFile(p)
	if string(raw) != "hello world" {
		t.Fatalf("file mutated on zero match: %q", raw)
	}
}

func TestApplyEditMultipleMatchesFails(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("a a a"), 0o644)
	_, err := applyEdit(ws, "f.txt", "a", "b")
	if err == nil || !strings.Contains(err.Error(), "matches 3 times") {
		t.Fatalf("want ambiguous match error, got %v", err)
	}
}

func TestApplyEditSingleMatchSucceeds(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("hello world foo"), 0o644)
	msg, err := applyEdit(ws, "f.txt", "world", "there")
	if err != nil {
		t.Fatalf("applyEdit: %v", err)
	}
	raw, _ := os.ReadFile(p)
	if string(raw) != "hello there foo" {
		t.Fatalf("wrong replacement: %q", raw)
	}
	if !strings.Contains(msg, "edited f.txt") {
		t.Fatalf("bad success msg: %q", msg)
	}
}

func TestApplyEditRejectsOutsideWorkspace(t *testing.T) {
	ws := t.TempDir()
	_, err := applyEdit(ws, "../evil.txt", "x", "y")
	if err == nil || !strings.Contains(err.Error(), "escapes workspace") {
		t.Fatalf("want escapes workspace error, got %v", err)
	}
	if _, err := applyEdit(ws, "/etc/hosts", "x", "y"); err == nil || !strings.Contains(err.Error(), "must be relative") {
		t.Fatalf("want absolute path error, got %v", err)
	}
}

func TestApplyEditRejectsOversizedStrings(t *testing.T) {
	ws := t.TempDir()
	big := strings.Repeat("x", maxEditSize+1)
	if _, err := applyEdit(ws, "f.txt", big, "y"); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("want size cap error, got %v", err)
	}
}

func TestApplyEditRejectsWholeFileDelete(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("whole file"), 0o644)
	if _, err := applyEdit(ws, "f.txt", "whole file", ""); err == nil || !strings.Contains(err.Error(), "delete") {
		t.Fatalf("want delete refusal, got %v", err)
	}
}

func TestApplyCreateNewFile(t *testing.T) {
	ws := t.TempDir()
	sub := filepath.Join(ws, "nested")
	msg, err := applyCreate(ws, "nested/new.txt", "content here")
	if err != nil {
		t.Fatalf("applyCreate: %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(sub, "new.txt"))
	if string(raw) != "content here" {
		t.Fatalf("bad create content: %q", raw)
	}
	if !strings.Contains(msg, "created nested/new.txt") {
		t.Fatalf("bad success msg: %q", msg)
	}
}

func TestApplyCreateFailsIfExists(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("exists"), 0o644)
	if _, err := applyCreate(ws, "f.txt", "x"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("want already exists error, got %v", err)
	}
}

func TestApplyEditCRLFFileMatchesLFOldStr(t *testing.T) {
	ws := t.TempDir()
	p := filepath.Join(ws, "f.txt")
	os.WriteFile(p, []byte("line1\r\nline2\r\n"), 0o644)
	if _, err := applyEdit(ws, "f.txt", "line1\nline2", "line1\nline2 changed"); err != nil {
		t.Fatalf("CRLF edit with LF old_str failed: %v", err)
	}
	raw, _ := os.ReadFile(p)
	if !strings.Contains(string(raw), "\r\n") || strings.Contains(string(raw), "line2\r\nchanged") {
		t.Fatalf("CRLF not preserved: %q", raw)
	}
	if string(raw) != "line1\r\nline2 changed\r\n" {
		t.Fatalf("wrong CRLF result: %q", raw)
	}
}
