package main

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"node-agent/internal/transport"
)

func TestClaudeArgsFirstRun(t *testing.T) {
	got := claudeArgs(transport.DispatchRequest{})
	want := []string{"-p", "--output-format", "json", "--permission-mode", "bypassPermissions"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claudeArgs() = %q, want %q", got, want)
	}
}

func TestClaudeArgsContinuation(t *testing.T) {
	got := claudeArgs(transport.DispatchRequest{ClaudeSessionID: " 550e8400-e29b-41d4-a716-446655440000 "})
	want := []string{"-p", "--output-format", "json", "--permission-mode", "bypassPermissions", "--resume", "550e8400-e29b-41d4-a716-446655440000"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claudeArgs() = %q, want %q", got, want)
	}
}

func TestParseClaudeResultReadsTerminalResult(t *testing.T) {
	raw := strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"not-terminal"}`,
		`{"type":"result","subtype":"success","is_error":false,"session_id":"session-1","result":"All done."}`,
		"",
	}, "\n")
	got := parseClaudeResult([]byte(raw))
	if got.Type != "result" || got.SessionID != "session-1" || got.Result != "All done." || got.IsError {
		t.Fatalf("parseClaudeResult() = %+v", got)
	}
}

func TestParseClaudeResultIgnoresOtherAndTruncatedFrames(t *testing.T) {
	raw := strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"not-terminal"}`,
		`{"type":"result","session_id":`,
		"provenance executor=claude",
	}, "\n")
	if got := parseClaudeResult([]byte(raw)); got.SessionID != "" {
		t.Fatalf("session id = %q, want empty", got.SessionID)
	}
}

func TestParseClaudeResultKeepsLastCompleteTerminalFrame(t *testing.T) {
	raw := strings.Join([]string{
		`{"type":"result","session_id":"first","result":"first answer"}`,
		`{"type":"result","session_id":"last","result":"last answer"}`,
		`{"type":"result","session_id":`,
	}, "\n")
	got := parseClaudeResult([]byte(raw))
	if got.SessionID != "last" || got.Result != "last answer" {
		t.Fatalf("parseClaudeResult() = %+v", got)
	}
}

func TestClaudeResultCarriesReturnedSessionID(t *testing.T) {
	job := transport.DispatchRequest{TaskID: "t1", Executor: "claude"}
	out := `{"type":"result","subtype":"success","is_error":false,"session_id":"session-1","result":"done"}`
	got := dshResult(job, out, true, "", 42)
	if got.ClaudeSessionID != "session-1" {
		t.Fatalf("ClaudeSessionID = %q, want session-1", got.ClaudeSessionID)
	}
	if got.LastTurnSeq != nil {
		t.Fatalf("LastTurnSeq = %v, want nil", got.LastTurnSeq)
	}
}

func TestClaudeResultKeepsDispatchedSessionWhenFailureHasNoResult(t *testing.T) {
	job := transport.DispatchRequest{TaskID: "t1", Executor: "claude", ClaudeSessionID: "bound-session"}
	got := dshResult(job, "authentication failed", false, "exit status 1", 42)
	if got.ClaudeSessionID != "bound-session" {
		t.Fatalf("ClaudeSessionID = %q, want bound-session", got.ClaudeSessionID)
	}
}

func TestClaudeBinNamesMatchCapabilityProbe(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '2.1.0\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	if got := claudeBin(); got != bin {
		t.Fatalf("claudeBin() = %q, want %q", got, bin)
	}
	executors, versions := detectExecutors()
	if !containsString(executors, "claude") {
		t.Fatalf("Claude not advertised: %v", executors)
	}
	if versions["claude"] != "2.1.0" {
		t.Fatalf("Claude version = %q", versions["claude"])
	}
}

func TestClaudeFakeBinaryRunsInWorkspaceAndReturnsSession(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake executable uses a POSIX shell script")
	}
	workspace := t.TempDir()
	binDir := t.TempDir()
	capturePath := filepath.Join(binDir, "invocation.txt")
	bin := filepath.Join(binDir, "claude")
	script := `#!/bin/sh
printf '%s\n' "$PWD" "$@" > "$CLAUDE_TEST_CAPTURE"
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"fake-session","result":"done"}'
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("CLAUDE_TEST_CAPTURE", capturePath)
	job := transport.DispatchRequest{TaskID: "claude-fake", Workspace: workspace, Message: "a prompt with spaces", Executor: "claude"}
	output, ok, errText := runJobWithProgressAttempt(job, nil)
	if !ok {
		t.Fatalf("fake Claude run failed: %s\\n%s", errText, output)
	}
	gotCapture, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSuffix(string(gotCapture), "\n"), "\n")
	if len(args) < 7 || args[0] != workspace {
		t.Fatalf("unexpected invocation: %q", args)
	}
	if !containsString(args, "--output-format") || !containsString(args, "json") || !containsString(args, "--permission-mode") || !containsString(args, "bypassPermissions") || args[len(args)-1] != "a prompt with spaces" {
		t.Fatalf("invocation missing Claude flags or intact prompt: %q", args)
	}
	if !strings.Contains(output, "provenance executor=claude") || !strings.Contains(output, "claude_session_id=fake-session") {
		t.Fatalf("Claude provenance/session proof missing: %s", output)
	}
}

func TestClaudeContinuationWithoutSessionFailsClosed(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv("PATH", t.TempDir())
	_, ok, errText := runJobWithProgressAttempt(transport.DispatchRequest{
		TaskID:              "claude-continuation",
		Workspace:           workspace,
		Executor:            "claude",
		SessionContinuation: true,
	}, nil)
	if ok || !strings.HasPrefix(errText, "claude_session_missing:") {
		t.Fatalf("continuation result = success %v, error %q", ok, errText)
	}
}
