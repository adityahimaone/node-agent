package main

import (
	"strings"
	"testing"

	"node-agent/internal/transport"
)

func TestCommandCodeArgsFirstRunOmitsResume(t *testing.T) {
	args := commandCodeArgs(transport.DispatchRequest{}, true)
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--resume") {
		t.Fatalf("first run must not resume a session: %s", joined)
	}
	if !strings.Contains(joined, "--output-format json") {
		t.Fatalf("json output not requested: %s", joined)
	}
	if !strings.Contains(joined, "--yolo") || !strings.Contains(joined, "--skip-onboarding") {
		t.Fatalf("headless automation flags missing: %s", joined)
	}
}

func TestCommandCodeArgsContinuationResumesBoundSession(t *testing.T) {
	args := commandCodeArgs(transport.DispatchRequest{CommandCodeSessionID: " cc-session-9f4e "}, true)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--resume cc-session-9f4e") {
		t.Fatalf("continuation must resume the bound session: %s", joined)
	}
}

func TestCommandCodeArgsTextFallbackDropsJSONFlag(t *testing.T) {
	args := commandCodeArgs(transport.DispatchRequest{CommandCodeSessionID: "cc-1"}, false)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--output-format text") {
		t.Fatalf("fallback must request text: %s", joined)
	}
	if strings.Contains(joined, "json") {
		t.Fatalf("fallback must not request json: %s", joined)
	}
	// The resume flag must survive the fallback so continuity is not lost.
	if !strings.Contains(joined, "--resume cc-1") {
		t.Fatalf("fallback dropped --resume: %s", joined)
	}
}

func TestParseCommandCodeResultEventReadsTerminalFrame(t *testing.T) {
	out := strings.Join([]string{
		`{"type":"event","event":{"type":"tool_running","toolName":"read_file","description":"a.go"}}`,
		`{"type":"result","subtype":"success","sessionId":"cc-9f4e","stopReason":"end_turn","usage":{"inputTokens":120,"outputTokens":40,"totalTokens":160},"finalText":"All done."}`,
		"",
	}, "\n")
	sessionID, finalText, hasUsage := parseCommandCodeResultEvent([]byte(out))
	if sessionID != "cc-9f4e" {
		t.Fatalf("session id = %q, want cc-9f4e", sessionID)
	}
	if finalText != "All done." {
		t.Fatalf("final text = %q", finalText)
	}
	if !hasUsage {
		t.Fatal("usage frame not detected")
	}
}

// A failed run legitimately omits sessionId; the parser must not invent one.
func TestParseCommandCodeResultEventToleratesMissingSession(t *testing.T) {
	out := `{"type":"result","subtype":"error","error":"not authenticated","finalText":""}`
	sessionID, finalText, _ := parseCommandCodeResultEvent([]byte(out))
	if sessionID != "" {
		t.Fatalf("session id = %q, want empty on failure", sessionID)
	}
	if finalText != "" {
		t.Fatalf("final text = %q, want empty", finalText)
	}
}

// A partially streamed trailing line must not abort the parse of earlier frames.
func TestParseCommandCodeResultEventIgnoresTruncatedTail(t *testing.T) {
	out := "{\"type\":\"result\",\"sessionId\":\"cc-1\",\"finalText\":\"hi\"}\n{\"type\":\"result\",\"subty"
	sessionID, finalText, _ := parseCommandCodeResultEvent([]byte(out))
	if sessionID != "cc-1" || finalText != "hi" {
		t.Fatalf("truncated tail broke parse: session=%q text=%q", sessionID, finalText)
	}
}

func TestParseCommandCodeResultEventIgnoresProvenanceLines(t *testing.T) {
	out := strings.Join([]string{
		`provenance executor=commandcode requested=commandcode bin=/usr/bin/cmd args=[] ws=/repo`,
		`EXECUTOR_PROOF=commandcode`,
		`{"type":"result","sessionId":"cc-2","finalText":"ok"}`,
	}, "\n")
	sessionID, finalText, _ := parseCommandCodeResultEvent([]byte(out))
	if sessionID != "cc-2" || finalText != "ok" {
		t.Fatalf("provenance lines broke parse: session=%q text=%q", sessionID, finalText)
	}
}

func TestCommandCodeRejectsJSONOutputOnlyOnFlagErrors(t *testing.T) {
	if !commandCodeRejectsJSONOutput([]byte("error: unknown option '--output-format'")) {
		t.Fatal("unknown option should be detected as an unsupported flag")
	}
	// A genuine task failure must never be mistaken for an unsupported flag,
	// otherwise the worker would re-run a failing task forever.
	if commandCodeRejectsJSONOutput([]byte("Error: the test suite failed 3 assertions")) {
		t.Fatal("a task failure must not trigger the text fallback")
	}
}

func TestCommandCodeJSONOutputRequested(t *testing.T) {
	if !commandCodeJSONOutputRequested([]string{"-p", "--output-format", "json"}) {
		t.Fatal("json not detected")
	}
	if commandCodeJSONOutputRequested([]string{"-p", "--output-format", "text"}) {
		t.Fatal("text must not report json")
	}
	if commandCodeJSONOutputRequested([]string{"-p"}) {
		t.Fatal("no flag must not report json")
	}
}

// A continuation with no dispatched session id is a version mismatch. The worker
// must refuse rather than silently start a cold session, which would re-apply the
// same feedback to files the previous turn already edited.
func TestCommandCodeContinuationRequiresSessionID(t *testing.T) {
	// A continuation with a dispatched id must resume it.
	withID := transport.DispatchRequest{SessionContinuation: true, CommandCodeSessionID: "cc-1"}
	if !strings.Contains(strings.Join(commandCodeArgs(withID, true), " "), "--resume cc-1") {
		t.Fatal("a continuation with a session id must resume it")
	}
	// Without one there is nothing to resume: args must not carry --resume, and
	// the guard in the executor is what stops the cold run.
	withoutID := transport.DispatchRequest{SessionContinuation: true}
	if got := strings.TrimSpace(withoutID.CommandCodeSessionID); got != "" {
		t.Fatalf("precondition failed, session id = %q", got)
	}
	if args := commandCodeArgs(withoutID, true); strings.Contains(strings.Join(args, " "), "--resume") {
		t.Fatalf("must not fabricate a resume target: %s", strings.Join(args, " "))
	}
}
