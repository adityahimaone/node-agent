package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"node-agent/internal/transport"
)

// Regression: every commandcode run on Windows failed with "'C:\Program' is not
// recognized as an internal or external command". An npm install leaves a .cmd
// shim under "C:\Program Files\nodejs", and no quoting scheme survives a prompt
// containing a space — which every Kanban prompt has. The shim is resolved to the
// node invocation it wraps instead. Verified live: node + index.mjs returns
// EXIT=0 with a session id.
func TestResolveNpmShimIsWindowsOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exercised by the live Windows node")
	}
	if _, ok := resolveNpmShim("/usr/local/bin/cmdc"); ok {
		t.Fatal("shim resolution must not apply off Windows")
	}
}

// commandFor must run a resolved shim through node rather than the shim itself.
func TestCommandForUsesResolvedShim(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("shim resolution only applies on Windows")
	}
	bin := filepath.Join("C:", "Program Files", "nodejs", "cmdc.cmd")
	shim, ok := resolveNpmShim(bin)
	if !ok {
		t.Skip("no npm package on this host")
	}
	cmd := commandFor(context.Background(), bin, "-p", "CC OK with spaces")
	if filepath.Base(cmd.Path) != filepath.Base(shim.Node) {
		t.Fatalf("expected node, got %q", cmd.Path)
	}
	if cmd.Args[1] != shim.Entry {
		t.Fatalf("argv[1] = %q, want the package entry %q", cmd.Args[1], shim.Entry)
	}
	if len(cmd.Args) < 5 || cmd.Args[4] != "CC OK with spaces" {
		t.Fatalf("args = %q, want the spaced prompt preserved as one argument", cmd.Args)
	}
}

// npmPackageEntry must find the conventional dist/index.mjs.
func TestNpmPackageEntryPrefersDistIndex(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "command-code")
	if err := os.MkdirAll(filepath.Join(pkg, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(pkg, "dist", "index.mjs")
	if err := os.WriteFile(want, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := npmPackageEntry(pkg); got != want {
		t.Fatalf("entry = %q, want %q", got, want)
	}
	if got := npmPackageEntry(filepath.Join(dir, "missing")); got != "" {
		t.Fatalf("missing package must yield empty entry, got %q", got)
	}
}

// A native binary must be run directly, never through an interpreter.
func TestCommandForNativeBinaryIsNotWrapped(t *testing.T) {
	cmd := commandFor(context.Background(), filepath.Join("c:", "omp", "omp.exe"), "--version")
	if strings.HasSuffix(strings.ToLower(cmd.Path), ".exe") {
		if len(cmd.Args) > 1 && strings.EqualFold(cmd.Args[1], "/c") {
			t.Fatal("a native .exe must not be wrapped in cmd /c")
		}
	}
}

// Regression: the advertised candidates must match the ones execution resolves.
// Probing `cmd` on Windows finds the built-in cmd.exe, so the node reported the
// shell banner as Command Code's version while actually running `cmdc`.
func TestCommandCodeBinNamesMatchResolution(t *testing.T) {
	names := commandCodeBinNames()
	if len(names) == 0 {
		t.Fatal("commandcode must advertise at least one candidate")
	}
	if runtime.GOOS == "windows" {
		for _, n := range names {
			if n == "cmd" {
				t.Fatalf("cmd is the Windows shell, not Command Code: %v", names)
			}
		}
	}
	// Whatever is advertised first must be what commandCodeBin actually returns.
	first := findBinAny(names...)
	if first == "" {
		t.Skipf("no Command Code binary on this host")
	}
	if want := findBinAny(names[0]); first != want {
		t.Fatalf("advertised candidates resolve to %q, want %q", first, want)
	}
	// A bare `cmd` must never be advertised on Windows.
	if runtime.GOOS == "windows" && strings.Contains(strings.Join(names, " "), "cmd ") {
		t.Fatalf("Windows must not advertise bare cmd: %v", names)
	}
}

func TestOMPArgsFirstRunOmitsResume(t *testing.T) {
	args := ompArgs(transport.DispatchRequest{})
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--resume") {
		t.Fatalf("first run must not resume a session: %s", joined)
	}
	// NDJSON is what lets the worker prove session identity, so a first run that
	// drops --mode json can never bind the card.
	if !strings.Contains(joined, "--mode json") {
		t.Fatalf("json output not requested: %s", joined)
	}
	// A headless worker cannot answer an interactive approval prompt.
	if !strings.Contains(joined, "-p") || !strings.Contains(joined, "--auto-approve") {
		t.Fatalf("headless automation flags missing: %s", joined)
	}
}

func TestOMPArgsContinuationResumesBoundSession(t *testing.T) {
	args := ompArgs(transport.DispatchRequest{OMPSessionID: " 01JZ8Y2Aomp "})
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--resume 01JZ8Y2Aomp") {
		t.Fatalf("continuation must resume the bound session: %s", joined)
	}
}

func TestParseOMPSessionIDReadsTopLevelFrame(t *testing.T) {
	out := strings.Join([]string{
		`{"type":"agent_start"}`,
		`{"type":"message_update","messageId":"msg-1"}`,
		`{"type":"session_info_update","sessionId":"01JZ8Y2Aomp"}`,
	}, "\n")
	if got := parseOmpSessionID([]byte(out)); got != "01JZ8Y2Aomp" {
		t.Fatalf("session id = %q, want 01JZ8Y2Aomp", got)
	}
}

func TestParseOMPSessionIDReadsNestedDataFrame(t *testing.T) {
	out := `{"id":"req_1","type":"response","command":"open_session","success":true,"data":{"cancelled":false,"resumed":true,"sessionId":"01JZ8Y2Anested"}}`
	if got := parseOmpSessionID([]byte(out)); got != "01JZ8Y2Anested" {
		t.Fatalf("nested session id = %q, want 01JZ8Y2Anested", got)
	}
}

func TestParseOMPSessionIDIgnoresProvenanceAndProofLines(t *testing.T) {
	out := strings.Join([]string{
		`provenance executor=omp requested=omp bin=/usr/local/bin/omp args=["-p"] ws=/tmp/x`,
		`EXECUTOR_PROOF=omp`,
		`{"type":"notice","message":"hello"}`,
	}, "\n")
	if got := parseOmpSessionID([]byte(out)); got != "" {
		t.Fatalf("no session frame present, got %q", got)
	}
}

func TestParseOMPSessionIDToleratesTruncatedTail(t *testing.T) {
	out := "{\"type\":\"agent_start\"}\n{\"type\":\"message_upd"
	if got := parseOmpSessionID([]byte(out)); got != "" {
		t.Fatalf("truncated stream must yield no session, got %q", got)
	}
}

func TestParseOMPSessionIDKeepsLastSeenID(t *testing.T) {
	out := strings.Join([]string{
		`{"type":"ready","sessionId":"first"}`,
		`{"type":"session_info_update","sessionId":"last"}`,
	}, "\n")
	if got := parseOmpSessionID([]byte(out)); got != "last" {
		t.Fatalf("session id = %q, want last", got)
	}
}

// Regression: the omp parser accepts any frame carrying a top-level sessionId,
// and Command Code emits `{"type":"run_start","sessionId":...}`. Calling it
// unconditionally stamped omp_session_id onto commandcode provenance.
func TestParseOMPSessionIDWouldMatchCommandCodeFrames(t *testing.T) {
	out := `{"type":"run_start","sessionId":"cc-abc"}`
	if got := parseOmpSessionID([]byte(out)); got != "cc-abc" {
		t.Fatalf("precondition: parser should match a top-level sessionId, got %q", got)
	}
}

func TestOMPResultCarriesSessionID(t *testing.T) {
	job := transport.DispatchRequest{TaskID: "t1", Executor: "omp", OMPSessionID: "01JZ8Y2Aomp"}
	out := "provenance executor=omp\n{\"type\":\"session_info_update\",\"sessionId\":\"01JZ8Y2Aomp\"}\n"
	res := dshResult(job, out, true, "", 42)
	if res.OMPSessionID != "01JZ8Y2Aomp" {
		t.Fatalf("result session = %q, want 01JZ8Y2Aomp", res.OMPSessionID)
	}
	// omp emits no turn cursor, so the control plane relies on its run fence.
	if res.LastTurnSeq != nil {
		t.Fatalf("omp must leave the turn cursor nil, got %d", *res.LastTurnSeq)
	}
}

func TestOMPResultFallsBackToDispatchedSession(t *testing.T) {
	job := transport.DispatchRequest{TaskID: "t1", Executor: "omp", OMPSessionID: "bound-1"}
	res := dshResult(job, "no frames here", true, "", 10)
	if res.OMPSessionID != "bound-1" {
		t.Fatalf("result session = %q, want the dispatched bound-1", res.OMPSessionID)
	}
}
