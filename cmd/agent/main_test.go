package main

import (
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
