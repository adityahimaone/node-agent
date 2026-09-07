package main

import "testing"

// rewriteShellCmd must fall back to raw whenever rtk is missing, slow,
// or has no rewrite for the command — never error out the job path.
func TestRewriteShellCmd(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
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
			// Known rewrites should become rtk-prefixed; unknowns return raw.
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
