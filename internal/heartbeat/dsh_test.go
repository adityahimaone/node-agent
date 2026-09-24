package heartbeat

import (
	"os"
	"path/filepath"
	"testing"
)

// fakeDSHBin writes an executable sh script answering --version; any other
// invocation (e.g. dump-config) prints the same line.
func fakeDSHBin(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dsh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\necho \"dsh 0.5.2-test\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDSHProbeParsesVersionAndModel(t *testing.T) {
	dump := "provider:\n    provider: deepseek-official\n    model: deepseek-flash\n    apiKeyEnv: DEEPSEEK_API_KEY\n"
	h, err := ProbeDSH(fakeDSHBin(t), dump)
	if err != nil {
		t.Fatalf("ProbeDSH: %v", err)
	}
	if !h.OK {
		t.Fatal("OK = false, want true")
	}
	if h.Version == "" {
		t.Fatal("Version empty")
	}
	if h.Model != "deepseek-flash" {
		t.Fatalf("Model = %q, want deepseek-flash", h.Model)
	}
	if h.Provider != "deepseek-official" {
		t.Fatalf("Provider = %q, want deepseek-official", h.Provider)
	}
	if h.Error != "" {
		t.Fatalf("Error = %q, want empty", h.Error)
	}
}

func TestDSHProbeMissingBinary(t *testing.T) {
	h, err := ProbeDSH("/nonexistent/dsh", "")
	if err == nil {
		t.Fatal("want error for missing binary")
	}
	if h.OK {
		t.Fatal("OK = true, want false")
	}
	if h.Error != "no_binary" {
		t.Fatalf("Error = %q, want no_binary", h.Error)
	}
}

func TestDSHProbeCacheTTL(t *testing.T) {
	c := NewDSHProbeCache(0) // clamped to 60s
	bin := fakeDSHBin(t)
	first := c.Get(bin)
	if first.CheckedAt == 0 || !first.OK {
		t.Fatalf("first Get = %+v, want ok + checked", first)
	}
	second := c.Get(bin)
	if second.CheckedAt != first.CheckedAt {
		t.Fatalf("second Get re-probed: checked_at %d != %d", second.CheckedAt, first.CheckedAt)
	}
	if !second.OK {
		t.Fatal("second Get lost OK")
	}
	z := c.Get("")
	if z.OK || z.Error != "" || z.CheckedAt != 0 {
		t.Fatalf("empty-bin Get = %+v, want zero health", z)
	}
	// empty-bin call must not have flushed the cache
	third := c.Get(bin)
	if third.CheckedAt != first.CheckedAt {
		t.Fatalf("empty-bin Get flushed cache: %d != %d", third.CheckedAt, first.CheckedAt)
	}
}
