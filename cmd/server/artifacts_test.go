package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pngBytes is a minimal valid PNG header plus padding — enough for the
// content-sniffer, which is all storeArtifact asks for.
var pngBytes = append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 64)...)

func TestArtifactTaskDirRejectsBadIDs(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	for _, id := range []string{
		"", "..", "../escape", "a/b", `a\b`, "a b", "a;b", "a\x00b",
		"a/../../b", string(make([]byte, 200)),
	} {
		if _, err := artifactTaskDir(id); err == nil {
			t.Fatalf("task id %q was accepted", id)
		}
	}
	good, err := artifactTaskDir("task_abc-123")
	if err != nil {
		t.Fatalf("a valid task id was rejected: %v", err)
	}
	if filepath.Base(good) != "task_abc-123" {
		t.Fatalf("task dir %q does not end in the task id", good)
	}
}

func TestArtifactPathCannotEscape(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	for _, name := range []string{
		"../other.png", "..", ".", "sub/file.png", `sub\file.png`, "",
	} {
		if _, err := artifactPath("task1", name); err == nil {
			t.Fatalf("artifact name %q was accepted", name)
		}
	}
	full, err := artifactPath("task1", "ok.png")
	if err != nil {
		t.Fatalf("a valid artifact name was rejected: %v", err)
	}
	if filepath.Base(full) != "ok.png" {
		t.Fatalf("artifact path %q does not end in the name", full)
	}
}

func TestStoreArtifactSniffsAndNames(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", dir)
	art, err := storeArtifact("task1", "../../evil.png", pngBytes)
	if err != nil {
		t.Fatalf("storeArtifact: %v", err)
	}
	if art.MIME != "image/png" {
		t.Fatalf("MIME = %q, want image/png", art.MIME)
	}
	if filepath.Ext(art.Name) != ".png" {
		t.Fatalf("generated name %q does not carry the sniffed extension", art.Name)
	}
	// The client-supplied label must not reach the path in any form.
	if filepath.Dir(art.Path) != filepath.Join(dir, "task1") {
		t.Fatalf("artifact was written outside the task directory: %s", art.Path)
	}
	if _, err := os.Stat(art.Path); err != nil {
		t.Fatalf("artifact not written: %v", err)
	}
}

// TestStoreArtifactIsIdempotent proves re-uploading the same bytes does not
// accumulate near-duplicates, which is what makes a verify re-run cheap.
func TestStoreArtifactIsIdempotent(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	first, err := storeArtifact("task1", "a.png", pngBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := storeArtifact("task1", "b.png", pngBytes)
	if err != nil {
		t.Fatal(err)
	}
	if first.Name != second.Name {
		t.Fatalf("the same bytes produced two names: %q and %q", first.Name, second.Name)
	}
	if got := len(listTaskArtifacts("task1")); got != 1 {
		t.Fatalf("re-upload left %d artifacts, want 1", got)
	}
}

func TestStoreArtifactRejectsBadInput(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	if _, err := storeArtifact("task1", "a.png", nil); err == nil {
		t.Fatal("an empty artifact was accepted")
	}
	// A declared type is not consulted: content is what counts.
	if _, err := storeArtifact("task1", "a.png", []byte("not an image at all")); err == nil {
		t.Fatal("a non-image artifact was accepted")
	}
	big := make([]byte, maxArtifactBytes+1)
	copy(big, pngBytes)
	if _, err := storeArtifact("task1", "big.png", big); err == nil {
		t.Fatal("an oversized artifact was accepted")
	}
}

func TestStoreArtifactEnforcesTheTaskBudget(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	// Fill the task with distinct content until the next file would cross the
	// per-task cap. Each file is maxArtifactBytes, so the cap is reached partway
	// through the seventh.
	var stored int
	for i := 0; i < 20; i++ {
		chunk := make([]byte, maxArtifactBytes)
		copy(chunk, pngBytes)
		// Vary the tail so each upload is distinct content rather than a
		// duplicate the store would collapse onto the first file.
		chunk[len(chunk)-1] = byte(i)
		_, err := storeArtifact("task2", "a.png", chunk)
		if err != nil {
			if stored == 0 {
				t.Fatalf("the very first artifact was refused: %v", err)
			}
			if stored*maxArtifactBytes < maxTaskArtifactBytes {
				t.Fatalf("artifact %d refused early, after only %d bytes against a %d-byte cap: %v",
					i, stored*maxArtifactBytes, maxTaskArtifactBytes, err)
			}
			return
		}
		stored++
	}
	t.Fatalf("the budget was never enforced after %d files of %d bytes (cap %d)",
		stored, maxArtifactBytes, maxTaskArtifactBytes)
}

// TestStoreArtifactDoesNotChargeForDuplicates keeps an idempotent retry from
// looking like disk exhaustion.
func TestStoreArtifactDoesNotChargeForDuplicates(t *testing.T) {
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", t.TempDir())
	chunk := make([]byte, maxArtifactBytes)
	copy(chunk, pngBytes)
	for i := 0; i < 10; i++ {
		if _, err := storeArtifact("task5", "a.png", chunk); err != nil {
			t.Fatalf("re-upload %d was refused: %v", i, err)
		}
	}
	if got := len(listTaskArtifacts("task5")); got != 1 {
		t.Fatalf("re-uploads left %d artifacts, want 1", got)
	}
}

func TestListTaskArtifactsSkipsUnreadable(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", dir)
	if _, err := storeArtifact("task3", "a.png", pngBytes); err != nil {
		t.Fatal(err)
	}
	// A stray non-image file in the directory must not be reported as evidence.
	if err := os.WriteFile(filepath.Join(dir, "task3", "notes.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := listTaskArtifacts("task3")
	if len(got) != 1 {
		t.Fatalf("listTaskArtifacts returned %d entries, want 1", len(got))
	}
	if got[0].MIME != "image/png" {
		t.Fatalf("listed MIME = %q", got[0].MIME)
	}
}

func TestCleanupArtifactsRemovesExpired(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NODE_AGENT_ARTIFACT_DIR", dir)
	art, err := storeArtifact("task4", "a.png", pngBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Backdate the file well past the TTL, then run one sweep by hand.
	old := time.Now().Add(-2 * artifactTTL)
	if err := os.Chtimes(art.Path, old, old); err != nil {
		t.Fatal(err)
	}
	dirs, err := os.ReadDir(dir)
	if err != nil || len(dirs) != 1 {
		t.Fatalf("setup: %d task dirs", len(dirs))
	}
	sweepArtifactsOnce()
	if _, err := os.Stat(art.Path); !os.IsNotExist(err) {
		t.Fatal("an expired artifact survived the sweep")
	}
	// The emptied task directory should go too, so the root does not accumulate
	// one folder per task the node has ever run.
	if _, err := os.Stat(filepath.Join(dir, "task4")); !os.IsNotExist(err) {
		t.Fatal("an empty task directory survived the sweep")
	}
}
