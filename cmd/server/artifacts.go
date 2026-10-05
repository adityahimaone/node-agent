package main

// Artifact storage for the verification loop.
//
// The worker produces files (verify screenshots, an axe report) that the control
// plane pulls back over the token-authenticated client. Two decisions shape this:
//
//   - On disk, not in the results map. Results live in memory with a 2h TTL; a
//     screenshot that vanished on restart would mean a card reaching review with
//     its evidence missing, which is precisely the failure this transport
//     exists to prevent.
//   - Uploads are untrusted. A client-supplied filename is never used as a path.
//     Each file gets a generated name under a per-task directory, the MIME type
//     is sniffed from the content, and the original name is kept only as a
//     display label.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"node-agent/internal/transport"
)

const (
	// maxArtifactBytes caps one file. It matches the control plane's blobstore
	// ceiling so a file that uploads cleanly cannot be rejected on the way in.
	maxArtifactBytes = 5 << 20
	// maxTaskArtifactBytes caps one task's total, so a runaway producer cannot
	// fill the disk on a node nobody is watching.
	maxTaskArtifactBytes = 30 << 20
	// artifactTTL is much longer than resultTTL: the control plane pulls shortly
	// after the run, but a pull can be delayed by a retry, and evidence that is
	// already gone cannot be re-fetched.
	artifactTTL = 7 * 24 * time.Hour
	// artifactSweepInterval is coarse on purpose — this walks a directory tree,
	// unlike cleanupResults which only touches an in-memory map.
	artifactSweepInterval = time.Hour
)

// allowedArtifactMIME mirrors the control plane's attachment allowlist. Only
// images and PDFs are worth transporting: the rest would be rejected on arrival.
var allowedArtifactMIME = map[string]bool{
	"image/png":       true,
	"image/jpeg":      true,
	"image/webp":      true,
	"image/gif":       true,
	"application/pdf": true,
}

// artifactDir is the root for stored artifacts.
//
// Read per call rather than captured at init: this is a server, and a value
// frozen at process start cannot be pointed elsewhere without a restart — which
// is also what makes it impossible to test without writing to the real temp
// directory.
func artifactDir() string {
	if v := os.Getenv("NODE_AGENT_ARTIFACT_DIR"); v != "" {
		return v
	}
	return filepath.Join(os.TempDir(), "node-agent-artifacts")
}

// artifactTaskDir is the directory one task's artifacts live in.
//
// The task id comes from the URL, so it is validated rather than trusted: a
// task id containing a separator would otherwise write outside this directory.
func artifactTaskDir(taskID string) (string, error) {
	if taskID == "" || len(taskID) > 128 {
		return "", fmt.Errorf("invalid task id")
	}
	for _, r := range taskID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return "", fmt.Errorf("invalid task id")
		}
	}
	return filepath.Join(artifactDir(), taskID), nil
}

// artifactPath resolves one stored file, refusing anything that escapes the
// task directory even if a name somehow got past validation.
func artifactPath(taskID, name string) (string, error) {
	dir, err := artifactTaskDir(taskID)
	if err != nil {
		return "", err
	}
	if name == "" || name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("invalid artifact name")
	}
	full := filepath.Join(dir, name)
	// Defence in depth: even with a validated base name, confirm the resolved
	// path is really under the task directory.
	if !strings.HasPrefix(full, dir+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifact path escapes its task directory")
	}
	return full, nil
}

// sniffArtifactMIME identifies an upload from its content rather than trusting
// the declared type, and reports the extension to store it under.
func sniffArtifactMIME(data []byte) (string, bool) {
	if len(data) == 0 {
		return "", false
	}
	// PNG and PDF have magic bytes worth checking before http.DetectContentType,
	// which is more reliable when it is not asked to guess.
	if len(data) >= 8 && string(data[:8]) == "\x89PNG\r\n\x1a\n" {
		return "image/png", true
	}
	if len(data) >= 4 && string(data[:4]) == "%PDF" {
		return "application/pdf", true
	}
	mime := http.DetectContentType(data)
	if i := strings.Index(mime, ";"); i != -1 {
		mime = strings.TrimSpace(mime[:i])
	}
	if allowedArtifactMIME[mime] {
		return mime, true
	}
	// DetectContentType is conservative about webp and gif; accept the family.
	switch {
	case strings.HasPrefix(mime, "image/webp"):
		return "image/webp", true
	case strings.HasPrefix(mime, "image/gif"):
		return "image/gif", true
	}
	return "", false
}

func artifactExt(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "application/pdf":
		return ".pdf"
	}
	return ""
}

// storeArtifact writes one uploaded file and returns its metadata.
//
// name is a caller-supplied label used only to pick a readable generated name;
// it is never used as a path component verbatim.
func storeArtifact(taskID, label string, data []byte) (transport.Artifact, error) {
	dir, err := artifactTaskDir(taskID)
	if err != nil {
		return transport.Artifact{}, err
	}
	if len(data) == 0 {
		return transport.Artifact{}, fmt.Errorf("empty artifact")
	}
	if int64(len(data)) > maxArtifactBytes {
		return transport.Artifact{}, fmt.Errorf("artifact too large (max %d bytes)", int64(maxArtifactBytes))
	}
	mime, ok := sniffArtifactMIME(data)
	if !ok {
		return transport.Artifact{}, fmt.Errorf("unsupported artifact type")
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	// Enforce the per-task total before writing, so a rejected upload never
	// lands on disk at all.
	//
	// The budget is spent on distinct content: re-uploading bytes already stored
	// adds nothing, and charging for it would make an idempotent retry look
	// like disk exhaustion.
	var stored int64
	for _, a := range listTaskArtifacts(taskID) {
		if a.SHA256 != sha {
			stored += a.Bytes
		}
	}
	if stored+int64(len(data)) > maxTaskArtifactBytes {
		return transport.Artifact{}, fmt.Errorf("task artifact budget exceeded (%d bytes)", int64(maxTaskArtifactBytes))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return transport.Artifact{}, err
	}

	// The stored name is generated, and the hash prefix keeps re-uploads of the
	// same bytes idempotent instead of accumulating near-duplicates.
	name := sha[:16] + artifactExt(mime)
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, data, 0o644); err != nil {
		return transport.Artifact{}, err
	}
	return transport.Artifact{
		Name:   name,
		Path:   full,
		SHA256: sha,
		Bytes:  int64(len(data)),
		MIME:   mime,
	}, nil
}

// listTaskArtifacts returns a task's stored artifacts, for reporting in a result.
func listTaskArtifacts(taskID string) []transport.Artifact {
	dir, err := artifactTaskDir(taskID)
	if err != nil {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []transport.Artifact
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		full := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		// The type is re-derived from the bytes rather than stored alongside
		// them, so a file cannot be served claiming to be something it is not.
		m, ok := sniffArtifactMIME(data)
		if !ok {
			continue
		}
		sum := sha256.Sum256(data)
		out = append(out, transport.Artifact{
			Name:   e.Name(),
			Path:   full,
			SHA256: hex.EncodeToString(sum[:]),
			Bytes:  info.Size(),
			MIME:   m,
		})
	}
	return out
}

// cleanupArtifacts sweeps stored artifacts older than artifactTTL.
func cleanupArtifacts() {
	ticker := time.NewTicker(artifactSweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		sweepArtifactsOnce()
	}
}

// sweepArtifactsOnce is one pass of the sweep, split out so a test can run it
// without waiting an hour for a ticker.
func sweepArtifactsOnce() {
	entries, err := os.ReadDir(artifactDir())
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-artifactTTL)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(artifactDir(), e.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, f := range files {
			info, err := f.Info()
			if err != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				_ = os.Remove(filepath.Join(dir, f.Name()))
			}
		}
		// Drop the task directory once it is empty, so the root does not
		// accumulate one folder per task the node has ever run.
		if remaining, err := os.ReadDir(dir); err == nil && len(remaining) == 0 {
			_ = os.Remove(dir)
		}
	}
}

// handleArtifactUpload accepts one file for a task.
func handleArtifactUpload(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "task_id")
	if _, err := artifactTaskDir(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Cap the parsed body before reading it, so an oversized upload is refused
	// rather than buffered.
	r.Body = http.MaxBytesReader(w, r.Body, maxTaskArtifactBytes+maxArtifactBytes)
	if err := r.ParseMultipartForm(maxArtifactBytes); err != nil {
		http.Error(w, "expected a multipart upload", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "expected a multipart upload", http.StatusBadRequest)
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxArtifactBytes+1))
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	art, err := storeArtifact(taskID, header.Filename, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("artifact %s stored for task %s (%d bytes, %s)", art.Name, taskID, art.Bytes, art.MIME)
	transport.WriteJSON(w, http.StatusOK, art)
}

// handleArtifactDownload serves one stored artifact back to the control plane.
func handleArtifactDownload(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "task_id")
	name := chi.URLParam(r, "name")
	full, err := artifactPath(taskID, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f, err := os.Open(full)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, name, info.ModTime(), f)
}
