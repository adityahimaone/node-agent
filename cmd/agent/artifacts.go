package main

// Agent side of artifact transport.
//
// A run that produces files writes them into a per-job directory; before the
// result is posted, the agent uploads whatever is there and reports the
// metadata back. The control plane pulls the bytes later, so nothing large ever
// rides in the result message itself.
//
// The upload directory is set by the dispatch (the control plane points it at
// the job's artifact dir). When it is unset — which is every job that predates
// this, and every non-verify job — this is a no-op.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"node-agent/internal/transport"
)

// maxLocalArtifactBytes mirrors the server's cap, so an oversized file is
// refused locally rather than after a pointless upload.
const maxLocalArtifactBytes = 5 << 20

// artifactUploadTimeout bounds the whole upload batch. The server is on the
// tailnet and the files are already local.
const artifactUploadTimeout = 60 * time.Second

// serverBase is the control plane address, shared by the poll loop and the
// artifact uploader so the two cannot disagree about where the server is.
func serverBase() string {
	if v := os.Getenv("NODE_AGENT_SERVER"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://100.64.0.1:8788"
}

// jobArtifactDir returns the directory this job's artifacts are collected in,
// or "" when the job produces none.
//
// Only a dispatch-supplied directory counts. The agent cannot invent one the
// server would know to look in, and guessing at a shared location would mean
// uploading a run's files to a place nothing ever reads — which looks like it
// works and silently drops the evidence.
func jobArtifactDir(job transport.DispatchRequest) string {
	return strings.TrimSpace(job.ArtifactDir)
}

// uploadJobArtifacts uploads a job's artifacts and returns their metadata.
//
// Best-effort by design: a failed upload is logged and reported as no artifacts,
// never as a job error. The alternative — failing a green run because a
// screenshot did not make it — would make the loop trustworthy for the wrong
// reason.
func uploadJobArtifacts(job transport.DispatchRequest) []transport.Artifact {
	dir := os.Getenv("NODE_AGENT_JOB_ARTIFACT_DIR")
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var uploaded []transport.Artifact
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || info.Size() == 0 || info.Size() > maxLocalArtifactBytes {
			continue
		}
		path := filepath.Join(dir, e.Name())
		art, err := uploadArtifactFile(job.TaskID, path, e.Name())
		if err != nil {
			log.Printf("artifact upload %s: %v", e.Name(), err)
			continue
		}
		uploaded = append(uploaded, art)
	}
	return uploaded
}

// uploadArtifactFile posts one file to the node-agent server under a task id.
func uploadArtifactFile(taskID, path, filename string) (transport.Artifact, error) {
	if taskID == "" {
		return transport.Artifact{}, fmt.Errorf("no task id to file this artifact under")
	}
	f, err := os.Open(path)
	if err != nil {
		return transport.Artifact{}, err
	}
	defer f.Close()

	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", filepath.Base(filename))
	if err != nil {
		return transport.Artifact{}, err
	}
	if _, err := io.Copy(part, io.LimitReader(f, maxLocalArtifactBytes)); err != nil {
		return transport.Artifact{}, err
	}
	if err := w.Close(); err != nil {
		return transport.Artifact{}, err
	}

	url := fmt.Sprintf("%s/api/nodes/artifacts/%s", serverBase(), url.PathEscape(taskID))
	req, err := http.NewRequest("POST", url, &body)
	if err != nil {
		return transport.Artifact{}, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	if agentToken != "" {
		req.Header.Set("X-Node-Agent-Token", agentToken)
	}
	client := &http.Client{Timeout: artifactUploadTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return transport.Artifact{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return transport.Artifact{}, fmt.Errorf("server returned %s", resp.Status)
	}
	var art transport.Artifact
	if err := json.Unmarshal(raw, &art); err != nil {
		return transport.Artifact{}, fmt.Errorf("bad response: %w", err)
	}
	return art, nil
}
