package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"node-agent/internal/heartbeat"
	"node-agent/internal/transport"
)

var (
	reg     = heartbeat.New(45 * time.Second)
	queues  = map[string]chan transport.DispatchRequest{}
	qmu     sync.Mutex
	results = map[string]storedResult{}
	rmu     sync.Mutex
)

// storedResult wraps a ResultRequest with the time it was stored, so stale
// entries can be evicted instead of growing the map forever.
type storedResult struct {
	res transport.ResultRequest
	at  time.Time
}

const resultTTL = 2 * time.Hour

func getQueue(nodeID string) chan transport.DispatchRequest {
	qmu.Lock()
	defer qmu.Unlock()
	if ch, ok := queues[nodeID]; ok {
		return ch
	}
	ch := make(chan transport.DispatchRequest, 16)
	queues[nodeID] = ch
	return ch
}

// requireToken enforces a shared-secret header on every request when
// NODE_AGENT_TOKEN is set. Without this, anything that can reach the
// listen address (misconfigured firewall, VPS with a public IP, etc.)
// can dispatch arbitrary shell commands to every connected node.
func requireToken(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" {
				next.ServeHTTP(w, r)
				return
			}
			if r.Header.Get("X-Node-Agent-Token") != token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// cleanupResults periodically evicts results older than resultTTL so the
// in-memory map doesn't grow without bound over long server uptimes.
func cleanupResults() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-resultTTL)
		rmu.Lock()
		for id, sr := range results {
			if sr.at.Before(cutoff) {
				delete(results, id)
			}
		}
		rmu.Unlock()
	}
}


// workspaceNoteFor looks up the Note field of the workspace (in
// $HOME/.hermes/workspaces.json) that is the longest prefix of wsPath.
// Empty string when none matches.
func workspaceNoteFor(wsPath string) string {
	raw, err := os.ReadFile(os.ExpandEnv("$HOME/.hermes/workspaces.json"))
	if err != nil {
		return ""
	}
	var data struct {
		Workspaces []struct {
			Path string `json:"path"`
			Note string `json:"note"`
		} `json:"workspaces"`
	}
	if json.Unmarshal(raw, &data) != nil {
		return ""
	}
	best, note := "", ""
	for _, w := range data.Workspaces {
		if w.Note == "" || w.Path == "" {
			continue
		}
		if wsPath == w.Path || (len(wsPath) > len(w.Path) && wsPath[:len(w.Path)] == w.Path) {
			if len(w.Path) > len(best) {
				best, note = w.Path, w.Note
			}
		}
	}
	return note
}

func main() {
	addr := os.Getenv("NODE_AGENT_ADDR")
	if addr == "" {
		addr = ":8788"
	}
	authToken := os.Getenv("NODE_AGENT_TOKEN")
	if authToken == "" {
		log.Printf("WARNING: NODE_AGENT_TOKEN is not set — /api endpoints are UNAUTHENTICATED. " +
			"Set NODE_AGENT_TOKEN (and the same value on every agent) before exposing this beyond localhost.")
	}
	distDir := os.Getenv("NODE_AGENT_DIST_DIR")
	if distDir == "" {
		distDir = "./dist"
	}

	go cleanupResults()

	r := chi.NewRouter()
	r.Use(requireToken(authToken))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		transport.WriteJSON(w, 200, map[string]any{"ok": true, "nodes": reg.List()})
	})
	r.Get("/api/nodes", func(w http.ResponseWriter, r *http.Request) {
		transport.WriteJSON(w, 200, reg.List())
	})
	r.Post("/api/nodes/register", func(w http.ResponseWriter, r *http.Request) {
		var req transport.RegisterRequest
		if err := transport.ReadJSON(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		reg.Upsert(&heartbeat.Node{NodeID: req.NodeID, Hostname: req.Hostname, Workspaces: req.Workspaces, Status: "idle"})
		qmu.Lock()
		if _, ok := queues[req.NodeID]; !ok {
			queues[req.NodeID] = make(chan transport.DispatchRequest, 16)
		}
		qmu.Unlock()
		log.Printf("register %s (%s) workspaces=%v", req.NodeID, req.Hostname, req.Workspaces)
		transport.WriteJSON(w, 200, map[string]string{"status": "ok"})
	})
	r.Post("/api/nodes/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		var req transport.HeartbeatRequest
		_ = transport.ReadJSON(r, &req)
		if !reg.Heartbeat(id, req.Status) {
			http.Error(w, "unknown node", 404)
			return
		}
		transport.WriteJSON(w, 200, map[string]string{"status": "ok"})
	})
	r.Get("/api/nodes/{id}/poll", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if _, ok := reg.Get(id); !ok {
			http.Error(w, "unknown node", 404)
			return
		}
		ch := getQueue(id)
		// long-poll up to 25s
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		select {
		case job := <-ch:
			transport.WriteJSON(w, 200, job)
		case <-ctx.Done():
			// 204 no job — client retries immediately
			w.WriteHeader(204)
		case <-r.Context().Done():
			return
		}
	})
	r.Post("/api/nodes/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		var req transport.ResultRequest
		if err := transport.ReadJSON(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		rmu.Lock()
		results[req.TaskID] = storedResult{res: req, at: time.Now()}
		rmu.Unlock()
		log.Printf("result %s from %s success=%v %dms", req.TaskID, id, req.Success, req.DurationMs)
		transport.WriteJSON(w, 200, map[string]string{"status": "ok"})
	})
	r.Post("/api/dispatch", func(w http.ResponseWriter, r *http.Request) {
		var req transport.DispatchRequest
		if err := transport.ReadJSON(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		// find node by workspace prefix: choose node that owns workspace
		nodeID := ""
		if req.Workspace != "" {
			for _, n := range reg.List() {
				for _, ws := range n.Workspaces {
					if req.Workspace == ws || len(req.Workspace) > len(ws) && req.Workspace[:len(ws)] == ws {
						nodeID = n.NodeID
						break
					}
				}
				if nodeID != "" {
					break
				}
			}
		}
		// fallback: first idle node
		if nodeID == "" {
			for _, n := range reg.List() {
				if n.Status != "offline" {
					nodeID = n.NodeID
					break
				}
			}
		}
		if nodeID == "" {
			http.Error(w, "no nodes available", 503)
			return
		}
		// Inject PrequestNote: match req.Workspace against workspaces.json
		// paths (longest prefix) and copy that workspace's Note, so the
		// agent gets project prerequisites without reading it itself.
		if req.PrequestNote == "" && req.Workspace != "" {
			req.PrequestNote = workspaceNoteFor(req.Workspace)
		}
		ch := getQueue(nodeID)
		select {
		case ch <- req:
			log.Printf("dispatch %s -> %s ws=%s", req.TaskID, nodeID, req.Workspace)
			transport.WriteJSON(w, 200, map[string]any{"status": "queued", "node_id": nodeID})
		default:
			http.Error(w, "node queue full", 503)
		}
	})
	r.Get("/api/results/{task_id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "task_id")
		rmu.Lock()
		sr, ok := results[id]
		rmu.Unlock()
		if !ok {
			http.Error(w, "not found", 404)
			return
		}
		transport.WriteJSON(w, 200, sr.res)
	})
	r.Get("/api/workspaces", func(w http.ResponseWriter, req *http.Request) {
		// return server-side workspaces.json + live node status
		f, err := os.ReadFile(os.ExpandEnv("$HOME/.hermes/workspaces.json"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var data map[string]any
		_ = json.Unmarshal(f, &data)
		transport.WriteJSON(w, 200, map[string]any{"workspaces": data["workspaces"], "nodes": reg.List()})
	})

	// Serve pre-built agent binaries so a fresh machine only needs curl,
	// not a full Go toolchain + scp round-trip. Filenames are looked up
	// through a fixed allowlist so the URL param can never path-traverse
	// into arbitrary files on the server.
	allowedBinaries := map[string]string{
		"mac":     "node-agent-darwin-arm64",
		"windows": "node-agent-windows-amd64.exe",
	}
	r.Get("/dl/{platform}", func(w http.ResponseWriter, r *http.Request) {
		platform := chi.URLParam(r, "platform")
		fname, ok := allowedBinaries[platform]
		if !ok {
			http.Error(w, "unknown platform", 404)
			return
		}
		path := filepath.Join(distDir, fname)
		if _, err := os.Stat(path); err != nil {
			http.Error(w, "binary not built yet — run ./ctl.sh build-"+platform+" on the VPS", 404)
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename="`+fname+`"`)
		http.ServeFile(w, r, path)
	})

	log.Printf("node-agent server listening on %s (auth=%v)", addr, authToken != "")
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}
