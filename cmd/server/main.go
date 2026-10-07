package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"node-agent/internal/conversation"
	"node-agent/internal/heartbeat"
	"node-agent/internal/transport"
)

var (
	reg      = heartbeat.New(45 * time.Second)
	queues   = map[string]chan transport.DispatchRequest{}
	qmu      sync.Mutex
	results  = map[string]storedResult{}
	progress = map[string]string{}
	// progressBase[id] is the absolute offset of progress[id]'s
	// first byte: the buffer is capped by dropping its oldest
	// bytes, so the base tracks what was dropped and lets a
	// poller's offset stay absolute across a truncation.
	progressBase = map[string]int{}
	rmu          sync.Mutex
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
	currentAuthToken = authToken

	// gRPC listener (worker lane). Disabled by setting NODE_AGENT_GRPC_ENABLED=0.
	grpcAddr := os.Getenv("NODE_AGENT_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":8789"
	}
	if os.Getenv("NODE_AGENT_GRPC_ENABLED") != "0" {
		go func() {
			gs := grpc.NewServer(grpc.ForceServerCodec(transport.Codec{}))
			transport.RegisterNodeAgentServiceServer(gs, grpcService{})
			lis, err := net.Listen("tcp", grpcAddr)
			if err != nil {
				log.Printf("grpc listen %s failed: %v (HTTP long-poll remains the worker lane)", grpcAddr, err)
				return
			}
			log.Printf("node-agent gRPC listening on %s", grpcAddr)
			if err := gs.Serve(lis); err != nil {
				log.Printf("grpc serve: %v", err)
			}
		}()
	}

	go cleanupResults()
	go cleanupArtifacts()

	// Conversation store for server-side conversation management API.
	home := os.Getenv("HOME")
	if home == "" {
		home = os.TempDir()
	}
	convStore, err := conversation.NewStore(home)
	if err != nil {
		log.Fatalf("conversation store: %v", err)
	}

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
		reg.Upsert(&heartbeat.Node{NodeID: req.NodeID, Hostname: req.Hostname, Workspaces: req.Workspaces, Executors: req.Executors, Versions: req.Versions, Status: "idle", DSHHealth: req.DSHHealth})
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
		if !reg.HeartbeatWithHealth(id, req.Status, req.DSHHealth) {
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
	// Artifact transport. Uploads and downloads are token-guarded like every
	// other route, and the task id is validated into a directory before it
	// touches the filesystem — see artifacts.go.
	r.Post("/api/nodes/artifacts/{task_id}", handleArtifactUpload)
	r.Get("/api/nodes/artifacts/{task_id}/{name}", handleArtifactDownload)
	r.Post("/api/nodes/progress", func(w http.ResponseWriter, r *http.Request) {
		var req transport.ProgressRequest
		if err := transport.ReadJSON(r, &req); err != nil || req.TaskID == "" {
			http.Error(w, "invalid progress", 400)
			return
		}
		rmu.Lock()
		progress[req.TaskID] += req.Chunk
		// cap at 512KB to avoid unbounded growth
		if len(progress[req.TaskID]) > 512*1024 {
			dropped := len(progress[req.TaskID]) - 512*1024
			progressBase[req.TaskID] += dropped
			progress[req.TaskID] = progress[req.TaskID][dropped:]
		}
		rmu.Unlock()
		transport.WriteJSON(w, 200, map[string]string{"status": "ok"})
	})
	r.Get("/api/progress/{task_id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "task_id")
		offStr := r.URL.Query().Get("offset")
		off := 0
		if offStr != "" {
			if n, err := strconv.Atoi(offStr); err == nil {
				off = n
			}
		}
		rmu.Lock()
		full := progress[id]
		base := progressBase[id]
		sr, hasResult := results[id]
		rmu.Unlock()
		text, next := progressWindow(full, base, off)
		done := hasResult
		transport.WriteJSON(w, 200, map[string]any{"task_id": id, "text": text, "offset": next, "done": done, "has_result": hasResult, "result": sr.res})
	})
	r.Post("/api/dispatch", func(w http.ResponseWriter, r *http.Request) {
		var req transport.DispatchRequest
		if err := transport.ReadJSON(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		executor := strings.ToLower(strings.TrimSpace(req.Executor))
		if executor == "" {
			executor = "auto"
		}
		// Route to the node that actually owns the workspace. A miss is an
		// error: falling back to an arbitrary online node used to silently run
		// macOS workspaces on the Windows node, which failed much later with
		// "workspace not found" and surfaced to callers as an empty result.
		// The decision itself lives in pickDispatchNode so every outcome —
		// the match, both 409s, the no-workspace fallback and the 503 — is
		// testable without HTTP scaffolding.
		nodeID, status, pickErr := pickDispatchNode(reg.List(), req.Workspace, executor)
		if pickErr != nil {
			http.Error(w, pickErr.Error(), status)
			return
		}
		// A retry can reuse task_id. Clear prior result/progress before enqueueing,
		// otherwise control plane can read stale result immediately.
		rmu.Lock()
		delete(results, req.TaskID)
		delete(progress, req.TaskID)
		rmu.Unlock()

		// Inject PrequestNote: match req.Workspace against workspaces.json
		// paths (longest prefix) and copy that workspace's Note, so the
		// agent gets project prerequisites without reading it itself.
		if req.PrequestNote == "" && req.Workspace != "" {
			req.PrequestNote = workspaceNoteFor(req.Workspace)
		}
		if deliveryID, ok := dispatchGRPC(req, nodeID); ok {
			transport.WriteJSON(w, 200, map[string]any{"status": "queued", "node_id": nodeID, "transport": "grpc", "delivery_id": deliveryID})
			return
		}
		ch := getQueue(nodeID)
		select {
		case ch <- req:
			log.Printf("dispatch %s -> %s ws=%s transport=http", req.TaskID, nodeID, req.Workspace)
			transport.WriteJSON(w, 200, map[string]any{"status": "queued", "node_id": nodeID, "transport": "http"})
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

	// ---- Conversation management API ----
	// These endpoints let the control plane manage persistent conversations.
	// The worker agent reads/writes the same store directory, so changes are
	// visible immediately.

	// List all conversations (metadata only, no message bodies).
	r.Get("/api/conversations", func(w http.ResponseWriter, r *http.Request) {
		list, err := convStore.ListConversations()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		transport.WriteJSON(w, 200, list)
	})

	// Get a conversation including messages.
	r.Get("/api/conversations/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" || strings.ContainsAny(id, "./\\") {
			http.Error(w, "invalid conversation id", 400)
			return
		}
		c, err := convStore.GetConversation(id)
		if err != nil {
			http.Error(w, "not found", 404)
			return
		}
		transport.WriteJSON(w, 200, c)
	})

	// Get messages for a conversation with optional ?limit=N.
	r.Get("/api/conversations/{id}/messages", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" || strings.ContainsAny(id, "./\\") {
			http.Error(w, "invalid conversation id", 400)
			return
		}
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := parseLimit(v); err == nil && n > 0 {
				limit = n
			}
		}
		msgs, err := convStore.GetContext(id, limit)
		if err != nil {
			http.Error(w, "not found", 404)
			return
		}
		transport.WriteJSON(w, 200, msgs)
	})

	// Reset a conversation (clear messages, keep metadata).
	r.Post("/api/conversations/{id}/reset", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" || strings.ContainsAny(id, "./\\") {
			http.Error(w, "invalid conversation id", 400)
			return
		}
		if err := convStore.ResetConversation(id); err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		transport.WriteJSON(w, 200, map[string]string{"status": "reset"})
	})

	// Delete a conversation entirely.
	r.Delete("/api/conversations/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" || strings.ContainsAny(id, "./\\") {
			http.Error(w, "invalid conversation id", 400)
			return
		}
		if err := convStore.DeleteConversation(id); err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		transport.WriteJSON(w, 200, map[string]string{"status": "deleted"})
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

func parseLimit(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, nil
		}
		n = n*10 + int(c-'0')
		if n > 10000 {
			return 10000, nil
		}
	}
	return n, nil
}

func nodeSupports(n *heartbeat.Node, executor string) bool {
	if n == nil {
		return false
	}
	for _, e := range n.Executors {
		if e == executor {
			return true
		}
	}
	return false
}

// workspaceOwnedBy reports whether ws is exactly nodeWorkspace or nested inside
// it. The comparison is path-segment aware so a registered "/Users/dev/app"
// never claims the unrelated sibling "/Users/dev/application".
func workspaceOwnedBy(nodeWorkspace, ws string) bool {
	if nodeWorkspace == ws {
		return true
	}
	if len(ws) <= len(nodeWorkspace) {
		return false
	}
	if !strings.HasPrefix(ws, nodeWorkspace) {
		return false
	}
	// Guard the separator so prefix matching stops at a path boundary
	// instead of mid-segment ("/Users/dev" must not match "/Users/devX/repo").
	switch nodeWorkspace[len(nodeWorkspace)-1] {
	case '/', '\\':
		return true
	}
	c := ws[len(nodeWorkspace)]
	return c == '/' || c == '\\'
}

// pickDispatchNode is the dispatch endpoint's node decision: which node
// runs a task, or which rejection the caller gets. Extracted from the
// handler so every outcome is testable — the workspace match, both 409
// rejections, the no-workspace fallback and the empty-registry 503.
//
// The no-workspace fallback takes the LOWEST online node id. reg.List()
// ranges a map, so "first online" would be random per call — the same
// nondeterminism the tie-break in selectNodeForWorkspace exists to avoid.
func pickDispatchNode(nodes []*heartbeat.Node, ws, executor string) (string, int, error) {
	if ws != "" {
		if n := selectNodeForWorkspace(nodes, ws, executor); n != nil {
			return n.NodeID, 0, nil
		}
		// The workspace is known but no owner advertises the executor:
		// a capability miss, not a routing miss.
		if len(knownWorkspaceOwners(nodes, ws)) > 0 {
			return "", http.StatusConflict, fmt.Errorf("executor unavailable on node(s) owning workspace: %s", executor)
		}
		return "", http.StatusConflict, fmt.Errorf("no node owns workspace %q (registered: %s)", ws, registeredWorkspaceList(nodes))
	}
	var online []string
	for _, n := range nodes {
		if n != nil && n.Status != "offline" {
			online = append(online, n.NodeID)
		}
	}
	if len(online) == 0 {
		return "", http.StatusServiceUnavailable, errors.New("no nodes available")
	}
	sort.Strings(online)
	return online[0], 0, nil
}

// selectNodeForWorkspace returns the node that owns ws and supports executor,
// preferring the longest matching workspace prefix. It returns "" when no node
// claims the workspace — callers must NOT fall back to an arbitrary node,
// because running a macOS workspace on a Windows node fails with a confusing
// "workspace not found" error at execution time.
//
// Ties on prefix length are broken by NodeID so routing is deterministic
// (reg.List() iterates a map, whose order Go randomizes per call).
func selectNodeForWorkspace(nodes []*heartbeat.Node, ws, executor string) *heartbeat.Node {
	var best *heartbeat.Node
	bestLen := -1
	for _, n := range nodes {
		if n == nil || n.Status == "offline" {
			continue
		}
		if executor != "" && executor != "auto" && !nodeSupports(n, executor) {
			continue
		}
		for _, registered := range n.Workspaces {
			if !workspaceOwnedBy(registered, ws) {
				continue
			}
			if l := len(registered); l > bestLen || (l == bestLen && best != nil && n.NodeID < best.NodeID) {
				best, bestLen = n, l
			}
		}
	}
	return best
}

// knownWorkspaceOwners lists the nodes that registered ws, regardless of
// executor support. Used to tell a genuine routing miss ("no node knows this
// workspace") apart from a capability miss ("only that node is missing the
// executor"), which need different fixes.
func knownWorkspaceOwners(nodes []*heartbeat.Node, ws string) []string {
	var owners []string
	for _, n := range nodes {
		if n == nil || n.Status == "offline" {
			continue
		}
		for _, registered := range n.Workspaces {
			if workspaceOwnedBy(registered, ws) {
				owners = append(owners, n.NodeID)
				break
			}
		}
	}
	return owners
}

// registeredWorkspaceList renders every online node's registered workspaces for
// the routing-miss error message, sorted so the message is stable.
func registeredWorkspaceList(nodes []*heartbeat.Node) string {
	var all []string
	for _, n := range nodes {
		if n == nil {
			continue
		}
		for _, ws := range n.Workspaces {
			all = append(all, n.NodeID+":"+ws)
		}
	}
	if len(all) == 0 {
		return "none"
	}
	sort.Strings(all)
	return strings.Join(all, ", ")
}
