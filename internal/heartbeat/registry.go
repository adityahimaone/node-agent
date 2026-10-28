package heartbeat

import (
	"sync"
	"time"
)

type Node struct {
	NodeID     string            `json:"node_id"`
	Hostname   string            `json:"hostname"`
	Workspaces []string          `json:"workspaces"`
	Executors  []string          `json:"executors,omitempty"`
	Versions   map[string]string `json:"versions,omitempty"`
	DSHHealth  *DSHHealth        `json:"dsh_health,omitempty"`
	LastSeen   time.Time         `json:"last_seen"`
	Status     string            `json:"status"`               // idle|busy|offline
	Transports []string          `json:"transports,omitempty"` // e.g. ["http","grpc"]
	Route      string            `json:"route,omitempty"`      // direct|relay(sin)
	CurAddr    string            `json:"cur_addr,omitempty"`
	// ModelSummaries describes each catalog this node can serve (etag + count):
	// small enough to refresh on every heartbeat.
	ModelSummaries map[string]ModelSummary `json:"model_summaries,omitempty"`
	// ModelCatalogs holds the lists themselves. Never serialised into
	// /api/nodes — one dsh router profile is ~100 KB and would bloat the node
	// list for every caller that only wants hostnames.
	ModelCatalogs map[string]ModelCatalog `json:"-"`
}

type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	ttl   time.Duration
	// refresh holds nodes that have been asked to re-probe their tool
	// versions. The request reaches a worker through its next heartbeat
	// response, so the flag lives here until it is consumed. A worker that
	// never beats (offline) keeps its flag harmlessly set.
	refresh map[string]bool
	// wantModels holds catalog keys (executor[:profile]) the control plane does
	// not yet hold at the advertised etag. Same delivery mechanism as refresh.
	wantModels map[string]map[string]bool
}

func New(ttl time.Duration) *Registry {
	return &Registry{nodes: map[string]*Node{}, ttl: ttl, refresh: map[string]bool{}, wantModels: map[string]map[string]bool{}}
}

// RequestRefresh marks every known node for a version re-probe and returns how
// many were marked. Offline nodes are marked too: their flag is delivered when
// they come back rather than being lost.
func (r *Registry) RequestRefreshAll() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.nodes {
		r.refresh[id] = true
	}
	return len(r.nodes)
}

// RequestRefresh marks one node. It reports false for an unknown node so the
// caller can 404 instead of silently queueing work nobody will do.
func (r *Registry) RequestRefresh(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; !ok {
		return false
	}
	r.refresh[id] = true
	return true
}

// ConsumeRefresh reports whether id was asked to re-probe and clears the flag.
// Clearing is what keeps a worker from re-registering on every heartbeat.
func (r *Registry) ConsumeRefresh(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.refresh[id] {
		return false
	}
	delete(r.refresh, id)
	return true
}

func (r *Registry) Upsert(n *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n.LastSeen = time.Now()
	if n.Status == "" {
		n.Status = "idle"
	}
	r.nodes[n.NodeID] = n
}
func (r *Registry) Heartbeat(id, status string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	n.LastSeen = time.Now()
	if status != "" {
		n.Status = status
	}
	return true
}

// HeartbeatWithHealth updates status and (when non-nil) the DSH liveness
// snapshot in one lock.
func (r *Registry) HeartbeatWithHealth(id, status string, h *DSHHealth) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	n.LastSeen = time.Now()
	if status != "" {
		n.Status = status
	}
	if h != nil {
		n.DSHHealth = h
	}
	return true
}
// HeartbeatWithState is HeartbeatWithHealth plus the model catalog summaries the
// node advertises every beat.
func (r *Registry) HeartbeatWithState(id, status string, h *DSHHealth, sums map[string]ModelSummary) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	n.LastSeen = time.Now()
	if status != "" {
		n.Status = status
	}
	if h != nil {
		n.DSHHealth = h
	}
	if sums != nil {
		n.ModelSummaries = sums
		// An etag we do not hold is the whole reason to ask for a payload.
		for key, s := range sums {
			have, ok := n.ModelCatalogs[key]
			if !ok || have.ETag != s.ETag {
				if r.wantModels[id] == nil {
					r.wantModels[id] = map[string]bool{}
				}
				r.wantModels[id][key] = true
				continue
			}
			// Same etag, newer fetch: the worker did re-probe and the list simply
			// did not change. Without this the stored fetched_at would freeze at
			// the first payload, and every picker would keep asking for a
			// refresh it can never observe finishing.
			if s.FetchedAt > have.FetchedAt {
				have.FetchedAt = s.FetchedAt
				have.Source = s.Source
				have.Detail = ""
				n.ModelCatalogs[key] = have
			}
		}
	}
	return true
}

// RequestModelRefresh queues a catalog key for one node. Empty key means "every
// catalog this node advertises", which is what the Settings "Fetch models now"
// button sends. Unknown nodes report false so the caller can 404.
func (r *Registry) RequestModelRefresh(id, key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	if r.wantModels[id] == nil {
		r.wantModels[id] = map[string]bool{}
	}
	if key != "" {
		r.wantModels[id][key] = true
		return true
	}
	for k := range n.ModelSummaries {
		r.wantModels[id][k] = true
	}
	// A node that has never advertised keeps a wildcard so its first
	// heartbeat still hands over a full list.
	if len(n.ModelSummaries) == 0 {
		r.wantModels[id]["*"] = true
	}
	return true
}

// RequestAllModelRefresh queues a full re-fetch for every known node.
func (r *Registry) RequestAllModelRefresh() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, n := range r.nodes {
		if r.wantModels[id] == nil {
			r.wantModels[id] = map[string]bool{}
		}
		for k := range n.ModelSummaries {
			r.wantModels[id][k] = true
		}
		if len(n.ModelSummaries) == 0 {
			r.wantModels[id]["*"] = true
		}
	}
	return len(r.nodes)
}

// ConsumeWantedModels returns the catalog keys this node owes and clears them.
// Clearing keeps a worker from re-sending 100 KB on every beat; if the payload
// never arrives, the next heartbeat re-adds the key because its etag still
// mismatches, so nothing is lost.
func (r *Registry) ConsumeWantedModels(id string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	want := r.wantModels[id]
	if len(want) == 0 {
		return nil
	}
	out := make([]string, 0, len(want))
	for k := range want {
		out = append(out, k)
	}
	delete(r.wantModels, id)
	return out
}

// SetModelCatalog stores one catalog a node sent after being asked for it.
func (r *Registry) SetModelCatalog(id string, cat ModelCatalog) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	if n.ModelCatalogs == nil {
		n.ModelCatalogs = map[string]ModelCatalog{}
	}
	key := cat.CatalogKey()
	n.ModelCatalogs[key] = cat
	if n.ModelSummaries == nil {
		n.ModelSummaries = map[string]ModelSummary{}
	}
	n.ModelSummaries[key] = cat.Summary()
	return true
}

// ModelCatalog reads a stored catalog for one node.
func (r *Registry) ModelCatalog(id, key string) (ModelCatalog, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return ModelCatalog{}, false
	}
	cat, ok := n.ModelCatalogs[key]
	return cat, ok
}

func (r *Registry) Get(id string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return n, ok
}
func (r *Registry) List() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		c := *n
		if time.Since(n.LastSeen) > r.ttl {
			c.Status = "offline"
		}
		out = append(out, &c)
	}
	return out
}
