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
}

func New(ttl time.Duration) *Registry {
	return &Registry{nodes: map[string]*Node{}, ttl: ttl, refresh: map[string]bool{}}
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
