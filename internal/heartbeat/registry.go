package heartbeat

import (
	"sync"
	"time"
)

type Node struct {
	NodeID     string
	Hostname   string
	Workspaces []string
	LastSeen   time.Time
	Status     string // idle|busy|offline
	Route      string // direct|relay(sin)
	CurAddr    string
}

type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	ttl   time.Duration
}

func New(ttl time.Duration) *Registry { return &Registry{nodes: map[string]*Node{}, ttl: ttl} }

func (r *Registry) Upsert(n *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n.LastSeen = time.Now()
	if n.Status=="" { n.Status="idle" }
	r.nodes[n.NodeID]=n
}
func (r *Registry) Heartbeat(id, status string) bool {
	r.mu.Lock(); defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok { return false }
	n.LastSeen=time.Now()
	if status!="" { n.Status=status }
	return true
}
func (r *Registry) Get(id string) (*Node,bool) { r.mu.RLock(); defer r.mu.RUnlock(); n,ok:=r.nodes[id]; return n,ok }
func (r *Registry) List() []*Node {
	r.mu.RLock(); defer r.mu.RUnlock()
	out:=make([]*Node,0,len(r.nodes))
	for _,n:=range r.nodes { c:=*n; if time.Since(n.LastSeen) > r.ttl { c.Status="offline" }; out=append(out,&c) }
	return out
}
