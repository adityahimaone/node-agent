package session

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Result mirrors transport.ResultRequest for persistence.
type Result struct {
	TaskID  string
	Success bool
	Output  string
	Error   string
}

type Delivery struct {
	ID        string
	TaskID    string
	Board     string
	Workspace string
	CreatedAt time.Time
	ExpiresAt time.Time
	Acked     bool
	Result    *Result
}

type Manager struct {
	mu       sync.Mutex
	leaseTTL time.Duration
	entries  map[string]*Delivery
}

func NewManager(ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = 660 * time.Second
	}
	return &Manager{leaseTTL: ttl, entries: map[string]*Delivery{}}
}

func (m *Manager) NewDelivery(taskID, board, workspace string) Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	id := hex.EncodeToString(raw[:])
	now := time.Now()
	d := &Delivery{ID: id, TaskID: taskID, Board: board, Workspace: workspace, CreatedAt: now, ExpiresAt: now.Add(m.leaseTTL)}
	m.entries[id] = d
	return *d
}

func (m *Manager) AcceptAck(deliveryID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.entries[deliveryID]
	if !ok || d.Acked || d.Result != nil {
		return false
	}
	d.Acked = true
	return true
}

func (m *Manager) AcceptResult(deliveryID string, r Result) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.entries[deliveryID]
	if !ok || d.Result != nil {
		return false
	}
	d.Result = &r
	return true
}

func (m *Manager) Result(deliveryID string) (Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.entries[deliveryID]
	if !ok || d.Result == nil {
		return Result{}, false
	}
	return *d.Result, true
}

func (m *Manager) Expired() []Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var out []Delivery
	for _, d := range m.entries {
		if d.Result == nil && !d.Acked && now.After(d.ExpiresAt) {
			out = append(out, *d)
		}
	}
	return out
}
