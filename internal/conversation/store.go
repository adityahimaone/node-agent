package conversation

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type Conversation struct {
	ID         string    `json:"id"`
	Workspace  string    `json:"workspace"`
	Executor   string    `json:"executor"`
	CreatedAt  time.Time `json:"created_at"`
	LastActive time.Time `json:"last_active"`
	Messages   []Message `json:"messages"`
}

type Info struct {
	ID         string    `json:"id"`
	Workspace  string    `json:"workspace"`
	Executor   string    `json:"executor"`
	CreatedAt  time.Time `json:"created_at"`
	LastActive time.Time `json:"last_active"`
	Count      int       `json:"message_count"`
}

// Store keeps conversations as one JSON file per conversation under
// $HOME/.node-agent/conversations/<id>.json. The node-agent worker is
// single-threaded (one job at a time), so a file per conversation with a
// process-wide mutex is enough — no SQLite dependency needed.
type Store struct {
	dir string
	mu  sync.Mutex
}

func NewStore(home string) (*Store, error) {
	dir := filepath.Join(home, ".node-agent", "conversations")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func (s *Store) EnsureConversation(workspace, executor string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ents, err := os.ReadDir(s.dir)
	if err == nil {
		var best *Conversation
		for _, e := range ents {
			if e.IsDir() {
				continue
			}
			c, err := s.read(e.Name())
			if err != nil || c.Workspace != workspace || c.Executor != executor {
				continue
			}
			if best == nil || c.LastActive.After(best.LastActive) {
				best = c
			}
		}
		if best != nil {
			return best.ID, nil
		}
	}
	id := fmt.Sprintf("%x", time.Now().UnixNano())
	c := &Conversation{
		ID:         id,
		Workspace:  workspace,
		Executor:   executor,
		CreatedAt:  time.Now(),
		LastActive: time.Now(),
		Messages:   nil,
	}
	if err := s.write(c); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) AppendMessage(convID, role, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.read(convID + ".json")
	if err != nil {
		return err
	}
	c.Messages = append(c.Messages, Message{
		ID:        fmt.Sprintf("%x", time.Now().UnixNano()),
		Role:      role,
		Content:   content,
		CreatedAt: time.Now(),
	})
	c.LastActive = time.Now()
	return s.write(c)
}

// GetContext returns up to `limit` most recent messages in chronological order.
func (s *Store) GetContext(convID string, limit int) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.read(convID + ".json")
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit >= len(c.Messages) {
		return c.Messages, nil
	}
	return c.Messages[len(c.Messages)-limit:], nil
}

// GetConversation returns full conversation metadata plus messages.
func (s *Store) GetConversation(convID string) (*Conversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read(convID + ".json")
}

// ListConversations returns metadata for all conversations sorted by last_active desc.
func (s *Store) ListConversations() ([]Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []Info
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		c, err := s.read(e.Name())
		if err != nil {
			continue
		}
		out = append(out, Info{
			ID:         c.ID,
			Workspace:  c.Workspace,
			Executor:   c.Executor,
			CreatedAt:  c.CreatedAt,
			LastActive: c.LastActive,
			Count:      len(c.Messages),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastActive.After(out[j].LastActive) })
	return out, nil
}

// DeleteConversation removes a conversation file.
func (s *Store) DeleteConversation(convID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.Remove(s.path(convID))
}

func (s *Store) ResetConversation(convID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.read(convID + ".json")
	if err != nil {
		return err
	}
	c.Messages = nil
	c.LastActive = time.Now()
	return s.write(c)
}

func (s *Store) Close() error { return nil }

func (s *Store) read(name string) (*Conversation, error) {
	id := name
	if filepath.Ext(name) == ".json" {
		id = name[:len(name)-5]
	}
	b, err := os.ReadFile(s.path(id))
	if err != nil {
		return nil, err
	}
	var c Conversation
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) write(c *Conversation) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(c.ID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(c.ID))
}
