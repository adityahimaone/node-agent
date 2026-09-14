package conversation

import (
	"fmt"
	"strings"
	"testing"
)

func TestEnsureConversation(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id1, err := store.EnsureConversation("/Users/a/dev", "hermes")
	if err != nil {
		t.Fatal(err)
	}
	if id1 == "" {
		t.Fatal("expected non-empty id")
	}
	// Second call same workspace+executor should return same conversation.
	id2, err := store.EnsureConversation("/Users/a/dev", "hermes")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("expected same conversation id, got %s vs %s", id1, id2)
	}
	// Different executor should create new.
	id3, err := store.EnsureConversation("/Users/a/dev", "codex")
	if err != nil {
		t.Fatal(err)
	}
	if id3 == id1 {
		t.Fatal("expected different id for different executor")
	}
}

func TestAppendAndGetContext(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id, _ := store.EnsureConversation("/w", "hermes")
	if err := store.AppendMessage(id, "user", "hi"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendMessage(id, "assistant", "hello"); err != nil {
		t.Fatal(err)
	}
	msgs, err := store.GetContext(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[1].Role != "assistant" {
		t.Fatalf("wrong order: %s then %s", msgs[0].Role, msgs[1].Role)
	}
	// Limit.
	msgs, _ = store.GetContext(id, 1)
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Content != "hello" {
		t.Fatalf("expected last message, got %s", msgs[0].Content)
	}
}

func TestResetConversation(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	defer store.Close()
	id, _ := store.EnsureConversation("/w", "hermes")
	_ = store.AppendMessage(id, "user", "x")
	msgs, _ := store.GetContext(id, 100)
	if len(msgs) != 1 {
		t.Fatal("expected 1 before reset")
	}
	_ = store.ResetConversation(id)
	msgs, _ = store.GetContext(id, 100)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 after reset, got %d", len(msgs))
	}
}

func TestBuildPromptWithContext(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "hello"},
	}
	prompt := BuildPrompt(msgs, "what's next?")
	if !strings.Contains(prompt, "User: hi") {
		t.Fatal("missing user message")
	}
	if !strings.Contains(prompt, "Assistant: hello") {
		t.Fatal("missing assistant message")
	}
	if !strings.Contains(prompt, "what's next?") {
		t.Fatal("missing new message")
	}
	fmt.Println(prompt)
}
