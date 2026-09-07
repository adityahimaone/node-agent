package session

import (
	"testing"
	"time"
)

func TestResultStoreAcceptsOnlyFirstTerminalResult(t *testing.T) {
	m := NewManager(1 * time.Minute)
	d := m.NewDelivery("task-1", "board", "/workspace")
	if !m.AcceptAck(d.ID) {
		t.Fatal("first ack must be accepted")
	}
	if !m.AcceptResult(d.ID, Result{TaskID: "task-1", Success: true}) {
		t.Fatal("first result must be accepted")
	}
	if m.AcceptResult(d.ID, Result{TaskID: "task-1", Success: false}) {
		t.Fatal("duplicate result must be rejected")
	}
	got, ok := m.Result(d.ID)
	if !ok || !got.Success {
		t.Fatalf("stored result = %#v, ok=%v", got, ok)
	}
}

func TestUnackedDeliveryExpires(t *testing.T) {
	m := NewManager(1 * time.Millisecond)
	d := m.NewDelivery("task-1", "board", "/workspace")
	time.Sleep(3 * time.Millisecond)
	if len(m.Expired()) != 1 || m.Expired()[0].ID != d.ID {
		t.Fatalf("expired deliveries = %#v", m.Expired())
	}
}
