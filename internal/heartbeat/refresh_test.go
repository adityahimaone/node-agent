package heartbeat

import (
	"testing"
	"time"
)

// The Overview's "ping all versions" button asks every worker to re-probe its
// tool versions. The flag has to survive until the next heartbeat (the agent
// only learns about it from a response) and must be consumed exactly once, or
// a worker would re-register on every beat forever.
func TestRequestRefreshAllConsumedOncePerNode(t *testing.T) {
	r := New(time.Minute)
	r.Upsert(&Node{NodeID: "mac", Hostname: "mac"})
	r.Upsert(&Node{NodeID: "win", Hostname: "win"})

	if n := r.RequestRefreshAll(); n != 2 {
		t.Fatalf("RequestRefreshAll = %d, want 2", n)
	}
	if !r.ConsumeRefresh("mac") {
		t.Fatal("mac: first heartbeat should be told to refresh")
	}
	if r.ConsumeRefresh("mac") {
		t.Fatal("mac: refresh flag must be one-shot")
	}
	if !r.ConsumeRefresh("win") {
		t.Fatal("win: independent flag expected")
	}
}

func TestConsumeRefreshUnknownNodeIsFalse(t *testing.T) {
	r := New(time.Minute)
	if r.ConsumeRefresh("nope") {
		t.Fatal("unknown node must never report a refresh")
	}
}
