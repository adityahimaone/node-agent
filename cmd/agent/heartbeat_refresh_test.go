package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// The Overview's "ping all versions" button reaches a worker only through its
// heartbeat response, so the agent must read that body and act on it.
func TestHeartbeatOnceReportsRefreshRequest(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		_, _ = w.Write([]byte(`{"status":"ok","refresh":true}`))
	}))
	defer srv.Close()

	refresh, err := heartbeatOnce(srv.URL, "node-1", "idle", nil)
	if err != nil {
		t.Fatalf("heartbeatOnce: %v", err)
	}
	if !refresh {
		t.Fatal("refresh=true in the heartbeat response must request a re-probe")
	}
	if got := posts.Load(); got != 1 {
		t.Fatalf("heartbeat posts = %d, want 1", got)
	}
}

// A server that predates the refresh flag (or a proxy in between) answers
// something that is not our JSON. That must never trigger a re-register loop.
func TestHeartbeatOnceIgnoresUnparsableBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	refresh, err := heartbeatOnce(srv.URL, "node-1", "idle", nil)
	if err != nil {
		t.Fatalf("heartbeatOnce: %v", err)
	}
	if refresh {
		t.Fatal("a non-JSON body must not request a refresh")
	}
}
