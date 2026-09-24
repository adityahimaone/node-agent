package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeDSHWeb is a minimal stub of the dsh web unary gateway: it serves the
// launch-token exchange (GET /?token=...) and the two RPC methods the sync
// path calls. It records what it saw so the test can assert envelope shape.
func fakeDSHWeb(t *testing.T, launchToken string) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var calls []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			if r.URL.Query().Get("token") != launchToken {
				t.Logf("token mismatch: got %q want %q", r.URL.Query().Get("token"), launchToken)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Set-Cookie", "dsh-auth-fake=ok; Path=/; Max-Age=2592000; HttpOnly; SameSite=Strict")
			w.Header().Set("Location", "/")
			w.WriteHeader(http.StatusSeeOther)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		cookie := r.Header.Get("Cookie")
		if !strings.Contains(cookie, "dsh-auth-fake=ok") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var envelope struct {
			Type    string         `json:"type"`
			RPCID   string         `json:"rpcId"`
			Method  string         `json:"method"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		calls = append(calls, map[string]any{
			"type": envelope.Type, "method": envelope.Method, "payload": envelope.Payload,
		})
		var value map[string]any
		switch envelope.Method {
		case "workspace/create":
			value = map[string]any{
				"workspace": map[string]any{"workspaceId": "ws-1", "path": "/tmp/x", "sessionIds": []string{}},
			}
		case "session/create":
			value = map[string]any{"sessionId": "session-test-1"}
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"type": "server-response", "rpcId": envelope.RPCID,
			"result": map[string]any{"ok": true, "value": value},
		})
	}))
	return srv, &calls
}

// TestSyncDSHWebSessionFullFlow drives the real syncDSHWebSession against a
// fake daemon and asserts: token exchange happened, both RPCs were called in
// order with the Typert envelope, and the session id was passed through.
func TestSyncDSHWebSessionFullFlow(t *testing.T) {
	srv, calls := fakeDSHWeb(t, "tok123")
	defer srv.Close()
	old := dshWebBaseURLFor
	dshWebBaseURLFor = func() string { return srv.URL }
	t.Cleanup(func() { dshWebBaseURLFor = old })

	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".dsh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".dsh", "web.log"),
		[]byte("dsh web: http://127.0.0.1:3080/?token=tok123\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := syncDSHWebSession(context.Background(), "session-test-1", "/tmp/x"); err != nil {
		t.Fatalf("syncDSHWebSession: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("got %d rpc calls, want 2: %v", len(*calls), *calls)
	}
	ws := (*calls)[0]
	if ws["method"] != "workspace/create" {
		t.Fatalf("first call method=%v want workspace/create", ws["method"])
	}
	wsArgs, _ := ws["payload"].(map[string]any)["args"].(map[string]any)
	req, _ := wsArgs["request"].(map[string]any)
	if req["path"] != "/tmp/x" {
		t.Fatalf("workspace/create path=%v want /tmp/x", req["path"])
	}
	sc := (*calls)[1]
	if sc["method"] != "session/create" {
		t.Fatalf("second call method=%v want session/create", sc["method"])
	}
	scArgs, _ := sc["payload"].(map[string]any)["args"].(map[string]any)
	sreq, _ := scArgs["request"].(map[string]any)
	if sreq["sessionId"] != "session-test-1" || sreq["workspaceId"] != "ws-1" {
		t.Fatalf("session/create request=%v want sessionId=session-test-1 workspaceId=ws-1", sreq)
	}

	// disabled via env
	t.Setenv("NODE_AGENT_DSH_RPC_SYNC", "0")
	before := len(*calls)
	if err := syncDSHWebSession(context.Background(), "session-test-1", "/tmp/x"); err != nil {
		t.Fatalf("disabled sync should return nil, got %v", err)
	}
	if len(*calls) != before {
		t.Fatalf("env-disabled sync still made calls")
	}
}

// TestSyncDSHWebSessionSoftFails verifies sync returns an error (for logging,
// not dispatch failure) when the daemon token is unavailable.
func TestSyncDSHWebSessionSoftFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	err := syncDSHWebSession(context.Background(), "session-x", "/tmp/ws")
	if err == nil {
		t.Fatal("expected error when daemon token is unavailable")
	}
}

// TestDSHWebLaunchTokenEmptyMissing verifies missing/invalid web.log yields an
// error (soft path), so a daemon that never printed a token is non-fatal.
func TestDSHWebLaunchTokenEmptyMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".dsh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := dshWebLaunchToken(); err == nil {
		t.Fatal("expected error for missing web.log")
	}
	if err := os.WriteFile(filepath.Join(home, ".dsh", "web.log"), []byte("no token here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := dshWebLaunchToken(); err == nil {
		t.Fatal("expected error for web.log without token")
	}
	// valid token parses; multiple banners -> last token wins (rotation)
	if err := os.WriteFile(filepath.Join(home, ".dsh", "web.log"),
		[]byte("token=aaaa1111\ndsh web: token=bbb2222\ndsh web: http://127.0.0.1:3080/?token=ccc3333\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok, err := dshWebLaunchToken()
	if err != nil || tok != "ccc3333" {
		t.Fatalf("token=%q err=%v want ccc3333 (last wins)", tok, err)
	}
}