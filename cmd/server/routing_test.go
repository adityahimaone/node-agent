package main

import (
	"net/http"
	"strings"
	"testing"

	"node-agent/internal/heartbeat"
)

func node(id string, status string, executors []string, workspaces ...string) *heartbeat.Node {
	return &heartbeat.Node{NodeID: id, Status: status, Executors: executors, Workspaces: workspaces}
}

// pickDispatchNode is the handler's decision. Every outcome is
// testable here: the match, both 409s, the no-workspace fallback
// and the 503.
func TestPickDispatchNodeMatchesWorkspaceAndExecutor(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "idle", []string{"shell"}, "/ws"),
		node("other", "idle", []string{"hermes"}, "/ws"),
	}
	id, status, err := pickDispatchNode(nodes, "/ws/sub", "shell")
	if err != nil || status != 0 || id != "mac" {
		t.Fatalf("expected mac, got id=%q status=%d err=%v", id, status, err)
	}
}

func TestPickDispatchNodeExecutorUnavailableOnOwner(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("other", "idle", []string{"hermes"}, "/ws"),
	}
	id, status, err := pickDispatchNode(nodes, "/ws", "shell")
	if id != "" || status != http.StatusConflict || err == nil {
		t.Fatalf("expected 409, got id=%q status=%d err=%v", id, status, err)
	}
	if want := "executor unavailable on node(s) owning workspace: shell"; err.Error() != want {
		t.Fatalf("message = %q, want %q", err.Error(), want)
	}
}

func TestPickDispatchNodeNoOwner(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "idle", []string{"shell"}, "/elsewhere"),
	}
	id, status, err := pickDispatchNode(nodes, "/ws", "shell")
	if id != "" || status != http.StatusConflict || err == nil {
		t.Fatalf("expected 409, got id=%q status=%d err=%v", id, status, err)
	}
	if !strings.Contains(err.Error(), `no node owns workspace "/ws"`) || !strings.Contains(err.Error(), "registered: mac:/elsewhere") {
		t.Fatalf("message must name the workspace and the registered list, got %q", err.Error())
	}
}

// The no-workspace fallback must not depend on reg.List()'s map
// order: the lowest online node id wins, every call.
func TestPickDispatchNodeNoWorkspacePicksLowestOnline(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("zeta", "idle", []string{"shell"}, "/z"),
		node("alpha", "idle", []string{"shell"}, "/a"),
		node("mid", "idle", []string{"shell"}),
	}
	first, status, err := pickDispatchNode(nodes, "", "shell")
	if err != nil || status != 0 || first != "alpha" {
		t.Fatalf("expected alpha, got %q status=%d err=%v", first, status, err)
	}
	for i := 0; i < 50; i++ {
		got, _, err := pickDispatchNode(nodes, "", "shell")
		if err != nil || got != first {
			t.Fatalf("fallback unstable: first=%q got=%q err=%v", first, got, err)
		}
	}
}

func TestPickDispatchNodeNoWorkspaceSkipsOffline(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("aaa", "offline", []string{"shell"}),
		node("zzz", "idle", []string{"shell"}),
	}
	id, status, err := pickDispatchNode(nodes, "", "shell")
	if err != nil || status != 0 || id != "zzz" {
		t.Fatalf("expected zzz (offline skipped), got %q status=%d err=%v", id, status, err)
	}
}

func TestPickDispatchNodeEmptyRegistry(t *testing.T) {
	// No workspace to route on and no node to fall back to: the 503.
	id, status, err := pickDispatchNode(nil, "", "shell")
	if id != "" || status != http.StatusServiceUnavailable || err == nil || err.Error() != "no nodes available" {
		t.Fatalf("expected 503 no nodes available, got id=%q status=%d err=%v", id, status, err)
	}
	// An unroutable workspace on an empty registry is the 409 no-owner
	// rejection, not the 503 — the workspace is the problem.
	id, status, err = pickDispatchNode(nil, "/ws", "shell")
	if id != "" || status != http.StatusConflict || err == nil {
		t.Fatalf("expected 409, got id=%q status=%d err=%v", id, status, err)
	}
}

// The reported bug: a macOS workspace that no node registered used to fall
// through to an arbitrary online node (the Windows one) and fail later with
// "workspace not found" instead of a routing error.
func TestSelectNodeNoFallbackToArbitraryNode(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "idle", []string{"shell"}, "/Users/adityahimawan/Development/next-portfolio-blog"),
		node("windows", "idle", []string{"shell"}, `C:\Development`),
	}
	got := selectNodeForWorkspace(nodes, "/Users/adityahimawan/Development/habbit-tracking-next", "shell")
	if got != nil {
		t.Fatalf("expected no node for unregistered workspace, got %q", got.NodeID)
	}
	if owners := knownWorkspaceOwners(nodes, "/Users/adityahimawan/Development/habbit-tracking-next"); len(owners) != 0 {
		t.Fatalf("expected no owners, got %v", owners)
	}
}

func TestSelectNodePicksOwningNode(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "idle", []string{"shell"}, "/Users/adityahimawan/Development/next-portfolio-blog"),
		node("windows", "idle", []string{"shell"}, `C:\Development`),
	}
	got := selectNodeForWorkspace(nodes, "/Users/adityahimawan/Development/next-portfolio-blog/src", "shell")
	if got == nil || got.NodeID != "mac" {
		t.Fatalf("expected mac, got %v", got)
	}
}

func TestSelectNodeLongestPrefixWins(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("broad", "idle", []string{"shell"}, "/Users/dev"),
		node("narrow", "idle", []string{"shell"}, "/Users/dev/app"),
	}
	got := selectNodeForWorkspace(nodes, "/Users/dev/app/pkg", "shell")
	if got == nil || got.NodeID != "narrow" {
		t.Fatalf("expected longest-prefix node narrow, got %v", got)
	}
}

// reg.List() ranges over a map, so ordering is random per call. Selection must
// still be stable when two nodes claim the same workspace with equal length.
func TestSelectNodeDeterministicTieBreak(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("beta", "idle", []string{"shell"}, "/ws"),
		node("alpha", "idle", []string{"shell"}, "/ws"),
	}
	first := selectNodeForWorkspace(nodes, "/ws/sub", "shell")
	for i := 0; i < 50; i++ {
		got := selectNodeForWorkspace(nodes, "/ws/sub", "shell")
		if got == nil || got.NodeID != first.NodeID {
			t.Fatalf("tie-break unstable: first=%v got=%v", first, got)
		}
	}
	if first.NodeID != "alpha" {
		t.Fatalf("expected lowest NodeID alpha, got %q", first.NodeID)
	}
}

// A registered "/Users/dev/app" must not claim the sibling
// "/Users/dev/application" — plain string prefixes used to over-match.
func TestWorkspaceOwnedByRespectsPathBoundary(t *testing.T) {
	cases := []struct {
		registered, ws string
		want           bool
	}{
		{"/ws", "/ws", true},
		{"/ws", "/ws/sub", true},
		{"/ws/", "/ws/sub", true},
		{"/ws", "/wsx", false},
		{"/ws", "/wsx/repo", false},
		{"/Users/dev", "/Users/developerXYZ", false},
		{"/Users/dev", "/Users/dev/repo", true},
		{`C:\Development`, `C:\Development\saas`, true},
		{`C:\Development`, `C:\DevelopmentX\saas`, false},
	}
	for _, c := range cases {
		if got := workspaceOwnedBy(c.registered, c.ws); got != c.want {
			t.Errorf("workspaceOwnedBy(%q, %q) = %v, want %v", c.registered, c.ws, got, c.want)
		}
	}
}

func TestSelectNodeSkipsOfflineAndWrongExecutor(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "offline", []string{"shell"}, "/ws"),
		node("other", "idle", []string{"hermes"}, "/ws"),
	}
	if got := selectNodeForWorkspace(nodes, "/ws", "shell"); got != nil {
		t.Fatalf("expected nil (offline + no executor), got %q", got.NodeID)
	}
	// The workspace is known, but the only owner cannot run the executor —
	// that is a capability miss, not a routing miss.
	owners := knownWorkspaceOwners(nodes, "/ws")
	if len(owners) != 1 || owners[0] != "other" {
		t.Fatalf("expected owner other, got %v", owners)
	}
}

func TestRegisteredWorkspaceListStable(t *testing.T) {
	nodes := []*heartbeat.Node{
		node("mac", "idle", []string{"shell"}, "/z", "/a"),
		node("win", "idle", []string{"shell"}, `C:\Dev`),
	}
	got := registeredWorkspaceList(nodes)
	want := "mac:/a, mac:/z, win:C:\\Dev"
	if got != want {
		t.Fatalf("registeredWorkspaceList = %q, want %q", got, want)
	}
	if registeredWorkspaceList(nil) != "none" {
		t.Fatal("expected none for empty registry")
	}
}
