package main

// dsh_web_sync.go — best-effort live sync of agent sessions into the running
// `dsh web` daemon via its unary Typert RPC gateway (POST /api/<ns>/<method>).
//
// Why: the daemon keeps the workspace registry (and every session header) in
// memory and only sees writes it performs itself. node-agent publishes to
// ~/.dsh on disk (publishSessionToLegacyHome), so the UI cannot list agent
// sessions until the daemon restarts. Calling the same RPCs the web frontend
// uses makes the daemon re-index its in-memory registry and persist through
// its own storage domain — the UI updates immediately, no restart, no races.
//
// Soft-fail by design: a sync failure must never abort a finished dispatch.
// Disable entirely with NODE_AGENT_DSH_RPC_SYNC=0.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// dshWebAuthHost is the loopback authority dsh web binds by default. The
// cookie is signed per-authority; the launch-token exchange mints it for the
// Host header actually sent, so keep both in lockstep.
const dshWebAuthHost = "127.0.0.1"

// dshWebCredentialsPath is the launch-token/credential store dsh web writes on
// startup (same home the daemon reads; node-agent intentionally does not use
// its isolated home here because the daemon is the user's ~/.dsh).
func dshWebCredentialsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".dsh", ".credentials.yaml"), nil
}

// dshWebLogPath is where the daemon's launch banner (with ?token=...) lands.
func dshWebLogPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".dsh", "web.log"), nil
}

var dshLaunchTokenRe = regexp.MustCompile(`token=([A-Za-z0-9_-]+)`)

// dshWebLaunchToken reads the daemon's current launch token from web.log.
func dshWebLaunchToken() (string, error) {
	path, err := dshWebLogPath()
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	m := dshLaunchTokenRe.FindAllSubmatch(raw, -1)
	if len(m) == 0 {
		return "", fmt.Errorf("no launch token in %s (daemon possibly not started)", path)
	}
	// web.log accumulates one banner per daemon start; only the LAST token is
	// valid (the daemon rotates the secret each launch).
	return string(m[len(m)-1][1]), nil
}

// dshCookieName returns the per-authority cookie name
// dsh-auth-<base64url(sha256(authority))>.
func dshCookieName(authority string) string {
	sum := sha256.Sum256([]byte(authority))
	return "dsh-auth-" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// dshWebBaseURL is the loopback origin the daemon listens on. Overridable in
// tests via dshWebBaseURLFor; prod always hits 127.0.0.1:3080.
var dshWebBaseURLFor = func() string { return "http://127.0.0.1:3080" }

// dshWebRPC post the Typert client-request envelope and decode the response.
// Token exchange happens once per call to keep the cookie fresh (30d)
// without persisting secrets.
func dshWebRPC(ctx context.Context, client *http.Client, method string, args map[string]any) (map[string]any, error) {
	token, err := dshWebLaunchToken()
	if err != nil {
		return nil, err
	}
	cookie, err := dshMintCookie(ctx, client, token)
	if err != nil {
		return nil, fmt.Errorf("mint auth cookie: %w", err)
	}

	rpcID, err := newUUID()
	if err != nil {
		return nil, err
	}
	envelope := map[string]any{
		"type":    "client-request",
		"rpcId":   rpcID,
		"method":  method,
		"payload": map[string]any{"args": args},
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode rpc envelope: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		dshWebBaseURLFor()+"/api/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookie)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rpc %s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("rpc %s read: %w", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc %s: http %d: %s", method, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var envelopeResp struct {
		Result struct {
			OK    bool            `json:"ok"`
			Value json.RawMessage `json:"value"`
			Error *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &envelopeResp); err != nil {
		return nil, fmt.Errorf("rpc %s: decode response: %w", method, err)
	}
	if !envelopeResp.Result.OK {
		code, msg := "unknown", "unknown"
		if e := envelopeResp.Result.Error; e != nil {
			code, msg = e.Code, e.Message
		}
		return nil, fmt.Errorf("rpc %s failed: %s: %s", method, code, msg)
	}
	var value map[string]any
	if len(envelopeResp.Result.Value) > 0 {
		if err := json.Unmarshal(envelopeResp.Result.Value, &value); err != nil {
			return nil, fmt.Errorf("rpc %s: decode value: %w", method, err)
		}
	}
	return value, nil
}

// dshMintCookie exchanges the launch token for a session cookie (GET /?token=)
// and returns it in Cookie header form.
func dshMintCookie(ctx context.Context, client *http.Client, token string) (string, error) {
	// Do NOT follow the 303 redirect: it goes to `/` with no token (401) and
	// the cookie we need is on this first response.
	noFollow := &http.Client{Timeout: client.Timeout}
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		dshWebBaseURLFor()+"/?token="+token, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Host", dshWebAuthHost)
	resp, err := noFollow.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused. The 303 body is irrelevant.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	cookies := resp.Cookies()
	for _, c := range cookies {
		if strings.HasPrefix(c.Name, "dsh-auth-") {
			return c.Name + "=" + c.Value, nil
		}
	}
	return "", fmt.Errorf("token exchange returned no dsh-auth cookie (status %d)", resp.StatusCode)
}

// syncDSHWebSession makes the daemon see `sessionID` under the workspace for
// `workspacePath`, the same way the web UI would create it. Soft-fail: returns
// error for logging; callers ignore it for dispatch success.
func syncDSHWebSession(ctx context.Context, sessionID, workspacePath string) error {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(workspacePath) == "" {
		return fmt.Errorf("sync requires session id and workspace path")
	}
	if os.Getenv("NODE_AGENT_DSH_RPC_SYNC") == "0" {
		return nil
	}
	client := &http.Client{Timeout: 8 * time.Second}

	// 1. Ensure the workspace exists in the daemon (idempotent by path).
	wsValue, err := dshWebRPC(ctx, client, "workspace/create",
		map[string]any{"request": map[string]any{"path": workspacePath}})
	if err != nil {
		return fmt.Errorf("workspace/create: %w", err)
	}
	wsMap, _ := wsValue["workspace"].(map[string]any)
	workspaceID, _ := wsMap["workspaceId"].(string)
	if workspaceID == "" {
		return fmt.Errorf("workspace/create: no workspaceId in response")
	}

	// 2. Attach the agent's session id to that workspace.
	_, err = dshWebRPC(ctx, client, "session/create",
		map[string]any{"request": map[string]any{"sessionId": sessionID, "workspaceId": workspaceID}})
	if err != nil {
		return fmt.Errorf("session/create: %w", err)
	}
	return nil
}

