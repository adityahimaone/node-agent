package main

import (
	"testing"
	"time"
)

func TestClassifyProviderProbe(t *testing.T) {
	cases := []struct {
		name   string
		output string
		want   string
	}{
		{"adapter missing", `{"type":"final","reason":{"kind":"error","error":{"code":"NO_ADAPTER","message":"no adapter registered"}}}`, "no adapter in the card profile"},
		{"credential missing", `{"type":"final","reason":{"kind":"error","error":{"code":"AUTH","message":"401"}}}`, "credential not available to cards"},
		{"healthy", `{"type":"final","reason":{"kind":"completed"},"text":"ok"}`, ""},
		{"unverifiable", `{"type":"final","reason":{"kind":"error","error":{"code":"UNKNOWN_MODEL","message":"has no configured model"}}}`, "could not verify this provider"},
		{"no verdict", "dsh: something exploded", ""},
		{"rate limited", `{"type":"final","reason":{"kind":"error","error":{"code":"RATE_LIMIT","message":"Free usage limit reached"}}}`, "rate limited right now"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyProviderProbe(c.output); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

// A provider with more models than the per-model budget is probed once, and that
// verdict has to reach every one of its models.
func TestModelAvailabilityFansOutLargeProviderVerdicts(t *testing.T) {
	availMu.Lock()
	availCache = map[string]string{providerKey("big"): "rate limited right now"}
	availFetched = map[string]time.Time{providerKey("big"): time.Now()}
	availInflight = map[string]bool{}
	availMu.Unlock()

	values := make([]string, 0, maxPerModelProbe+1)
	for i := 0; i <= maxPerModelProbe; i++ {
		values = append(values, "big/m"+string(rune('a'+i)))
	}
	got := modelAvailability("/nonexistent/dsh", values)
	for _, v := range values {
		if got[v] != "rate limited right now" {
			t.Fatalf("%s: got %q, want the provider verdict", v, got[v])
		}
	}
}

func TestCardProviderTranslatesRouter(t *testing.T) {
	if got := cardProvider("adit-router"); got != "adit-9router" {
		t.Fatalf("got %q, a card dispatches the headless router name", got)
	}
	if got := cardProvider("gemini"); got != "gemini" {
		t.Fatalf("got %q, unrelated providers must pass through", got)
	}
}

// A model with no verdict must not be reported as broken: the probe runs in
// the background, and inventing a reason would hide working models behind an
// error the user cannot clear.
func TestModelAvailabilityNeverBlocksNorInventsFailures(t *testing.T) {
	availMu.Lock()
	availCache = map[string]string{}
	availFetched = map[string]time.Time{}
	availInflight = map[string]bool{}
	availMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		got := modelAvailability("/nonexistent/dsh", []string{"gemini/gemini-3.8-flash"})
		if reason, ok := got["gemini/gemini-3.8-flash"]; ok {
			t.Errorf("got %q, want no invented reason for an unprobed model", reason)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("providerAvailability blocked the caller")
	}

	deadline := time.After(5 * time.Second)
	for {
		availMu.Lock()
		verdict, ok := availCache["gemini/gemini-3.8-flash"]
		busy := availInflight["gemini/gemini-3.8-flash"]
		availMu.Unlock()
		if ok && !busy {
			if verdict == "" {
				t.Fatal("a missing dsh binary must produce a verdict, not silence")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("the background probe never recorded a verdict")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
