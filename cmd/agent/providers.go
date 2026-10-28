package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// providerAvailabilityTTL bounds how often the worker boots dsh to ask which
// providers the card profile can actually reach. A boot is ~1s of CPU and no
// tokens, but five of them on every catalog refresh would still be waste.
const providerAvailabilityTTL = 10 * time.Minute

// providerProbeModel is a deliberately impossible model. Both answers we want
// arrive before any completion is attempted: a provider with no adapter or no
// credentials fails on the way in, and a healthy provider rejects the model
// name itself.
const providerProbeModel = "__switchyard_probe__"

var (
	availMu       sync.Mutex
	availCache    = map[string]string{}
	availFetched  = map[string]time.Time{}
	availInflight = map[string]bool{}
)

// maxProbeModels bounds how many of a provider's own models one availability
// check will try. A gateway can fail one model upstream (503) while the rest of
// the provider works, so a single failure must not condemn the whole provider.
const maxProbeModels = 3

// modelAvailability reports, per catalog entry, why a card cannot use it ("" is
// usable). The picker asks the running GUI host for its catalog, but a card runs
// the headless profile from the worker's own DSH_HOME, where an adapter may be
// missing or a credential absent. Without this check the picker offers models
// that fail on first use.
//
// A verdict costs one dsh turn, so it is never computed on the caller's path: the
// caller gets whatever is cached, and a model with no verdict yet is reported
// usable while a probe runs in the background. Blocking here stalls the heartbeat
// and makes the node look offline for minutes — that is how this first shipped.
//
// Small providers are probed model by model, because one upstream model can be
// down while its provider is fine. A provider with more models than the per-model
// budget is probed once, and that verdict covers the provider.
func modelAvailability(dshBin string, values []string) map[string]string {
	out := make(map[string]string, len(values))
	availMu.Lock()
	defer availMu.Unlock()
	byProvider := map[string][]string{}
	order := make([]string, 0, len(values))
	for _, v := range values {
		provider := v
		model := ""
		if i := strings.Index(v, "/"); i > 0 {
			provider, model = v[:i], v[i+1:]
		}
		if _, seen := byProvider[provider]; !seen {
			order = append(order, provider)
		}
		byProvider[provider] = append(byProvider[provider], model)
	}
	for _, provider := range order {
		models := byProvider[provider]
		if len(models) <= maxPerModelProbe {
			for _, model := range models {
				key := provider + "/" + model
				reason := availFor(dshBin, key, provider, []string{model})
				if reason != "" {
					out[provider+"/"+model] = reason
				}
			}
			continue
		}
		reason := availFor(dshBin, providerKey(provider), provider, models[:maxProbeModels])
		if reason == "" {
			continue
		}
		for _, model := range models {
			out[provider+"/"+model] = reason
		}
	}
	return out
}

// maxPerModelProbe is how many models of one provider are still cheap enough to
// verify one by one.
const maxPerModelProbe = 8

func providerKey(provider string) string { return "provider:" + provider }

// availFor returns the cached verdict for key, starting a background probe when
// there is none or it has gone stale.
func availFor(dshBin, key, provider string, models []string) string {
	if fetched, ok := availFetched[key]; ok {
		if reason := availCache[key]; reason != "" || time.Since(fetched) <= providerAvailabilityTTL {
			if time.Since(fetched) <= providerAvailabilityTTL {
				return reason
			}
		}
	}
	if !availInflight[key] {
		availInflight[key] = true
		dispatch := cardProvider(provider)
		go func() {
			verdict := probeProviderSamples(dshBin, dispatch, models)
			availMu.Lock()
			availCache[key] = verdict
			availFetched[key] = time.Now()
			availInflight[key] = false
			availMu.Unlock()
		}()
	}
	return ""
}

// cardProvider is the provider name a card actually dispatches. The live catalog
// is named by the GUI host (adit-router); the card profile declares the same
// router as adit-9router, so the check must use the dispatch name or it reports
// a missing adapter that dispatch would never ask for.
func cardProvider(provider string) string {
	if provider == "adit-router" {
		return "adit-9router"
	}
	return provider
}

// probeProviderSamples returns the first verdict that clears the provider, and
// the last one otherwise, so one flaky upstream model cannot mark a whole
// provider unusable.
func probeProviderSamples(dshBin, provider string, models []string) string {
	if len(models) > maxProbeModels {
		models = models[:maxProbeModels]
	}
	last := ""
	for _, m := range models {
		last = probeProvider(dshBin, provider, m)
		if last == "" {
			return ""
		}
	}
	return last
}

// probeProvider asks dsh to start one turn on a provider with an unnameable
// model and classifies the failure. Anything that is a complaint about the model
// means the provider itself resolved: adapter present, credentials accepted.
// probeSerial gives each probe the machine to itself. Nine concurrent dsh turns
// made every one of them miss the timeout, which reported working models as
// dead.
var probeSerial = make(chan struct{}, 1)

func probeProvider(dshBin, provider, model string) string {
	probeSerial <- struct{}{}
	defer func() { <-probeSerial }()
	cmd := exec.Command(dshBin, "--profile", "headless", "--json", "Reply with: ok")
	cmd.Env = append(dshEnvWithPermission(dshCommandEnv(), "danger-full-access"),
		"DSH_PROVIDER="+provider, "DSH_MODEL="+model)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return "could not start dsh"
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		// A probe that ran out of time says nothing about the model. Reporting it
		// as unusable would hide a working model behind a reason the user cannot
		// clear, so it stays unverified.
		return ""
	}
	return classifyProviderProbe(out.String())
}

func classifyProviderProbe(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, `{"type":"final"`) {
			continue
		}
		var frame struct {
			Reason struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"reason"`
		}
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			continue
		}
		code := strings.ToUpper(frame.Reason.Error.Code)
		switch code {
		case "NO_ADAPTER":
			return "no adapter in the card profile"
		case "MISSING_CREDENTIAL", "AUTH", "UNAUTHORIZED", "FORBIDDEN":
			return "credential not available to cards"
		case "RATE_LIMIT", "QUOTA", "QUOTA_EXCEEDED":
			return "rate limited right now"
		case "UNKNOWN_MODEL":
			// pi-ai rejects an unconfigured model before it touches credentials,
			// so this code proves nothing either way; only a real model can.
			return "could not verify this provider"
		}
		return ""
	}
	// No final frame at all: inconclusive, not a failure.
	return ""
}
