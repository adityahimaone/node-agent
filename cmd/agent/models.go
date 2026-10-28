package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"node-agent/internal/heartbeat"
)

// defaultDSHProfile is the profile cards are dispatched with
// (`dsh --profile headless`). The dsh web GUI reads a different profile, which
// is why the create-task modal used to advertise a model list the worker could
// not run.
const defaultDSHProfile = "headless"

// modelProber owns this node's model catalogs: it probes on demand, keeps the
// result until the control plane asks for a refresh, and reports only etag-sized
// summaries on the ordinary heartbeat.
type modelProber struct {
	mu       sync.Mutex
	catalogs map[string]heartbeat.ModelCatalog
	ttl      time.Duration
}

func newModelProber() *modelProber {
	p := &modelProber{catalogs: map[string]heartbeat.ModelCatalog{}, ttl: 10 * time.Minute}
	p.refresh(nil)
	return p
}

// dshProfile is configurable so a user who dispatches with another profile can
// advertise that one instead of editing code.
func dshProfile() string {
	if v := strings.TrimSpace(os.Getenv("NODE_AGENT_DSH_PROFILE")); v != "" {
		return v
	}
	return defaultDSHProfile
}

// dshHomeForProbe prefers the isolated home the agent dispatches with, then the
// plain ~/.dsh. The isolated profiles dir is normally a symlink to ~/.dsh, but
// reading both means a node with a real copy still gets a catalog.
func dshHomeForProbe() string {
	for _, home := range []string{dshIsolatedHome(), dshRealHome()} {
		if home == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(home, "profiles", dshProfile())); err == nil {
			return home
		}
	}
	return dshIsolatedHome()
}

func dshRealHome() string {
	if h := strings.TrimSpace(os.Getenv("DSH_HOME")); h != "" {
		return h
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".dsh")
}

// liveDSHProfile is the pseudo-profile whose catalog comes from the running DSH
// host instead of a file. It is what the GUI shows, so it is what the picker
// offers by default: the alternative is a list the user cannot reconcile with
// their own harness.
const liveDSHProfile = "live"

// probe rebuilds the catalog for one key, or every key when key is empty.
func (p *modelProber) probe(key string) {
	profile := dshProfile()
	switch {
	case key == "dsh" || key == "dsh:"+liveDSHProfile:
		p.catalogs["dsh:"+liveDSHProfile] = cardRunnable(heartbeat.LiveDSHCatalog(dshRealHome(), 0))
	case key == "dsh:"+profile:
		p.catalogs["dsh:"+profile] = heartbeat.DSHCatalog(dshHomeForProbe(), profile)
	case key == "commandcode":
		p.catalogs["commandcode"] = heartbeat.CommandCodeCatalog(commandCodeBin())
	case key == "":
		// Both: "live" is what the picker shows by default, "<profile>" stays
		// available as the truthful list for what dispatch will actually run.
		p.catalogs["dsh:"+liveDSHProfile] = cardRunnable(heartbeat.LiveDSHCatalog(dshRealHome(), 0))
		p.catalogs["dsh:"+profile] = heartbeat.DSHCatalog(dshHomeForProbe(), profile)
		p.catalogs["commandcode"] = heartbeat.CommandCodeCatalog(commandCodeBin())
	}
}

// cardRunnable marks the entries of a live catalog that the card profile cannot
// reach. The GUI host and a card run different profiles from different DSH_HOMEs,
// so a provider the user sees in their own harness can still be unusable here;
// the picker shows those models disabled with the reason instead of accepting a
// choice that fails on the first request.
func cardRunnable(cat heartbeat.ModelCatalog) heartbeat.ModelCatalog {
	if len(cat.Models) == 0 {
		return cat
	}
	bin := findBin("dsh")
	if bin == "" {
		return cat
	}
	values := make([]string, 0, len(cat.Models))
	for _, m := range cat.Models {
		values = append(values, m.Value)
	}
	avail := modelAvailability(bin, values)
	for i := range cat.Models {
		if reason := avail[cat.Models[i].Value]; reason != "" {
			cat.Models[i].Disabled = true
			cat.Models[i].Note = reason
		}
	}
	cat.ETag = heartbeat.CatalogETag(cat.Models)
	return cat
}

// refresh re-probes what the control plane asked for (empty = everything) and
// probes anything it has never seen.
func (p *modelProber) refresh(want []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(want) == 0 {
		if len(p.catalogs) == 0 {
			p.probe("")
		}
		return
	}
	for _, key := range want {
		if key == "*" {
			p.probe("")
			continue
		}
		p.probe(key)
	}
}

// beat returns the summaries to advertise and the payloads owed for want.
func (p *modelProber) beat(want []string) (map[string]heartbeat.ModelSummary, map[string]heartbeat.ModelCatalog) {
	p.refresh(want)
	p.mu.Lock()
	defer p.mu.Unlock()
	summaries := heartbeat.Summaries(p.catalogs)
	var payloads map[string]heartbeat.ModelCatalog
	for _, key := range want {
		if key == "*" {
			payloads = map[string]heartbeat.ModelCatalog{}
			for k, c := range p.catalogs {
				payloads[k] = c
			}
			break
		}
		if c, ok := p.catalogs[key]; ok {
			if payloads == nil {
				payloads = map[string]heartbeat.ModelCatalog{}
			}
			payloads[key] = c
		}
	}
	return summaries, payloads
}
