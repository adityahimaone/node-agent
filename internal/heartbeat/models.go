package heartbeat

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// ModelEntry is one selectable model, shaped like the control plane's
// ModelOption so the picker can render it without a second mapping.
type ModelEntry struct {
	Value    string `json:"value"`
	Label    string `json:"label"`
	Sublabel string `json:"sublabel,omitempty"`
	Badge    string `json:"badge,omitempty"`
	Provider string `json:"provider,omitempty"`
	Pinned   bool   `json:"pinned,omitempty"`
	// Disabled marks a model the running host can route but a card cannot. The
	// picker must show it and refuse it, rather than accept a choice that dies
	// on the first request.
	Disabled bool   `json:"disabled,omitempty"`
	Note     string `json:"note,omitempty"`
}

// ModelCatalog is a worker's answer to "which models can this executor run
// HERE". The key is executor+profile, not executor: a node holds several dsh
// profiles, the GUI shows one of them and cards run another, and answering
// with the wrong one is exactly the bug this type exists to prevent.
type ModelCatalog struct {
	Executor  string       `json:"executor"`
	Profile   string       `json:"profile,omitempty"`
	Source    string       `json:"source"` // probed|unverified
	Detail    string       `json:"detail,omitempty"`
	FetchedAt int64        `json:"fetched_at"`
	ETag      string       `json:"etag"`
	Models    []ModelEntry `json:"models,omitempty"`
}

// CatalogKey identifies a catalog on the wire and in the registry.
func (c ModelCatalog) CatalogKey() string {
	if c.Profile == "" {
		return c.Executor
	}
	return c.Executor + ":" + c.Profile
}

// ModelSummary is the few hundred bytes that ride every heartbeat. The full
// catalog would be ~100 KB for a large dsh profile, so it is only sent when the
// control plane proves it does not have this etag.
type ModelSummary struct {
	Profile   string `json:"profile,omitempty"`
	Source    string `json:"source"`
	FetchedAt int64  `json:"fetched_at"`
	ETag      string `json:"etag"`
	Count     int    `json:"count"`
}

// Summary collapses a catalog into its heartbeat form.
func (c ModelCatalog) Summary() ModelSummary {
	return ModelSummary{Profile: c.Profile, Source: c.Source, FetchedAt: c.FetchedAt, ETag: c.ETag, Count: len(c.Models)}
}

// Summaries is the per-catalog view the agent posts on each heartbeat.
func Summaries(catalogs map[string]ModelCatalog) map[string]ModelSummary {
	if len(catalogs) == 0 {
		return nil
	}
	out := make(map[string]ModelSummary, len(catalogs))
	for k, c := range catalogs {
		out[k] = c.Summary()
	}
	return out
}

// catalogETag hashes the model values so an unchanged profile keeps its etag
// across restarts and the payload is never re-sent for nothing.
// CatalogETag is catalogETag for callers that change a catalog after it was
// built (the worker annotates availability): the etag must describe what is
// actually being advertised, or the control plane keeps its stale copy.
func CatalogETag(entries []ModelEntry) string { return catalogETag(entries) }

func catalogETag(entries []ModelEntry) string {
	h := sha256.New()
	for _, e := range entries {
		h.Write([]byte(e.Value))
		h.Write([]byte{0})
		// Availability is part of the catalog: a worker that can suddenly
		// authenticate a provider must move the etag, or the control plane keeps
		// serving the stale "disabled" marks it already holds.
		if e.Disabled {
			h.Write([]byte(e.Note))
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

var (
	// providerKeyRe finds the provider a models: block belongs to by scanning
	// upwards for the nearest shallower `name:` key.
	providerKeyRe = regexp.MustCompile(`^(\s*)([A-Za-z0-9_.-]+):\s*$`)
	modelIDRe     = regexp.MustCompile(`^\s*-\s*id:\s*([^\s#]+)`)
	modelsLineRe  = regexp.MustCompile(`^(\s*)models:\s*$`)
	// cmdModelRowRe parses one line of `cmd --list-models`: the id starts at the
	// left margin and two-or-more spaces separate it from its description. The id
	// does not have to carry a provider prefix (`claude-sonnet-5-5` does not), and
	// requiring leading whitespace parsed zero rows on the current CLI.
	// cmdSkipPrefixes are non-model lines that share the row shape: the docs
	// footer, the "pass the full id" hint, and prose rows that wrap into a
	// description long enough to look like a model.
	cmdSkipPrefixes = regexp.MustCompile(`^\s*(Docs:|Available models|Pass the full id|cmd |Decision models|typesafe/)`)
	cmdModelRowRe   = regexp.MustCompile(`^\s*([A-Za-z0-9_.:/-]+)\s{2,}(\S.*)$`)
)

// ParseCordisModels extracts the models declared in a dsh profile patch file.
//
// The file is a top-level YAML array of loader patches, so the model list lives
// at providers.<name>.models. A profile may legitimately declare six models
// (the one the dsh web GUI shows) or fourteen hundred (a router passthrough);
// both are valid and neither may be second-guessed here.
func ParseCordisModels(data []byte) ([]ModelEntry, string) {
	lines := strings.Split(string(data), "\n")
	var (
		entries  []ModelEntry
		provider string
		primary  string
	)
	inModels := false
	modelsIndent := 0
	seen := map[string]bool{}

	for i, line := range lines {
		if m := modelsLineRe.FindStringSubmatch(line); m != nil {
			inModels = true
			modelsIndent = len(m[1])
			provider = providerAbove(lines, i, modelsIndent)
			if primary == "" {
				primary = provider
			}
			continue
		}
		if !inModels {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent <= modelsIndent {
			// A sibling of `models:` — the list ended.
			inModels = false
			continue
		}
		m := modelIDRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		id := m[1]
		value := id
		if provider != "" {
			value = provider + "/" + id
		}
		if seen[value] {
			continue
		}
		seen[value] = true
		entries = append(entries, ModelEntry{Value: value, Label: id, Provider: provider, Badge: provider})
	}
	return entries, primary
}

// providerAbove walks up from a models: line to the provider name it belongs to.
// The models block sits at providers.<provider>.models, so the provider key is
// the nearest shallower mapping key.
func providerAbove(lines []string, from, modelsIndent int) string {
	for i := from - 1; i >= 0; i-- {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent >= modelsIndent {
			continue
		}
		if m := providerKeyRe.FindStringSubmatch(line); m != nil {
			key := m[2]
			if key == "providers" || key == "config" {
				return ""
			}
			return key
		}
		if indent == 0 {
			return ""
		}
	}
	return ""
}

// ParseCommandCodeModels extracts model ids from `cmd --list-models` output.
func ParseCommandCodeModels(out string) []ModelEntry {
	var entries []ModelEntry
	seen := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.Contains(line, "·"):
			// Banner line: "Available models  ·  94 models".
			continue
		case cmdSkipPrefixes.MatchString(line):
			// Footers and prose that happen to be indented like a row.
			continue
		}
		m := cmdModelRowRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		value, desc := m[1], strings.TrimSpace(m[2])
		if seen[value] {
			continue
		}
		seen[value] = true
		provider := ""
		if i := strings.Index(value, "/"); i > 0 {
			provider = value[:i]
		}
		entries = append(entries, ModelEntry{Value: value, Label: value, Sublabel: desc, Provider: provider, Badge: provider})
	}
	return entries
}

// CommandCodeCatalog probes the local Command Code CLI. An absent binary is not
// an error worth failing a heartbeat over — it means the node cannot run this
// executor, and Source says so.
func CommandCodeCatalog(cmdPath string) ModelCatalog {
	cat := ModelCatalog{Executor: "commandcode", FetchedAt: time.Now().Unix()}
	if cmdPath == "" {
		cat.Source = "unverified"
		cat.Detail = "cmd not found on this node"
		return cat
	}
	cmd := exec.Command(cmdPath, "--list-models")
	// The CLI is usually a shim (`#!/usr/bin/env node`), and the PATH this process
	// inherited may not contain its own directory or the interpreter it names.
	// Exit 127 from a working binary is that, so the resolved directory is put
	// first and every one of its siblings stays reachable.
	cmd.Env = append(os.Environ(), "PATH="+filepath.Dir(cmdPath)+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.Output()
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = "cmd --list-models failed: " + err.Error()
		return cat
	}
	cat.Models = ParseCommandCodeModels(string(out))
	cat.Source = "probed"
	if len(cat.Models) == 0 {
		cat.Source = "unverified"
		cat.Detail = "cmd --list-models returned no parseable rows"
	}
	cat.ETag = catalogETag(cat.Models)
	return cat
}

// DSHCatalog reads the model list out of one dsh profile on this node.
//
// It reads the profile file rather than shelling out so it behaves the same on
// darwin, windows and linux; dsh has no model-listing flag that does not boot
// the app. dshHome may be the isolated ~/.dsh-nodeagent, whose profiles dir is
// symlinked to ~/.dsh/profiles, so either answers for the same profile.
func DSHCatalog(dshHome, profile string) ModelCatalog {
	cat := ModelCatalog{Executor: "dsh", Profile: profile, FetchedAt: time.Now().Unix()}
	if dshHome == "" {
		cat.Source = "unverified"
		cat.Detail = "no DSH_HOME on this node"
		return cat
	}
	path := filepath.Join(dshHome, "profiles", profile, "cordis.patch.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = fmt.Sprintf("read %s: %v", path, err)
		return cat
	}
	entries, provider := ParseCordisModels(data)
	cat.Models = entries
	cat.Source = "probed"
	cat.Detail = path
	if provider != "" {
		cat.Detail = path + " (provider " + provider + ")"
	}
	if len(entries) == 0 {
		cat.Source = "unverified"
		cat.Detail = "no models declared in " + path
	}
	cat.ETag = catalogETag(cat.Models)
	return cat
}
