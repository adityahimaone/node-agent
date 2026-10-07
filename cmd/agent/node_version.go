package main

import (
	"sort"
	"strconv"
	"strings"
)

// commandCodeMinNodeMajor is the Node major Command Code requires;
// an older node fails every run in ~140ms with a version error
// instead of running the task.
const commandCodeMinNodeMajor = 22

// parseNodeVersion parses a "v22.8.0" directory name into
// its major, minor and patch components.
func parseNodeVersion(v string) ([3]int, bool) {
	var out [3]int
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	if v == "" {
		return out, false
	}
	parts := strings.Split(v, ".")
	if len(parts) == 0 || len(parts) > 3 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return [3]int{}, false
		}
		out[i] = n
	}
	return out, true
}

// parseNodeMajor extracts the major version from a `node --version`
// style string ("v22.8.0"). ok is false when nothing parseable
// is present.
func parseNodeMajor(v string) (int, bool) {
	ver, ok := parseNodeVersion(v)
	if !ok {
		return 0, false
	}
	return ver[0], true
}

// parseNvmSettings reads nvm-windows' settings.txt content and
// returns the nvm root (installed versions live in root/vX.Y.Z)
// and the symlink path nvm repoints (the nodejs install dir).
// Either may be empty when the line is absent.
func parseNvmSettings(content string) (root, linkPath string) {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "root:"); ok {
			root = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "path:"); ok {
			linkPath = strings.TrimSpace(v)
		}
	}
	return root, linkPath
}

// pickNodeVersions returns the installed nvm versions whose
// major is at least minMajor, lowest first — the smallest
// switches that satisfy the requirement. A broken install is
// not filtered here (only a direct probe can tell); the
// caller walks the list and skips candidates that fail it.
func pickNodeVersions(entries []string, minMajor int) []string {
	type versioned struct {
		name string
		v    [3]int
	}
	var picks []versioned
	for _, name := range entries {
		v, ok := parseNodeVersion(name)
		if !ok || v[0] < minMajor {
			continue
		}
		picks = append(picks, versioned{name, v})
	}
	sort.SliceStable(picks, func(i, j int) bool {
		for k := 0; k < 3; k++ {
			if picks[i].v[k] != picks[j].v[k] {
				return picks[i].v[k] < picks[j].v[k]
			}
		}
		return false
	})
	ordered := make([]string, len(picks))
	for i, p := range picks {
		ordered[i] = p.name
	}
	return ordered
}

// nodeVersionTooOldOutput reports whether a Command Code run failed
// with the Node-version error — the signature the worker can fix by
// switching node, as opposed to any other failure.
func nodeVersionTooOldOutput(out string) bool {
	return strings.Contains(out, "needs Node.js")
}
