package heartbeat

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

// DSHHealth is the result of a liveness probe of the dsh binary.
type DSHHealth struct {
	OK        bool   `json:"ok"`
	Version   string `json:"version,omitempty"`
	Model     string `json:"model,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Error     string `json:"error,omitempty"`
	CheckedAt int64  `json:"checked_at"`
}

var (
	modelRe    = regexp.MustCompile(`(?m)^\s+model:\s+(\S+)`)
	providerRe = regexp.MustCompile(`(?m)^\s+provider:\s+(\S+)`)
)

// dshProbeEnv matches dshCommandEnv's PATH prepend so the probe finds the
// same binaries (homebrew/usr-local first) the executor relies on.
// dump-config is invoked with DSH_HOME isolation like the executor's
// dshCommandEnv does; this cheap probe deliberately reuses the shared home to
// avoid racing the isolated-home mkdir against a first-run.
func dshProbeEnv() []string {
	return append(os.Environ(), "PATH=/opt/homebrew/bin:/usr/local/bin:"+os.Getenv("PATH"))
}

// ProbeDSH runs `bin --version` (5s cap) and parses model/provider out of the
// caller-provided dump-config output. OK is set when the version command
// succeeded; with the binary missing or a timeout the Error field carries a
// machine-readable code instead.
func ProbeDSH(bin, dumpOut string) (DSHHealth, error) {
	h := DSHHealth{}
	if bin == "" {
		return h, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	cmd.Env = dshProbeEnv()
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		h.Error = "timeout"
		return h, err
	}
	if err != nil {
		h.Error = "no_binary"
		return h, err
	}
	h.OK = true
	h.Version = strings.TrimSpace(string(out))
	h.CheckedAt = time.Now().Unix()
	h.Model = "unknown"
	if m := modelRe.FindStringSubmatch(dumpOut); len(m) > 1 {
		h.Model = m[1]
	}
	if m := providerRe.FindStringSubmatch(dumpOut); len(m) > 1 {
		h.Provider = m[1]
	}
	return h, nil
}

// DSHProbeCache memoizes ProbeDSH results per TTL so the heartbeat goroutine
// never blocks on a re-probe while the cache is fresh.
type DSHProbeCache struct {
	mu  sync.Mutex
	ttl time.Duration
	val DSHHealth
	at  time.Time
}

// NewDSHProbeCache clamps non-positive TTLs to 60s (belt and braces against
// zero/fractional config).
func NewDSHProbeCache(ttl time.Duration) *DSHProbeCache {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &DSHProbeCache{ttl: ttl}
}

// Get returns the cached health while fresh, otherwise probes and caches.
// An empty bin returns the zero health value (checked_at=0, not ok).
func (c *DSHProbeCache) Get(bin string) DSHHealth {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bin == "" {
		return DSHHealth{}
	}
	if c.at.IsZero() || time.Since(c.at) >= c.ttl {
		h, err := ProbeDSH(bin, dumpConfig(bin))
		if err != nil {
			// keep the stale value if we have one rather than erasing it
			if c.at.IsZero() {
				c.val = h
				c.at = time.Now()
			}
			return c.val
		}
		c.val = h
		c.at = time.Now()
	}
	return c.val
}

// dumpConfig runs `bin --profile headless --dump-config` (5s cap) and returns
// its stdout.
func dumpConfig(bin string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--profile", "headless", "--dump-config")
	cmd.Env = dshProbeEnv()
	out, _ := cmd.CombinedOutput()
	return string(out)
}