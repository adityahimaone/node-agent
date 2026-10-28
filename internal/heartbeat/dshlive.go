package heartbeat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// tokenURLRe finds the access URL the harness prints when it boots the web GUI:
//
//	http://127.0.0.1:3080/?token=SljWopVH2XhcEOrxAf48iA6hboraBl_Eqck8got3oKs
//
// The newest one wins; older lines belong to earlier boots whose tokens the host
// has already rotated out.
var tokenURLRe = regexp.MustCompile(`(http://(?:127\.0\.0\.1|localhost|\[::1\]):(\d+))/+\?token=([A-Za-z0-9_.\-]+)`)

// dshLogTail is how much of web.log to scan. The file grows by a few lines per
// boot and a running session writes nothing to it that matters here.
const dshLogTail = 256 << 10

// DSHLiveEndpoint locates the running DSH web host on this machine.
//
// There is no config for this: the harness mints a token per boot and prints it
// to <dshHome>/web.log. That printed URL is the only place the token exists, and
// exchanging it for the dsh-auth cookie is the only way in.
func DSHLiveEndpoint(dshHome string) (base, token string, err error) {
	if dshHome == "" {
		return "", "", fmt.Errorf("no DSH_HOME on this node")
	}
	path := filepath.Join(dshHome, "web.log")
	f, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", "", err
	}
	off := int64(0)
	if st.Size() > dshLogTail {
		off = st.Size() - dshLogTail
	}
	if _, err := f.Seek(off, 0); err != nil {
		return "", "", err
	}
	buf := make([]byte, st.Size()-off)
	if _, err := f.Read(buf); err != nil && len(bytes.TrimSpace(buf)) == 0 {
		return "", "", err
	}
	matches := tokenURLRe.FindAllStringSubmatch(string(buf), -1)
	if len(matches) == 0 {
		return "", "", fmt.Errorf("no access URL in %s", path)
	}
	last := matches[len(matches)-1]
	return last[1], last[3], nil
}

// liveCatalogResponse mirrors the host's /api RPC envelope for
// session/modelCatalog. Only the fields this node consumes are declared; the
// host sends reasoning metadata and per-provider failures that the picker does
// not need.
type liveCatalogResponse struct {
	Result struct {
		OK    bool `json:"ok"`
		Value struct {
			Default struct {
				Provider string `json:"provider"`
				Model    string `json:"model"`
			} `json:"default"`
			Routable []string `json:"routableProviders"`
			Groups   []struct {
				ID     string `json:"id"`
				Name   string `json:"name"`
				Models []struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"models"`
			} `json:"groups"`
			Failures []struct {
				Provider string `json:"provider"`
				Message  string `json:"message"`
			} `json:"failures"`
		} `json:"value"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	} `json:"result"`
}

// LiveDSHCatalog asks the running DSH host which models it can actually route.
//
// This is the answer the GUI renders, and it cannot be derived from files: an
// account-backed provider's models come from that provider's /models endpoint at
// runtime, so a profile only names the provider, never its models. Reading the
// profile file instead is what made the board advertise thirteen invented models
// while the GUI showed five providers.
func LiveDSHCatalog(dshHome string, timeout time.Duration) ModelCatalog {
	cat := ModelCatalog{Executor: "dsh", Profile: "live", FetchedAt: time.Now().Unix()}
	base, token, err := DSHLiveEndpoint(dshHome)
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = err.Error()
		return cat
	}
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = "cookie jar: " + err.Error()
		return cat
	}
	client := &http.Client{Jar: jar, Timeout: timeout}

	// Exchanging the token sets dsh-auth-<id>; the API rejects the bare token.
	login := base + "/?token=" + url.QueryEscape(token)
	if resp, err := client.Get(login); err != nil {
		cat.Source = "unverified"
		cat.Detail = "DSH host unreachable at " + base + ": " + err.Error()
		return cat
	} else {
		resp.Body.Close()
	}

	body := `{"type":"client-request","rpcId":"node-agent","method":"session/modelCatalog","payload":{"args":{}}}`
	req, err := http.NewRequest(http.MethodPost, base+"/api/session/modelCatalog", strings.NewReader(body))
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = err.Error()
		return cat
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		cat.Source = "unverified"
		cat.Detail = "modelCatalog RPC: " + err.Error()
		return cat
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		cat.Source = "unverified"
		cat.Detail = fmt.Sprintf("modelCatalog RPC: HTTP %d (is the DSH web GUI running on this node?)", resp.StatusCode)
		return cat
	}
	var decoded liveCatalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		cat.Source = "unverified"
		cat.Detail = "decode modelCatalog: " + err.Error()
		return cat
	}
	if !decoded.Result.OK {
		cat.Source = "unverified"
		cat.Detail = "modelCatalog refused: " + decoded.Result.Error.Code + " " + decoded.Result.Error.Message
		return cat
	}

	value := decoded.Result.Value
	for _, group := range value.Groups {
		label := group.Name
		if label == "" {
			label = group.ID
		}
		for _, m := range group.Models {
			name := m.Name
			if name == "" {
				name = m.ID
			}
			value := m.ID
			if group.ID != "" {
				value = group.ID + "/" + m.ID
			}
			cat.Models = append(cat.Models, ModelEntry{
				Value:    value,
				Label:    name,
				Badge:    label,
				Provider: group.ID,
			})
		}
	}
	if len(cat.Models) == 0 {
		cat.Source = "unverified"
		cat.Detail = "the DSH host advertises no routable models at " + base
		return cat
	}
	cat.Source = "probed"
	cat.Detail = fmt.Sprintf("live catalog from %s · %d providers", base, len(value.Groups))
	if len(value.Failures) > 0 {
		names := make([]string, 0, len(value.Failures))
		for _, f := range value.Failures {
			name := f.Provider
			if name == "" {
				name = f.Message
			}
			names = append(names, name)
		}
		cat.Detail += " · unavailable: " + strings.Join(names, ", ")
	}
	cat.ETag = catalogETag(cat.Models)
	return cat
}
