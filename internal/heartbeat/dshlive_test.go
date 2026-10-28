package heartbeat

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// The live catalog is the only answer that matches the GUI, so it must survive a
// host that speaks the harness's RPC envelope and must fail loudly when the host
// is not there at all.
func TestLiveDSHCatalogReadsHostRPC(t *testing.T) {
	var gotCookie bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("token") == "tok-123":
			http.SetCookie(w, &http.Cookie{Name: "dsh-auth-test", Value: "v1.x.y", Path: "/"})
			w.WriteHeader(http.StatusSeeOther)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/modelCatalog":
			if c, err := r.Cookie("dsh-auth-test"); err == nil && c.Value == "v1.x.y" {
				gotCookie = true
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"type":"server-response","rpcId":"node-agent","result":{"ok":true,"value":{`+
				`"default":{"provider":"deepseek-account","model":"deepseek-flash"},`+
				`"groups":[`+
				`{"id":"deepseek-account","name":"DeepSeek Account","models":[{"id":"deepseek-flash","name":"DeepSeek-V4.1-Flash"}]},`+
				`{"id":"adit-router","name":"9router","models":[{"id":"claude-office","name":"claude-office"}]}]}}}`)
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}
	}))
	defer srv.Close()

	home := t.TempDir()
	log := fmt.Sprintf("banner\n  http://%s/?token=tok-123\n", srv.Listener.Addr().String())
	if err := os.WriteFile(filepath.Join(home, "web.log"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}

	cat := LiveDSHCatalog(home, 0)
	if cat.Source != "probed" {
		t.Fatalf("source = %q (%s)", cat.Source, cat.Detail)
	}
	if !gotCookie {
		t.Fatal("the RPC was called without the dsh-auth cookie")
	}
	want := []ModelEntry{
		{Value: "deepseek-account/deepseek-flash", Label: "DeepSeek-V4.1-Flash", Badge: "DeepSeek Account", Provider: "deepseek-account"},
		{Value: "adit-router/claude-office", Label: "claude-office", Badge: "9router", Provider: "adit-router"},
	}
	if len(cat.Models) != len(want) {
		t.Fatalf("models = %d, want %d", len(cat.Models), len(want))
	}
	for i := range want {
		if cat.Models[i] != want[i] {
			t.Errorf("model[%d] = %+v, want %+v", i, cat.Models[i], want[i])
		}
	}
}

func TestLiveDSHCatalogWithoutHost(t *testing.T) {
	cat := LiveDSHCatalog(t.TempDir(), 0)
	if cat.Source != "unverified" {
		t.Fatalf("source = %q, want unverified", cat.Source)
	}
	if len(cat.Models) != 0 {
		t.Fatalf("invented %d models with no host", len(cat.Models))
	}
	if cat.Detail == "" {
		t.Fatal("an unverified catalog must say why")
	}
}
