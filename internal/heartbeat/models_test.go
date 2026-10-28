package heartbeat

import "testing"

// The shipped web profile declares exactly six models under its own provider.
// An earlier reader treated a list that small as implausible and slid to the
// router profile, so the picker advertised 1400 models the worker could not run.
const webProfileSnippet = `
- insert:
    id: providers
    config:
      adit-router:
        endpoint: https://router.local/v1
        models:
          - id: main
            name: Main
          - id: cheap
            name: Cheap
          - id: free
            name: Free
          - id: f8
            name: F8
          - id: codex
            name: Codex
          - id: claude-office
            name: Claude Office
      unused:
        models:
          - id: not-mine
`

func TestParseCordisModelsKeepsSmallLists(t *testing.T) {
	entries, provider := ParseCordisModels([]byte(webProfileSnippet))
	if provider != "adit-router" {
		t.Fatalf("provider = %q, want adit-router", provider)
	}
	if len(entries) != 7 {
		t.Fatalf("entries = %d, want 7 — every provider block counts, got %v", len(entries), entries)
	}
	if entries[0].Value != "adit-router/main" {
		t.Fatalf("first value = %q, want adit-router/main", entries[0].Value)
	}
}

// Router profile ids contain slashes of their own. Only the provider prefix may
// be split off, otherwise "ih/wan-2.6" would be read as provider "ih".
const routerProfileSnippet = `
- insert:
    id: providers
    config:
      adit-9router:
        models:
          - id: ih/wan-2.6
            name: Wan
          - id: main
            name: Main
`

func TestParseCordisModelsKeepsSlashInModelID(t *testing.T) {
	entries, _ := ParseCordisModels([]byte(routerProfileSnippet))
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	if entries[0].Value != "adit-9router/ih/wan-2.6" {
		t.Fatalf("value = %q", entries[0].Value)
	}
}

func TestCatalogETagTracksValues(t *testing.T) {
	a := []ModelEntry{{Value: "x/1"}, {Value: "x/2"}}
	b := []ModelEntry{{Value: "x/1"}, {Value: "x/2"}}
	c := []ModelEntry{{Value: "x/1"}, {Value: "x/3"}}
	if catalogETag(a) != catalogETag(b) {
		t.Fatal("same values must keep the same etag")
	}
	if catalogETag(a) == catalogETag(c) {
		t.Fatal("a changed list must change the etag")
	}
	if len(catalogETag(a)) != 16 {
		t.Fatalf("etag length = %d, want 16", len(catalogETag(a)))
	}
}

const cmdListSnippet = `Available models:

  deepseek/deepseek-v4-pro      Flagship reasoning model
  deepseek/deepseek-flash       Fast tier
  anthropic/claude-sonnet-4     200k context
`

func TestParseCommandCodeModels(t *testing.T) {
	entries := ParseCommandCodeModels(cmdListSnippet)
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (%v)", len(entries), entries)
	}
	if entries[0].Value != "deepseek/deepseek-v4-pro" || entries[0].Sublabel != "Flagship reasoning model" {
		t.Fatalf("first entry = %+v", entries[0])
	}
	if entries[0].Provider != "deepseek" {
		t.Fatalf("provider = %q, want deepseek", entries[0].Provider)
	}
}

// `cmd --list-models` prints ids flush left with two-or-more spaces before the
// description, plus a banner and a docs footer that look like rows. Requiring
// leading whitespace parsed zero models on the current CLI, which made the
// create-task picker show a fallback list on a node that could run every model.
func TestParseCommandCodeModelsRealLayout(t *testing.T) {
	out := "Available models  ·  94 models\n" +
		"\nOpen Source\n" +
		"\n" +
		"deepseek/deepseek-v4-pro               hybrid-attention long-context reasoning\n" +
		"claude-sonnet-5-5                       fast general model\n" +
		"  indented/id                          two leading spaces still parses\n" +
		"\nDocs:  https://commandcode.ai/docs/reference/cli/models\n" +
		"Pass the full id  to pin a model\n" +
		"typesafe/jev                           prose row, not a model\n"

	got := ParseCommandCodeModels(out)
	var ids []string
	for _, m := range got {
		ids = append(ids, m.Value)
	}
	want := []string{"deepseek/deepseek-v4-pro", "claude-sonnet-5-5", "indented/id"}
	if len(ids) != len(want) {
		t.Fatalf("parsed %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("parsed %v, want %v", ids, want)
		}
	}
	if got[0].Provider != "deepseek" {
		t.Fatalf("provider = %q, want deepseek", got[0].Provider)
	}
}
