package heartbeat

import (
	"testing"
	"time"
)


// A worker that re-probes and finds nothing changed sends no payload — the etag
// gate is deliberate. The stored fetched_at must still advance, or a UI that
// judges freshness by that timestamp waits forever for a refresh it cannot see.
func TestHeartbeatAdvancesFetchedAtWhenEtagIsUnchanged(t *testing.T) {
	r := New(time.Minute)
	r.Upsert(&Node{NodeID: "mac"})
	cat := ModelCatalog{Executor: "dsh", Profile: "live", Source: "probed", FetchedAt: 100, ETag: "abc"}
	cat.Models = []ModelEntry{{Value: "a"}}
	if !r.SetModelCatalog("mac", cat) {
		t.Fatal("SetModelCatalog failed")
	}
	sums := map[string]ModelSummary{"dsh:live": {Profile: "live", Source: "probed", FetchedAt: 900, ETag: "abc", Count: 1}}
	if !r.HeartbeatWithState("mac", "online", nil, sums) {
		t.Fatal("HeartbeatWithState failed")
	}
	got, ok := r.ModelCatalog("mac", "dsh:live")
	if !ok {
		t.Fatal("catalog vanished")
	}
	if got.FetchedAt != 900 {
		t.Fatalf("got fetched_at %d, want the re-probe time 900", got.FetchedAt)
	}
	if len(r.ConsumeWantedModels("mac")) != 0 {
		t.Fatal("an unchanged etag must not ask for the payload again")
	}
}
