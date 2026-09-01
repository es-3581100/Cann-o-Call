package replay

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/transition"
)

func b64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func TestReplayUnderstandsAuthorityEnvelopeAndIgnoresOpaqueEvents(t *testing.T) {
	path := "build-ledger/current/state.yaml"
	data := []byte("id: state-1\ntype: project\nrevision: 1\nstatus: active\n")
	accepted := transition.AcceptedTransition{Proposal: transition.ProposedTransition{
		Entity: "ws-1",
		ResultData: mustJSON(t, map[string]any{
			"legacy_action": "build_ledger.state.updated",
			"legacy_details": map[string]any{
				"path":           path,
				"content_base64": b64(data),
			},
		}),
	}}
	payload := mustJSON(t, accepted)
	events := []eventlog.Event{
		{ID: "opaque", Status: "accepted", Action: "build_ledger.state.updated"},
		{Type: "transition.authority.accepted", Status: "accepted", Action: "transition.accepted", Details: map[string]any{"accepted_transition": json.RawMessage(payload)}},
	}
	got, err := BuildLedgerProjectionFromEvents(events, "ws-1")
	if err != nil {
		t.Fatal(err)
	}
	want := projection.BuildFromFiles(map[string][]byte{path: data})
	if !projection.Equivalent(got, want) {
		t.Fatalf("authority envelope not replayed: got %s want %s", got.Hash, want.Hash)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReplayEquivalence(t *testing.T) {
	state1 := []byte(`id: state-msl-0001
type: project
revision: 1
status: active
`)

	state2 := []byte(`id: state-msl-0001
type: project
revision: 2
status: in_progress
`)

	path := "build-ledger/current/state.yaml"

	events := []eventlog.Event{
		{
			WorkspaceID: "ws-1",
			Status:      "accepted",
			Action:      "build_ledger.baseline.recorded",
			Details: map[string]any{
				"files": map[string]any{
					path: b64(state1),
				},
			},
		},
		{
			WorkspaceID: "ws-1",
			Status:      "accepted",
			Action:      "build_ledger.state.updated",
			Details: map[string]any{
				"path":           path,
				"content_base64": b64(state2),
			},
		},
	}

	replayed, err := BuildLedgerProjectionFromEvents(events, "ws-1")
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	current := projection.BuildFromFiles(map[string][]byte{
		path: state2,
	})

	if !projection.Equivalent(replayed, current) {
		t.Fatalf(
			"expected replay equivalence\ncurrent: %s\nreplayed: %s",
			current.Hash,
			replayed.Hash,
		)
	}
}
