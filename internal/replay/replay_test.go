package replay

import (
	"encoding/base64"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
)

func b64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
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
