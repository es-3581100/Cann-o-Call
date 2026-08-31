package snapshot

import (
	"bytes"
	"encoding/base64"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/workspace"
)

func TestReadAndVerifySnapshot(t *testing.T) {
	state := []byte(`id: state-msl-0001
type: project
revision: 1
status: active
`)

	p := "build-ledger/current/state.yaml"

	ws := &workspace.Workspace{
		ID: "ws-test",
		Files: map[string]*workspace.File{
			p: {
				Path: p,
				Data: state,
			},
		},
	}

	events := []eventlog.Event{
		{
			WorkspaceID: "ws-test",
			Status:      "accepted",
			Action:      "build_ledger.baseline.recorded",
			Details: map[string]any{
				"files": map[string]any{
					p: base64.StdEncoding.EncodeToString(state),
				},
			},
		},
	}

	buf := new(bytes.Buffer)

	if err := WriteZip(ws, events, nil, "snapshot-test", buf); err != nil {
		t.Fatalf("WriteZip failed: %v", err)
	}

	loaded, err := ReadZip(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadZip failed: %v", err)
	}

	result, err := VerifyLoaded(loaded, "ws-test")
	if err != nil {
		t.Fatalf("VerifyLoaded failed: %v", err)
	}

	if result["projection_ok"] != true {
		t.Fatalf("expected projection_ok true, got: %+v", result)
	}

	if result["replay_ok"] != true {
		t.Fatalf("expected replay_ok true, got: %+v", result)
	}
}
