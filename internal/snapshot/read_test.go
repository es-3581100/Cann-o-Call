package snapshot

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/transition"
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

func TestVerifyLoadedAcceptsValidTypedAuthorityHistory(t *testing.T) {
	state := []byte("id: state-msl-0001\ntype: project\nrevision: 1\nstatus: active\n")
	path := "build-ledger/current/state.yaml"
	log, err := eventlog.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	authority := transition.New(log, nil)
	resultData, err := json.Marshal(map[string]any{
		"legacy_action": "build_ledger.state.updated",
		"legacy_details": map[string]any{
			"path":           path,
			"content_base64": base64.StdEncoding.EncodeToString(state),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Propose(transition.ProposedTransition{
		TransitionID: "transition-1",
		ProposalID:   "proposal-1",
		RequestID:    "request-1",
		Prior:        authority.Projection().Ref(),
		Operation:    "upsert",
		Entity:       "ws-typed",
		Node:         "build-ledger",
		ResultData:   resultData,
	}); err != nil {
		t.Fatal(err)
	}
	events, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	ws := &workspace.Workspace{ID: "ws-typed", Files: map[string]*workspace.File{path: {Path: path, Data: state}}}
	buf := new(bytes.Buffer)
	if err := WriteZip(ws, events, nil, "snapshot-typed", buf); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadZip(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyLoaded(loaded, ws.ID)
	if err != nil {
		t.Fatalf("VerifyLoaded rejected valid typed authority history: %v", err)
	}
	if verified["replay_ok"] != true {
		t.Fatalf("expected replay_ok true, got: %+v", verified)
	}
}

func TestVerifyLoadedReplaysTargetWorkspaceFromGlobalTypedHistory(t *testing.T) {
	state := []byte("id: state-msl-0001\ntype: project\nrevision: 1\nstatus: active\n")
	path := "build-ledger/current/state.yaml"
	log, err := eventlog.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	authority := transition.New(log, nil)
	otherData, err := json.Marshal(map[string]any{"legacy_action": "workspace.imported_zip", "legacy_details": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Propose(transition.ProposedTransition{
		TransitionID: "other-transition",
		ProposalID:   "other-proposal",
		RequestID:    "other-request",
		Prior:        authority.Projection().Ref(),
		Operation:    "upsert",
		Entity:       "ws-other",
		Node:         "workspace",
		ResultData:   otherData,
	}); err != nil {
		t.Fatal(err)
	}
	targetData, err := json.Marshal(map[string]any{
		"legacy_action": "build_ledger.state.updated",
		"legacy_details": map[string]any{
			"path":           path,
			"content_base64": base64.StdEncoding.EncodeToString(state),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Propose(transition.ProposedTransition{
		TransitionID: "target-transition",
		ProposalID:   "target-proposal",
		RequestID:    "target-request",
		Prior:        authority.Projection().Ref(),
		Operation:    "upsert",
		Entity:       "ws-target",
		Node:         "build-ledger",
		ResultData:   targetData,
	}); err != nil {
		t.Fatal(err)
	}
	events, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	ws := &workspace.Workspace{ID: "ws-target", Files: map[string]*workspace.File{path: {Path: path, Data: state}}}
	buf := new(bytes.Buffer)
	if err := WriteZip(ws, events, nil, "snapshot-global-history", buf); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadZip(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Events) != len(events) {
		t.Fatalf("snapshot events = %d, want complete global history of %d", len(loaded.Events), len(events))
	}
	verified, err := VerifyLoaded(loaded, ws.ID)
	if err != nil {
		t.Fatalf("VerifyLoaded rejected valid global history: %v", err)
	}
	if verified["replay_ok"] != true {
		t.Fatalf("expected target workspace replay_ok true, got: %+v", verified)
	}
}

func TestVerifyLoadedRejectsMalformedTypedAuthorityHistory(t *testing.T) {
	log, err := eventlog.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(eventlog.Event{
		ID:          "corrupt-typed-envelope",
		Type:        "transition.authority.accepted",
		WorkspaceID: "ws-corrupt",
		Action:      "transition.accepted",
		Status:      "accepted",
		Details:     map[string]any{"accepted_transition": "not an accepted transition"},
	}); err != nil {
		t.Fatal(err)
	}
	events, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	loaded := &Loaded{Files: map[string][]byte{}, Events: events}
	_, err = VerifyLoaded(loaded, "ws-corrupt")
	if err == nil {
		t.Fatal("VerifyLoaded accepted malformed typed authority history")
	}
	if !strings.Contains(err.Error(), "accepted event \"corrupt-typed-envelope\" payload") {
		t.Fatalf("VerifyLoaded error = %v, want typed envelope rejection", err)
	}
}
