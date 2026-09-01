package snapshot

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/transition"
	"flatten-workspace/internal/workspace"
)

func acknowledgedEventLog(t *testing.T) *eventlog.Service {
	t.Helper()
	var mu sync.Mutex
	var events []eventlog.Event
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/events" {
			mu.Lock()
			defer mu.Unlock()
			records := make([]map[string]any, 0, len(events))
			previous := "genesis"
			for _, ev := range events {
				hash := strings.Repeat("a", 64)
				records = append(records, map[string]any{"seq": ev.Seq, "prev_hash": previous, "hash": hash, "evidence": ev})
				previous = hash
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"events": records})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		defer r.Body.Close()
		var ev eventlog.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded", "id": ev.ID, "seq": ev.Seq, "hash": strings.Repeat("a", 64),
		})
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}))
	t.Cleanup(sidecar.Close)
	log, err := eventlog.New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

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

func TestVerifyLoadedRejectsValidLookingTypedAuthorityArchive(t *testing.T) {
	state := []byte("id: state-msl-0001\ntype: project\nrevision: 1\nstatus: active\n")
	path := "build-ledger/current/state.yaml"
	log := acknowledgedEventLog(t)
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
	if _, err := VerifyLoaded(loaded, ws.ID); err == nil || !strings.Contains(err.Error(), "untrusted snapshot archive") {
		t.Fatalf("typed snapshot archive verification error = %v, want untrusted archive rejection", err)
	}
}

func TestVerifyLoadedRejectsGlobalTypedAuthorityArchive(t *testing.T) {
	state := []byte("id: state-msl-0001\ntype: project\nrevision: 1\nstatus: active\n")
	path := "build-ledger/current/state.yaml"
	log := acknowledgedEventLog(t)
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
	if _, err := VerifyLoaded(loaded, ws.ID); err == nil || !strings.Contains(err.Error(), "untrusted snapshot archive") {
		t.Fatalf("global typed snapshot archive verification error = %v, want untrusted archive rejection", err)
	}
}

func TestVerifyLoadedRejectsMalformedTypedAuthorityHistory(t *testing.T) {
	log := acknowledgedEventLog(t)
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
	if !strings.Contains(err.Error(), "untrusted snapshot archive") {
		t.Fatalf("VerifyLoaded error = %v, want typed archive rejection", err)
	}
}
