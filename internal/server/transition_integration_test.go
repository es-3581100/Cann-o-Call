package server

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/transition"
	"flatten-workspace/internal/workspace"
)

type failingTransitionWriter struct{ err error }

func (w failingTransitionWriter) Append(eventlog.Event) (eventlog.Event, error) {
	return eventlog.Event{}, w.err
}

type failOnSecondTransitionWriter struct{ calls int }

func (w *failOnSecondTransitionWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
	w.calls++
	if w.calls == 2 {
		return eventlog.Event{}, errors.New("baseline append failed")
	}
	ev.Seq = int64(w.calls)
	ev.Hash = "test-hash"
	ev.RustAck = &eventlog.RustAcknowledgement{ID: ev.ID, Sequence: ev.Seq, Hash: strings.Repeat("a", 64)}
	return ev, nil
}

func acknowledgingRustSidecar(t *testing.T) *httptest.Server {
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
		if r.Method == http.MethodPost && r.URL.Path == "/checkpoints" {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "checkpoint_created"})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		defer r.Body.Close()
		var ev eventlog.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Fatalf("decode Rust event: %v", err)
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
	return sidecar
}

func newTransitionTestServer(t *testing.T) *Server {
	t.Helper()
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("AUTHORITY_TOKEN", "test-token")
	t.Setenv("RUST_LEDGER_URL", acknowledgingRustSidecar(t).URL)
	return New()
}

func TestFreshServerWithoutRustStartsButRejectsSemanticAdmission(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("AUTHORITY_TOKEN", "test-token")
	t.Setenv("RUST_LEDGER_URL", "")
	s := New()
	ws := &workspace.Workspace{ID: "workspace-without-rust", Files: map[string]*workspace.File{}}
	s.store.Add(ws)
	_, _, err := s.opUpdateState(
		httptest.NewRequest(http.MethodPost, "/api/workspaces/workspace-without-rust/build-ledger/current/state", nil),
		ws,
		[]byte("id: state-1\ntype: project\nrevision: 1\nstatus: active\n"),
	)
	if err == nil || !strings.Contains(err.Error(), "durable_recorder_unavailable") {
		t.Fatalf("unconfigured server admission error = %v, want durable_recorder_unavailable", err)
	}
	if _, exists := ws.Files["build-ledger/current/state.yaml"]; exists {
		t.Fatal("unconfigured server admitted a semantic state update")
	}
}

func zipBytes(t *testing.T, name, contents string) []byte {
	t.Helper()
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	f, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(contents)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestImportAdmissionFailureLeavesStoreUnchanged(t *testing.T) {
	s := newTransitionTestServer(t)
	s.Authority = transition.New(failingTransitionWriter{errors.New("append failed")}, nil)
	request := httptest.NewRequest("POST", "/api/workspaces/from-zip", nil)

	if _, _, err := s.opImportZip(request, zipBytes(t, "safe.txt", "safe"), "safe.zip"); err == nil {
		t.Fatal("import accepted despite durable append failure")
	}
	if got := s.store.Summaries(); len(got) != 0 {
		t.Fatalf("store changed after rejected import: %#v", got)
	}
}

func TestImportBaselineAdmissionFailureLeavesStoreUnchanged(t *testing.T) {
	s := newTransitionTestServer(t)
	s.Authority = transition.New(&failOnSecondTransitionWriter{}, nil)
	request := httptest.NewRequest("POST", "/api/workspaces/from-zip", nil)

	if _, _, err := s.opImportZip(request, zipBytes(t, "build-ledger/current/state.yaml", "id: baseline\n"), "baseline.zip"); err == nil {
		t.Fatal("import accepted despite durable baseline failure")
	}
	if got := s.store.Summaries(); len(got) != 0 {
		t.Fatalf("store changed after rejected baseline: %#v", got)
	}
}

func TestStateAdmissionFailureLeavesWorkspaceUnchanged(t *testing.T) {
	s := newTransitionTestServer(t)
	ws := &workspace.Workspace{ID: "workspace-1", Files: map[string]*workspace.File{}}
	s.store.Add(ws)
	s.Authority = transition.New(failingTransitionWriter{errors.New("append failed")}, nil)
	request := httptest.NewRequest("POST", "/api/workspaces/workspace-1/build-ledger/current/state", nil)
	body := []byte("id: state-1\ntype: project\nrevision: 1\nstatus: active\n")

	if _, _, err := s.opUpdateState(request, ws, body); err == nil {
		t.Fatal("state update accepted despite durable append failure")
	}
	if _, exists := ws.Files["build-ledger/current/state.yaml"]; exists {
		t.Fatal("workspace file changed before durable transition admission")
	}
}

func TestStateTransitionUsesAuthorityBindingAndReadExportsDoNotAppend(t *testing.T) {
	s := newTransitionTestServer(t)
	ws := &workspace.Workspace{ID: "workspace-1", Files: map[string]*workspace.File{}}
	s.store.Add(ws)
	request := httptest.NewRequest("POST", "/api/workspaces/workspace-1/build-ledger/current/state", nil)
	body := []byte("id: state-1\ntype: project\nrevision: 1\nstatus: active\n")
	_, receipt, err := s.opUpdateState(request, ws, body)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.Events.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "transition.authority.accepted" || receipt.EventID != events[0].ID {
		t.Fatalf("receipt was not bound to authority event: receipt=%#v events=%#v", receipt, events)
	}
	before := len(events)
	for _, path := range []string{"/api/workspaces/workspace-1/snapshot", "/api/workspaces/workspace-1/verification-report"} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatalf("GET %s status = %d", path, response.Code)
		}
	}
	events, err = s.Events.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != before {
		t.Fatalf("GET export/report appended events: before=%d after=%d", before, len(events))
	}
}

func TestZipImportKeepsUnsafeBlobQuarantined(t *testing.T) {
	s := newTransitionTestServer(t)
	request := httptest.NewRequest("POST", "/api/workspaces/from-zip", nil)
	ws, _, err := s.opImportZip(request, zipBytes(t, "../recovered.txt", "quarantined"), "unsafe.zip")
	if err != nil {
		t.Fatal(err)
	}
	if len(ws.QuarantinedBlobs) != 1 || ws.QuarantinedBlobs[0].Status != "quarantined" {
		t.Fatalf("unsafe ZIP blob was not retained as quarantined: %#v", ws.QuarantinedBlobs)
	}
	if _, exists := ws.Files[ws.QuarantinedBlobs[0].SafePath]; exists {
		t.Fatal("import implicitly admitted a quarantined blob")
	}
}

func TestMaterializationAdmissionPrecedesExternalWrite(t *testing.T) {
	s := newTransitionTestServer(t)
	s.AllowAbsoluteRoot = true
	ws := &workspace.Workspace{
		ID: "workspace-1",
		Files: map[string]*workspace.File{
			"safe.txt": {Data: []byte("safe")},
		},
	}
	s.store.Add(ws)
	s.Authority = transition.New(failingTransitionWriter{errors.New("append failed")}, nil)
	root := filepath.Join(t.TempDir(), "materialized")

	_, _, _, err := s.opMaterialize(httptest.NewRequest(http.MethodPost, "/api/workspaces/workspace-1/materialize", nil), ws, root, true)
	if err == nil {
		t.Fatal("materialization accepted despite durable admission failure")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("external materialization occurred before admission: stat err=%v", err)
	}
}

func TestMaterializationAppliesAfterAdmissionAtControlledAbsoluteDataDir(t *testing.T) {
	s := newTransitionTestServer(t)
	ws := &workspace.Workspace{
		ID: "workspace-1",
		Files: map[string]*workspace.File{
			"safe.txt": {Data: []byte("safe")},
		},
	}
	s.store.Add(ws)

	root, written, receipt, err := s.opMaterialize(httptest.NewRequest(http.MethodPost, "/api/workspaces/workspace-1/materialize", nil), ws, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if receipt == nil || len(written) != 1 {
		t.Fatalf("materialization result = receipt=%#v written=%#v", receipt, written)
	}
	if _, err := os.Stat(filepath.Join(root, "safe.txt")); err != nil {
		t.Fatalf("admitted materialization was not applied: %v", err)
	}
	events, err := s.Events.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || receipt.EventID != events[0].ID {
		t.Fatalf("external materialization lacked prior durable admission: receipt=%#v events=%#v", receipt, events)
	}
}

func TestCheckpointIsDerivedAndDoesNotAppendSemanticTransition(t *testing.T) {
	s := newTransitionTestServer(t)
	request := httptest.NewRequest(http.MethodPost, "/api/checkpoints", nil)
	request.Header.Set("X-Authority-Token", "test-token")
	response := httptest.NewRecorder()

	s.createCheckpoint(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("checkpoint status = %d, want %d", response.Code, http.StatusCreated)
	}
	events, err := s.Events.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("derived checkpoint appended semantic history: %#v", events)
	}
}

func TestReplayBlocksMalformedTypedAuthorityEnvelope(t *testing.T) {
	s := newTransitionTestServer(t)
	ws := &workspace.Workspace{ID: "workspace-1", Files: map[string]*workspace.File{}}
	s.store.Add(ws)
	if _, err := s.Events.Append(eventlog.Event{
		ID:          "corrupt-typed-envelope",
		Type:        "transition.authority.accepted",
		WorkspaceID: ws.ID,
		Action:      "transition.accepted",
		Status:      "accepted",
		Details:     map[string]any{"accepted_transition": "not an accepted transition"},
	}); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/workspaces/workspace-1/replay/verify", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("replay status for malformed typed authority envelope = %d, want %d", response.Code, http.StatusInternalServerError)
	}
}
