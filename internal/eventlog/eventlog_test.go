package eventlog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGoForwardingContractRejectsBeforeForwardAndRetriesSidecarFailure(t *testing.T) {
	var mu sync.Mutex
	var forwards []Event
	failSidecar := true
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var forwarded Event
		if err := json.NewDecoder(r.Body).Decode(&forwarded); err != nil {
			t.Fatalf("decode forwarded Go event: %v", err)
		}
		mu.Lock()
		forwards = append(forwards, forwarded)
		fail := failSidecar
		mu.Unlock()
		if fail {
			http.Error(w, "sidecar write rejected", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded",
			"id":     forwarded.ID,
			"seq":    forwarded.Seq,
			"hash":   strings.Repeat("a", 64),
		})
	}))
	defer sidecar.Close()

	service, err := New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatalf("new event log: %v", err)
	}
	if _, err := service.Append(Event{}); err == nil {
		t.Fatal("missing Go event ID must be rejected before sidecar forwarding")
	}
	mu.Lock()
	if len(forwards) != 0 {
		t.Fatalf("rejected Go event was forwarded %d times", len(forwards))
	}
	mu.Unlock()

	event := Event{ID: "go-approved", Type: acceptedTransitionEventType, Action: "transition.accepted", Status: "accepted", CreatedAt: time.Now().UTC()}
	if _, err := service.Append(event); err == nil {
		t.Fatal("sidecar write failure must propagate to Go")
	}
	if events, err := service.List(); err != nil || len(events) != 0 {
		t.Fatalf("failed sidecar write created Go phantom: events=%d err=%v", len(events), err)
	}

	mu.Lock()
	failSidecar = false
	mu.Unlock()
	accepted, err := service.Append(event)
	if err != nil {
		t.Fatalf("retry accepted by sidecar: %v", err)
	}
	if accepted.Seq != 1 {
		t.Fatalf("retry sequence = %d, want 1", accepted.Seq)
	}
	if accepted.RustAck == nil || accepted.RustAck.ID != accepted.ID || accepted.RustAck.Sequence != accepted.Seq || accepted.RustAck.Hash != strings.Repeat("a", 64) {
		t.Fatalf("Go event did not retain Rust acknowledgement: %#v", accepted.RustAck)
	}
	if _, err := service.CheckedEvents(); err != nil {
		t.Fatalf("Rust acknowledgement must not alter Go hash validation: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(forwards) != 2 || forwards[0].Seq != 1 || forwards[1].Seq != 1 {
		t.Fatalf("forwarded sequences = %#v, want exactly failed/retried seq 1", forwards)
	}
}

func TestGoForwardingRejectsMismatchedAcknowledgement(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded",
			"id":     "wrong-id",
			"seq":    1,
			"hash":   strings.Repeat("a", 64),
		})
	}))
	defer sidecar.Close()

	service, err := New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(Event{ID: "expected-id", Action: "apply", CreatedAt: time.Now().UTC()}); err == nil {
		t.Fatal("mismatched sidecar acknowledgement accepted")
	}
	if events, err := service.List(); err != nil || len(events) != 0 {
		t.Fatalf("mismatched acknowledgement created Go event: events=%d err=%v", len(events), err)
	}
}

func TestAcceptedTransitionRequiresConfiguredRustRecorder(t *testing.T) {
	service, err := New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(Event{
		ID:     "unconfigured",
		Type:   acceptedTransitionEventType,
		Action: "transition.accepted",
		Status: "accepted",
	}); err == nil || !strings.Contains(err.Error(), "durable_recorder_unavailable") {
		t.Fatalf("unconfigured accepted transition error = %v, want durable_recorder_unavailable", err)
	}
	if events, err := service.List(); err != nil || len(events) != 0 {
		t.Fatalf("unconfigured recorder created mirror event: events=%d err=%v", len(events), err)
	}
}

func TestUnconfiguredRestartRejectsACKShapedSemanticGoMirror(t *testing.T) {
	dir := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var ev Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded", "id": ev.ID, "seq": ev.Seq, "hash": strings.Repeat("a", 64),
		})
	}))
	defer sidecar.Close()

	configured, err := New(dir, sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := configured.Append(Event{ID: "mirrored", Type: acceptedTransitionEventType, Action: "transition.accepted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir, ""); err == nil || !strings.Contains(err.Error(), "durable_recorder_unavailable") {
		t.Fatalf("unconfigured restart error = %v, want semantic mirror rejection", err)
	}
}

func TestRustAcknowledgedEventsIgnoresACKShapedMirrorWithoutRustURL(t *testing.T) {
	service, err := New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	mirrored := Event{
		ID:        "forged-mirror",
		Seq:       1,
		CreatedAt: time.Now().UTC(),
		Type:      acceptedTransitionEventType,
		Action:    "transition.accepted",
		Status:    "accepted",
		PrevHash:  "genesis",
		RustAck:   &RustAcknowledgement{ID: "forged-mirror", Sequence: 1, Hash: strings.Repeat("a", 64)},
	}
	mirrored.Hash, err = hashEvent(mirrored)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(mirrored)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(service.path, append(line, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	events, err := service.RustAcknowledgedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("unconfigured Rust authority reconstructed mirror events: %#v", events)
	}
}

func TestRustAcknowledgedEventsSerializesRebaseAndAppend(t *testing.T) {
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/events":
			close(fetchStarted)
			<-releaseFetch
			_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/events":
			defer r.Body.Close()
			var ev Event
			if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "recorded", "id": ev.ID, "seq": ev.Seq, "hash": strings.Repeat("a", 64),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer sidecar.Close()

	service, err := New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	fetched := make(chan error, 1)
	go func() {
		_, err := service.RustAcknowledgedEvents()
		fetched <- err
	}()
	<-fetchStarted
	appended := make(chan Event, 1)
	appendErr := make(chan error, 1)
	go func() {
		ev, err := service.Append(Event{ID: "during-rebase", Action: "append"})
		appended <- ev
		appendErr <- err
	}()
	select {
	case err := <-appendErr:
		t.Fatalf("append completed during Rust rebase: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFetch)
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := <-appendErr; err != nil {
		t.Fatal(err)
	}
	if first := <-appended; first.Seq != 1 {
		t.Fatalf("first append seq = %d, want 1", first.Seq)
	}
	second, err := service.Append(Event{ID: "after-rebase", Action: "append"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Seq != 2 {
		t.Fatalf("second append seq = %d, want 2", second.Seq)
	}
}

func TestCheckedEventsRejectsUnboundAcceptedTransition(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var ev Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded", "id": ev.ID, "seq": ev.Seq, "hash": strings.Repeat("a", 64),
		})
	}))
	defer sidecar.Close()

	service, err := New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := service.Append(Event{ID: "bound", Type: acceptedTransitionEventType, Action: "transition.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRustAcknowledgement(accepted); err != nil {
		t.Fatalf("mirror acknowledgement invalid: %v", err)
	}
	accepted.RustAck = nil
	line, err := json.Marshal(accepted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(service.path, append(line, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CheckedEvents(); err == nil || !strings.Contains(err.Error(), "rust acknowledgement") {
		t.Fatalf("unbound accepted history error = %v", err)
	}
}
