package eventlog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	event := Event{ID: "go-approved", Action: "apply", Status: "accepted", CreatedAt: time.Now().UTC()}
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
