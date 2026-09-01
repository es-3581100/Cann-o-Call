package transition

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"flatten-workspace/internal/eventlog"
)

type fakeWriter struct {
	events []eventlog.Event
	fail   error
}

func (f *fakeWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
	if f.fail != nil {
		return eventlog.Event{}, f.fail
	}
	ev.Seq = int64(len(f.events) + 1)
	ev.Hash = "fake-hash"
	f.events = append(f.events, ev)
	return ev, nil
}

func proposal(a *Authority, id string) ProposedTransition {
	return ProposedTransition{
		TransitionID:  id,
		ProposalID:    "proposal-" + id,
		RequestID:     "request-" + id,
		Prior:         a.Projection().Ref(),
		Operation:     "upsert",
		Entity:        "workspace",
		Node:          id,
		ResultData:    []byte(`{"z":2,"a":[true,null]}`),
		AdmissionData: []byte(`{"reason":"typed validation"}`),
	}
}

func assertClass(t *testing.T, want RejectClass, err error) {
	t.Helper()
	if got := Class(err); got != want {
		t.Fatalf("rejection class = %q, want %q (error %v)", got, want, err)
	}
}

func TestProjectionEmptyOneAndManyAreCanonical(t *testing.T) {
	writer := &fakeWriter{}
	a := New(writer, nil)
	empty := a.Projection()
	if empty.Version != 0 || len(empty.Nodes) != 0 || empty.Hash == "" {
		t.Fatalf("invalid empty projection: %#v", empty)
	}
	if _, err := a.Propose(proposal(a, "one")); err != nil {
		t.Fatal(err)
	}
	one := a.Projection()
	if one.Version != 1 || len(one.Nodes) != 1 || one.Hash == empty.Hash {
		t.Fatalf("invalid one-transition projection: %#v", one)
	}
	for _, id := range []string{"three", "two"} {
		if _, err := a.Propose(proposal(a, id)); err != nil {
			t.Fatal(err)
		}
	}
	many := a.Projection()
	if many.Version != 3 || len(many.Nodes) != 3 || many.Nodes[0].Node != "one" || many.Nodes[1].Node != "three" || many.Nodes[2].Node != "two" {
		t.Fatalf("many projection is not ordered deterministically: %#v", many)
	}
	other := New(&fakeWriter{}, nil)
	equivalent := proposal(other, "one")
	equivalent.ResultData = []byte(`{"a":[true,null],"z":2}`)
	if _, err := other.Propose(equivalent); err != nil {
		t.Fatal(err)
	}
	if other.Projection().Hash != one.Hash {
		t.Fatalf("equivalent unordered JSON produced different semantic hashes: %s != %s", other.Projection().Hash, one.Hash)
	}
}

func TestAdmissionAppendRetryDuplicateConflictAndRejectUnchanged(t *testing.T) {
	writer := &fakeWriter{fail: errors.New("disk unavailable")}
	a := New(writer, nil)
	initial := a.Projection()
	p := proposal(a, "transition-1")
	if _, err := a.Propose(p); err == nil {
		t.Fatal("durable append failure accepted")
	} else {
		assertClass(t, Durable, err)
	}
	if got := a.Projection(); !reflect.DeepEqual(got, initial) || len(writer.events) != 0 {
		t.Fatalf("durable failure changed projection or writer: state=%#v events=%d", got, len(writer.events))
	}

	writer.fail = nil
	accepted, err := a.Propose(p)
	if err != nil {
		t.Fatalf("retry accepted: %v", err)
	}
	if accepted.Durable.EventID != p.TransitionID || accepted.Durable.Sequence != 1 || len(writer.events) != 1 {
		t.Fatalf("accepted durable binding = %#v; writes=%d", accepted.Durable, len(writer.events))
	}
	after := a.Projection()
	duplicate, err := a.Propose(p)
	if err != nil || !duplicate.Duplicate || !reflect.DeepEqual(a.Projection(), after) || len(writer.events) != 1 {
		t.Fatalf("duplicate must be detected without append/state change: duplicate=%#v err=%v writes=%d", duplicate, err, len(writer.events))
	}

	conflict := p
	conflict.ResultData = []byte(`{"a":99}`)
	if _, err := a.Propose(conflict); err == nil {
		t.Fatal("same id different request accepted")
	} else {
		assertClass(t, Conflict, err)
	}
	stale := proposal(a, "transition-stale")
	stale.Prior = initial.Ref()
	if _, err := a.Propose(stale); err == nil {
		t.Fatal("stale predecessor accepted")
	} else {
		assertClass(t, Stale, err)
	}
	if !reflect.DeepEqual(a.Projection(), after) || len(writer.events) != 1 {
		t.Fatal("rejected requests changed projection or durable history")
	}
}

func TestTypedValidationPolicyRestoreAndImportLeaveRejectedStateUnchanged(t *testing.T) {
	writer := &fakeWriter{}
	policyErr := errors.New("denied by Go policy")
	a := New(writer, func(p ProposedTransition, _ Projection) error {
		if p.Entity == "denied" {
			return policyErr
		}
		return nil
	})
	initial := a.Projection()
	malformed := proposal(a, "malformed")
	malformed.ResultData = []byte(`{`)
	if _, err := a.Propose(malformed); err == nil {
		t.Fatal("malformed proposal accepted")
	} else {
		assertClass(t, Malformed, err)
	}
	invalid := proposal(a, "invalid")
	invalid.Operation = "delete"
	if _, err := a.Propose(invalid); err == nil {
		t.Fatal("invalid operation accepted")
	} else {
		assertClass(t, Invalid, err)
	}
	denied := proposal(a, "denied")
	denied.Entity = "denied"
	if _, err := a.Propose(denied); err == nil {
		t.Fatal("policy-denied proposal accepted")
	} else {
		assertClass(t, Policy, err)
	}
	if !reflect.DeepEqual(initial, a.Projection()) || len(writer.events) != 0 {
		t.Fatal("rejected typed proposal changed state")
	}

	restored, err := a.Restore(proposal(a, "restore"))
	if err != nil || restored.Proposal.Operation != "restore" {
		t.Fatalf("restore did not use admission path: %#v %v", restored, err)
	}
	beforeBadImport := a.Projection()
	badImport := proposal(a, "bad-import")
	badImport.ResultData = []byte(`nope`)
	if _, err := a.Import(badImport); err == nil {
		t.Fatal("invalid import accepted")
	} else {
		assertClass(t, Malformed, err)
	}
	if !reflect.DeepEqual(beforeBadImport, a.Projection()) {
		t.Fatal("invalid import changed projection")
	}
	imported, err := a.Import(proposal(a, "import"))
	if err != nil || imported.Proposal.Operation != "import" {
		t.Fatalf("import did not use admission path: %#v %v", imported, err)
	}
}

func TestEventLogBoundaryRestartReplayAndCorruptHistoryBlock(t *testing.T) {
	dir := t.TempDir()
	log, err := eventlog.New(dir, "")
	if err != nil {
		t.Fatalf("new event log: %v", err)
	}
	a := New(log, nil)
	if _, err := a.Propose(proposal(a, "one")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Propose(proposal(a, "two")); err != nil {
		t.Fatal(err)
	}
	full := a.Projection()
	restarted, err := NewFromEventLog(log, nil)
	if err != nil || !sameProjection(restarted.Projection(), full) {
		t.Fatalf("restart replay = %#v, %v", restarted, err)
	}
	events, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	discarded, err := Rebuild(events, log, nil)
	if err != nil || !sameProjection(discarded.Projection(), full) {
		t.Fatalf("discard/rebuild = %#v, %v", discarded, err)
	}

	// eventlog itself intentionally accepts opaque events. Only Authority emits
	// authority-transition events after its Go validation path.
	if _, err := log.Append(eventlog.Event{ID: "opaque-accepted", Status: "accepted"}); err != nil {
		t.Fatalf("low-level opaque append: %v", err)
	}
	opaqueHistory, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	fromOpaque, err := Rebuild(opaqueHistory, log, nil)
	if err != nil || !sameProjection(fromOpaque.Projection(), full) {
		t.Fatalf("opaque append changed authority projection: %#v, %v", fromOpaque, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("not-json\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFromEventLog(log, nil); err == nil {
		t.Fatal("corrupt history rebuilt")
	} else {
		assertClass(t, ReplayCorruption, err)
	}
}

func TestCheckpointMatchesFullHistoryAndSuffixRebuild(t *testing.T) {
	log, err := eventlog.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := New(log, nil)
	if _, err := a.Propose(proposal(a, "first")); err != nil {
		t.Fatal(err)
	}
	cp := a.Checkpoint()
	if _, err := a.Propose(proposal(a, "second")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Propose(proposal(a, "third")); err != nil {
		t.Fatal(err)
	}
	history, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(cp, history, log, nil); err != nil {
		t.Fatalf("verify checkpoint: %v", err)
	}
	fromCheckpoint, err := RebuildFromCheckpoint(cp, history, log, nil)
	if err != nil {
		t.Fatalf("rebuild from checkpoint: %v", err)
	}
	full, err := Rebuild(history, log, nil)
	if err != nil || !sameProjection(fromCheckpoint.Projection(), full.Projection()) {
		t.Fatalf("checkpoint/full mismatch: %v", err)
	}
}

func TestDurableBindingPreservesRustAcknowledgement(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var ev eventlog.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "recorded",
			"id":     ev.ID,
			"seq":    ev.Seq,
			"hash":   strings.Repeat("a", 64),
		})
	}))
	defer sidecar.Close()
	log, err := eventlog.New(t.TempDir(), sidecar.URL)
	if err != nil {
		t.Fatal(err)
	}
	a := New(log, nil)
	accepted, err := a.Propose(proposal(a, "with-rust-ack"))
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Durable.RustAck == nil || accepted.Durable.RustAck.ID != "with-rust-ack" || accepted.Durable.RustAck.Sequence != 1 {
		t.Fatalf("accepted transition lost Rust acknowledgement: %#v", accepted.Durable)
	}
	cp := a.Checkpoint()
	if len(cp.Accepted) != 1 || !reflect.DeepEqual(cp.Accepted[0].Durable.RustAck, accepted.Durable.RustAck) {
		t.Fatalf("checkpoint lost Rust acknowledgement: %#v", cp)
	}
	history, err := log.CheckedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(cp, history, log, nil); err != nil {
		t.Fatalf("checkpoint lost Rust acknowledgement: %v", err)
	}
	rebuilt, err := Rebuild(history, log, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := acceptedSlice(rebuilt); len(got) != 1 || !reflect.DeepEqual(got[0].Durable.RustAck, accepted.Durable.RustAck) {
		t.Fatalf("rebuild lost Rust acknowledgement: %#v", got)
	}
}

func TestRebuildValidatesCallerSuppliedHistory(t *testing.T) {
	_, err := Rebuild([]eventlog.Event{{ID: "corrupt-history"}}, &fakeWriter{}, nil)
	if err == nil {
		t.Fatal("Rebuild accepted caller-supplied corrupt history")
	}
	assertClass(t, ReplayCorruption, err)
}
