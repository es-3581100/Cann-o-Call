package capability_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/capability"
	"flatten-workspace/internal/capability/executors"
	"flatten-workspace/internal/capability/native"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/observability"
	"flatten-workspace/internal/orchestrator"
	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/transition"
)

func newTestRegistry() *capability.Registry {
	return capability.NewRegistry(8)
}

func newTestAuthority(t *testing.T) *transition.Authority {
	t.Helper()
	w := &fakeWriter{}
	return transition.New(w, nil)
}

type fakeWriter struct {
	events []eventlog.Event
	fail   error
	mu     sync.Mutex
}

func (f *fakeWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return eventlog.Event{}, f.fail
	}
	ev.Seq = int64(len(f.events) + 1)
	ev.Hash = strings.Repeat("a", 64)
	ev.RustAck = &eventlog.RustAcknowledgement{ID: ev.ID, Sequence: ev.Seq, Hash: strings.Repeat("b", 64)}
	f.events = append(f.events, ev)
	return ev, nil
}

type disabledExec struct {
	desc capability.Descriptor
}

func newDisabledExec() *disabledExec {
	return &disabledExec{
		desc: capability.Descriptor{
			ID: "cap.disabled.test", Name: "Disabled", Version: "1.0.0", Kind: capability.KindRead, Tier: capability.TierRead,
			InputType: "in", OutputType: "out", Timeout: time.Second,
			ResourceBounds: capability.ResourceBounds{MaxInputBytes: 1024, MaxOutputBytes: 1024},
			Mutating:       false, Native: false, Enabled: false, Provenance: "test", Provider: "test",
		},
	}
}
func (d *disabledExec) Describe() capability.Descriptor { return d.desc }
func (d *disabledExec) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	return capability.Result{RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: capability.ResultCompleted}, nil
}

func TestCHUNK06_Matrix(t *testing.T) {
	t.Run("01_empty_deterministic", func(t *testing.T) {
		reg := capability.NewRegistry(8)
		if len(reg.List()) != 0 {
			t.Fatalf("empty registry not empty")
		}
		ids := reg.ListIDs()
		if len(ids) != 0 {
			t.Fatalf("empty ids not empty")
		}
		reg2 := capability.NewRegistry(8)
		if fmt.Sprintf("%v", reg.List()) != fmt.Sprintf("%v", reg2.List()) {
			t.Fatal("empty listing not deterministic")
		}
	})
	t.Run("02_valid_registers", func(t *testing.T) {
		reg := newTestRegistry()
		if err := reg.Register(executors.NewFileMetadataExecutor()); err != nil {
			t.Fatalf("register file metadata: %v", err)
		}
		if err := reg.Register(executors.NewHashBytesExecutor()); err != nil {
			t.Fatalf("register hash: %v", err)
		}
		if reg.Count() != 2 {
			t.Fatalf("count %d want 2", reg.Count())
		}
	})
	t.Run("03_duplicate_rejected", func(t *testing.T) {
		reg := newTestRegistry()
		if err := reg.Register(executors.NewFileMetadataExecutor()); err != nil {
			t.Fatal(err)
		}
		err := reg.Register(executors.NewFileMetadataExecutor())
		if err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate not rejected: %v", err)
		}
		if reg.Count() != 1 {
			t.Fatal("duplicate changed count")
		}
	})
	t.Run("04_disabled_rejected", func(t *testing.T) {
		reg := newTestRegistry()
		if err := reg.Register(newDisabledExec()); err != nil {
			t.Fatalf("disabled register failed: %v", err)
		}
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{
			RequestID: "req-04", CapabilityID: "cap.disabled.test", WorkspaceID: "ws1",
			Inputs: json.RawMessage(`{}`), Timestamp: time.Now().UTC(),
		}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrCapabilityDisabled) {
			t.Fatalf("disabled not rejected on execute: %v", err)
		}
		if _, err := reg.Get("cap.disabled.test"); err == nil || !strings.Contains(err.Error(), capability.ErrCapabilityDisabled) {
			t.Fatalf("Get disabled should reject: %v", err)
		}
	})
	t.Run("05_unknown_rejected", func(t *testing.T) {
		reg := newTestRegistry()
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-05", CapabilityID: "cap.unknown.id", WorkspaceID: "ws1", Inputs: json.RawMessage(`{}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrUnknownCapability) {
			t.Fatalf("unknown not rejected: %v", err)
		}
	})
	t.Run("06_deterministic_listing", func(t *testing.T) {
		reg := capability.NewRegistry(8)
		reg.MustRegister(executors.NewTransformUpper())
		reg.MustRegister(executors.NewFileMetadataExecutor())
		reg.MustRegister(executors.NewHashBytesExecutor())
		ids := reg.ListIDs()
		if len(ids) != 3 {
			t.Fatalf("ids %v", ids)
		}
		if ids[0] != "cap.file.metadata" || ids[1] != "cap.hash.bytes" || ids[2] != "cap.transform.upper" {
			t.Fatalf("listing not deterministic sorted: %v", ids)
		}
		ids2 := reg.ListIDs()
		for i := range ids {
			if ids[i] != ids2[i] {
				t.Fatal("not deterministic")
			}
		}
	})
	t.Run("07_readonly_executes", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-07", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"foo/bar.txt"}`), Timestamp: time.Now().UTC()}
		res, err := mgr.Execute(context.Background(), req)
		if err != nil {
			t.Fatalf("read exec failed: %v", err)
		}
		if res.Status != capability.ResultCompleted {
			t.Fatalf("status %s", res.Status)
		}
		if len(res.Outputs) == 0 {
			t.Fatal("empty outputs")
		}
	})
	t.Run("08_input_bound", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewHashBytesExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		big := strings.Repeat("x", 9000)
		req := capability.Request{RequestID: "req-08", CapabilityID: "cap.hash.bytes", WorkspaceID: "ws1", Inputs: json.RawMessage(fmt.Sprintf(`{"data":%q}`, big)), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrResourceLimit) {
			t.Fatalf("input bound not enforced: %v", err)
		}
	})
	t.Run("09_output_bound", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewLargeOutputExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-09", CapabilityID: "cap.test.large_output", WorkspaceID: "ws1", Inputs: json.RawMessage(`{}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrResourceLimit) {
			t.Fatalf("output bound not enforced: %v", err)
		}
	})
	t.Run("10_timeout", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewSleepExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-10", CapabilityID: "cap.test.sleep", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"sleep_ms":200}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrTimeout) {
			t.Fatalf("timeout not enforced: %v", err)
		}
	})
	t.Run("11_error_classified", func(t *testing.T) {
		err := capability.NewError(capability.ErrInvalidInput, "bad", nil)
		if !capability.IsCode(err, capability.ErrInvalidInput) {
			t.Fatal("classify failed")
		}
		if capability.IsCode(err, capability.ErrTimeout) {
			t.Fatal("wrong classify")
		}
	})
	t.Run("12_validation", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), capability.ErrInvalidInput) {
			t.Fatalf("validation not enforced: %v", err)
		}
	})
	t.Run("13_actor_may_request", func(t *testing.T) {
		ctrl := actorstub.New(8, time.Minute)
		defer ctrl.Shutdown()
		act := ctrl.Activate("ws1", "test", "path1")
		if act == nil || strings.HasPrefix(act.Status, "rejected") {
			t.Fatalf("activate failed: %#v", act)
		}
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		bridge := capability.NewActorBridge(ctrl, mgr)
		req, err := bridge.RequestFromActor(act.ID, "cap.file.metadata", json.RawMessage(`{"path":"foo.txt"}`), "ws1", "read")
		if err != nil {
			t.Fatalf("actor request failed: %v", err)
		}
		if req.ActorID != act.ID {
			t.Fatalf("actor id not preserved")
		}
	})
	t.Run("14_cannot_self_authorize", func(t *testing.T) {
		ctrl := actorstub.New(8, time.Minute)
		defer ctrl.Shutdown()
		act := ctrl.Activate("ws1", "test", "path1")
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		bridge := capability.NewActorBridge(ctrl, mgr)
		req, _ := bridge.RequestFromActor(act.ID, "cap.file.metadata", json.RawMessage(`{}`), "ws1", "read")
		res, err := mgr.Execute(context.Background(), req)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		if res.ProposedTransition != nil && len(res.ProposedTransition) > 0 && strings.Contains(string(res.ProposedTransition), "accepted") {
			t.Fatal("result should not claim accepted")
		}
		if mgr.Authority().Projection().Version != 0 {
			t.Fatal("read path mutated projection")
		}
	})
	t.Run("15_lineage_preserved", func(t *testing.T) {
		ctrl := actorstub.New(8, time.Minute)
		defer ctrl.Shutdown()
		parent := ctrl.Activate("ws1", "test", "path1")
		child := ctrl.ActivateWithParent(parent.ID, "ws1", "test2", "path2")
		if child.LineageID != parent.LineageID {
			t.Fatalf("lineage not preserved parent %s child %s", parent.LineageID, child.LineageID)
		}
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		bridge := capability.NewActorBridge(ctrl, mgr)
		req, _ := bridge.RequestFromActor(child.ID, "cap.file.metadata", json.RawMessage(`{}`), "ws1", "read")
		if !bridge.LineagePreserved(child.ID, req) {
			t.Fatal("lineage not preserved in request")
		}
		if req.LineageID != parent.LineageID {
			t.Fatal("request lineage mismatch")
		}
	})
	t.Run("16_budget_not_replenished", func(t *testing.T) {
		ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 8, MaxDepth: 8, MaxBudget: 2, TTL: time.Minute})
		defer ctrl.Shutdown()
		a1 := ctrl.Activate("ws1", "a", "p1")
		a2 := ctrl.ActivateWithParent(a1.ID, "ws1", "a", "p2")
		a3 := ctrl.ActivateWithParent(a2.ID, "ws1", "a", "p3")
		if a3.RemainingBudget != 0 {
			t.Fatalf("unexpected budget %d", a3.RemainingBudget)
		}
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		bridge := capability.NewActorBridge(ctrl, mgr)
		req, _ := bridge.RequestFromActor(a3.ID, "cap.file.metadata", json.RawMessage(`{}`), "ws1", "read")
		_, _ = mgr.Execute(context.Background(), req)
		a4 := ctrl.ActivateWithParent(a3.ID, "ws1", "a", "p4")
		if !strings.Contains(a4.Status, "rejected_budget") {
			t.Fatalf("budget should be exhausted but got %s", a4.Status)
		}
	})
	t.Run("17_read_without_canonical", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		auth := newTestAuthority(t)
		mgr := capability.NewManager(reg, auth)
		initial := auth.Projection().Version
		req := capability.Request{RequestID: "req-17", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt"}`), Timestamp: time.Now().UTC()}
		res, _ := mgr.Execute(context.Background(), req)
		if res.Status != capability.ResultCompleted {
			t.Fatal("read not completed")
		}
		if auth.Projection().Version != initial {
			t.Fatal("read mutated projection")
		}
	})
	t.Run("18_mutating_routes_Go", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewBoundedWriteExecutor())
		auth := newTestAuthority(t)
		mgr := capability.NewManager(reg, auth)
		req := capability.Request{RequestID: "req-18", CapabilityID: "cap.file.write_bounded", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"bounded/file.txt","content":"hello"}`), Timestamp: time.Now().UTC()}
		res, accepted, err := mgr.ExecuteMutating(context.Background(), req, "upsert", "ws1", "bounded/file.txt", json.RawMessage(`{"content":"hello"}`))
		if err != nil {
			t.Fatalf("mutating failed: %v", err)
		}
		if res.Status != capability.ResultCompleted {
			t.Fatal("not completed")
		}
		if accepted.Durable.EventID == "" {
			t.Fatal("no durable binding")
		}
		if auth.Projection().Version != 1 {
			t.Fatalf("projection not advanced: %d", auth.Projection().Version)
		}
	})
	t.Run("19_Rust_ACK_required", func(t *testing.T) {
		log, _ := eventlog.New(t.TempDir(), "")
		auth := transition.New(log, nil)
		reg := newTestRegistry()
		reg.MustRegister(executors.NewBoundedWriteExecutor())
		mgr := capability.NewManager(reg, auth)
		req := capability.Request{RequestID: "req-19", CapabilityID: "cap.file.write_bounded", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt","content":"hi"}`), Timestamp: time.Now().UTC()}
		_, _, err := mgr.ExecuteMutating(context.Background(), req, "upsert", "ws1", "a.txt", json.RawMessage(`{"hi":1}`))
		if err == nil || !strings.Contains(err.Error(), "durable_recorder_unavailable") {
			t.Fatalf("Rust ACK not required: %v", err)
		}
	})
	t.Run("20_Rust_unavailable_closed", func(t *testing.T) {
		log, _ := eventlog.New(t.TempDir(), "")
		auth := transition.New(log, nil)
		initial := auth.Projection().Version
		reg := newTestRegistry()
		reg.MustRegister(executors.NewBoundedWriteExecutor())
		mgr := capability.NewManager(reg, auth)
		req := capability.Request{RequestID: "req-20", CapabilityID: "cap.file.write_bounded", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt","content":"hi"}`), Timestamp: time.Now().UTC()}
		_, _, err := mgr.ExecuteMutating(context.Background(), req, "upsert", "ws1", "a.txt", json.RawMessage(`{"hi":1}`))
		if err == nil {
			t.Fatal("should fail closed")
		}
		if auth.Projection().Version != initial {
			t.Fatal("projection changed despite Rust unavailable")
		}
	})
	t.Run("21_invalid_unchanged", func(t *testing.T) {
		auth := newTestAuthority(t)
		initial := auth.Projection()
		reg := newTestRegistry()
		reg.MustRegister(executors.NewBoundedWriteExecutor())
		mgr := capability.NewManager(reg, auth)
		req := capability.Request{RequestID: "req-21", CapabilityID: "cap.file.write_bounded", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt","content":"hi"}`), Timestamp: time.Now().UTC()}
		_, _, err := mgr.ExecuteMutating(context.Background(), req, "invalid_op", "ws1", "a.txt", json.RawMessage(`{invalid}`))
		if err == nil {
			t.Fatal("invalid should fail")
		}
		if auth.Projection().Version != initial.Version {
			t.Fatal("invalid changed projection")
		}
	})
	t.Run("22_exec_success_not_transition", func(t *testing.T) {
		auth := newTestAuthority(t)
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, auth)
		req := capability.Request{RequestID: "req-22", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt"}`), Timestamp: time.Now().UTC()}
		res, err := mgr.Execute(context.Background(), req)
		if err != nil || res.Status != capability.ResultCompleted {
			t.Fatalf("exec failed: %v %v", err, res)
		}
		if auth.Projection().Version != 0 {
			t.Fatal("exec success mutated projection")
		}
		prop := transition.ProposedTransition{
			TransitionID: "bad", ProposalID: "p-bad", RequestID: "req-22",
			Prior:     transition.StateRef{ID: "wrong", Version: 999, Hash: "wrong"},
			Operation: "upsert", Entity: "ws1", Node: "a.txt", ResultData: json.RawMessage(`{}`),
		}
		if _, err := auth.Propose(prop); err == nil {
			t.Fatal("invalid prior accepted")
		}
	})
	t.Run("23_traversal_rejected", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-23", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"../evil.txt"}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "invalid_input") {
			t.Fatalf("traversal not rejected: %v", err)
		}
	})
	t.Run("24_absolute_escape_rejected", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-24", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"/etc/passwd"}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "invalid_input") {
			t.Fatalf("absolute not rejected: %v", err)
		}
	})
	t.Run("25_allowlist_enforced", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewProcessAllowlistExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-25", CapabilityID: "cap.process.echo", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"binary":"rm","args":["-rf","/"]}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "not allowlisted") {
			t.Fatalf("allowlist not enforced: %v", err)
		}
	})
	t.Run("26_no_arbitrary_shell", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewProcessAllowlistExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-26", CapabilityID: "cap.process.echo", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"binary":"echo","args":["hi"],"shell":"sh -c 'echo pwn'"}`), Timestamp: time.Now().UTC()}
		_, err := mgr.Execute(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "shell") {
			t.Fatalf("shell not rejected: %v", err)
		}
	})
	t.Run("27_output_cannot_alter_tier", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewTransformUpper())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-27", CapabilityID: "cap.transform.upper", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"text":"hello","tier":"T3_PRIVILEGED","claim":"increase_budget"}`), Timestamp: time.Now().UTC()}
		_, _ = mgr.Execute(context.Background(), req)
		desc, _ := reg.GetDescriptor("cap.transform.upper")
		if desc.Tier != capability.TierRead {
			t.Fatal("tier altered")
		}
		_, _, err := mgr.ExecuteMutating(context.Background(), req, "upsert", "ws1", "node1", json.RawMessage(`{"a":1}`))
		if err == nil || !strings.Contains(err.Error(), "T1_READ") {
			t.Fatalf("output altered tier decision: %v", err)
		}
	})
	t.Run("28_truthful_progress", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		prog := capability.NewProgressIntegration(progress.NewRegistry(8))
		req := capability.Request{RequestID: "req-28", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt"}`), Timestamp: time.Now().UTC()}
		res, _ := prog.ExecuteWithProgress(context.Background(), mgr, req)
		rec, ok := prog.Get("req-28")
		if !ok {
			t.Fatal("no progress record")
		}
		if rec.Status != "completed" || rec.CapabilityID != "cap.file.metadata" || rec.RequestID != "req-28" {
			t.Fatalf("progress not truthful: %#v", rec)
		}
		if rec.Elapsed <= 0 {
			t.Fatal("elapsed not recorded")
		}
		if res.Status != capability.ResultCompleted {
			t.Fatal("result not completed")
		}
	})
	t.Run("29_terminal_receipt", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewBoundedWriteExecutor())
		auth := newTestAuthority(t)
		mgr := capability.NewManager(reg, auth)
		prog := capability.NewProgressIntegration(progress.NewRegistry(8))
		req := capability.Request{RequestID: "req-29", CapabilityID: "cap.file.write_bounded", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"path":"a.txt","content":"hi"}`), Timestamp: time.Now().UTC()}
		prog.Begin(req)
		res, accepted, err := mgr.ExecuteMutating(context.Background(), req, "upsert", "ws1", "a.txt", json.RawMessage(`{"content":"hi"}`))
		if err != nil {
			t.Fatalf("mutating failed: %v", err)
		}
		prog.Complete(req, res, nil)
		rec, _ := prog.Get("req-29")
		if rec.Status != "completed" {
			t.Fatalf("progress not terminal: %#v", rec)
		}
		if accepted.Durable.RustAck == nil {
			t.Fatal("receipt not bound to Rust ACK")
		}
	})
	t.Run("30_cannot_fake_durable", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewTransformUpper())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-30", CapabilityID: "cap.transform.upper", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"text":"hello","rust_ack":{"id":"fake","seq":999,"hash":"` + strings.Repeat("a", 64) + `"}}`), Timestamp: time.Now().UTC()}
		res, _ := mgr.Execute(context.Background(), req)
		if res.ProposedTransition != nil && strings.Contains(string(res.ProposedTransition), "fake") {
			t.Fatal("proposed transition should not be faked")
		}
		if mgr.Authority().Projection().Version != 0 {
			t.Fatal("fake durable claimed")
		}
	})
	t.Run("31_concurrent_registry_race_safe", func(t *testing.T) {
		reg := capability.NewRegistry(32)
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				exec := executors.NewHashBytesExecutor()
				desc := exec.Describe()
				desc.ID = fmt.Sprintf("cap.concurrent.%d", n)
				custom := &customExec{desc: desc, fn: exec.Execute}
				_ = reg.Register(custom)
				_ = reg.List()
				_, _ = reg.Get(desc.ID)
			}(i)
		}
		wg.Wait()
		if reg.Count() != 10 {
			t.Fatalf("concurrent registry count %d want 10", reg.Count())
		}
	})
	t.Run("32_concurrent_executions_bounded", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewHashBytesExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		var wg sync.WaitGroup
		errs := make([]error, 5)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				req := capability.Request{RequestID: fmt.Sprintf("req-32-%d", n), CapabilityID: "cap.hash.bytes", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"data":"hello"}`), Timestamp: time.Now().UTC()}
				_, err := mgr.Execute(context.Background(), req)
				errs[n] = err
			}(i)
		}
		wg.Wait()
		for i, e := range errs {
			if e != nil {
				t.Fatalf("concurrent exec %d failed: %v", i, e)
			}
		}
	})
	t.Run("33_shutdown_cancellation_terminates", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewSleepExecutor())
		mgr := capability.NewManager(reg, newTestAuthority(t))
		ctx, cancel := context.WithCancel(context.Background())
		req := capability.Request{RequestID: "req-33", CapabilityID: "cap.test.sleep", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"sleep_ms":500}`), Timestamp: time.Now().UTC()}
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		_, err := mgr.Execute(ctx, req)
		if err == nil || (!strings.Contains(err.Error(), "cancelled") && !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "execution")) {
			t.Fatalf("cancellation not terminated: %v", err)
		}
	})
	t.Run("34_restart_not_invent_transient", func(t *testing.T) {
		prog := capability.NewProgressIntegration(progress.NewRegistry(8))
		req := capability.Request{RequestID: "req-34", CapabilityID: "cap.file.metadata", WorkspaceID: "ws1", Timestamp: time.Now().UTC()}
		prog.Begin(req)
		prog.OnRestart()
		rec, _ := prog.Get("req-34")
		if rec.Status != "failed" {
			t.Fatalf("restart should mark pending as failed, got %s", rec.Status)
		}
		if prog.Registry().Count() == 0 {
			t.Fatal("registry should retain abandoned record")
		}
	})
	t.Run("35_chunk07_pipeline_green", func(t *testing.T) {
		store := graph.NewStore("ws1")
		auth := newTestAuthority(t)
		ctrl := actorstub.New(8, time.Minute)
		defer ctrl.Shutdown()
		orch := orchestrator.New("ws1", store, auth, ctrl, scoring.Config{MaxCandidates: 5})
		srcSHA := strings.Repeat("a", 64)
		ident := ingest.NewSourceIdentity("ws1", ingest.SourceTypeText, "ref1", srcSHA, "file1.txt", nil)
		cid := ingest.DeterministicContentID(ident.SourceID, srcSHA)
		pkt := ingest.SourcePacket{
			Identity:   ident,
			Content:    ingest.Content{SourceID: ident.SourceID, ContentID: cid, Text: "hello world", NormalizedText: "hello world", ContentHash: srcSHA, Ref: "ref1"},
			Metadata:   ingest.Metadata{SourceID: ident.SourceID, Title: "file1"},
			Provenance: ingest.Provenance{WorkspaceID: "ws1", ExtractionID: ingest.ExtractionIdentity, ExtractionVersion: ingest.ExtractionVersion, SourceLocator: "file1.txt", CreatedAt: time.Now().UTC()},
		}
		if _, err := orch.Ingest(pkt); err != nil {
			t.Fatalf("ingest failed: %v", err)
		}
		qr, err := orch.Query("req-35", "hello", "")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if qr.CandidatesConsidered == 0 {
			t.Fatal("no candidates")
		}
	})
	t.Run("36_chunk08_observability_green", func(t *testing.T) {
		store := graph.NewStore("ws1")
		ctrl := actorstub.New(8, time.Minute)
		defer ctrl.Shutdown()
		reg := progress.NewRegistry(8)
		if _, err := reg.Create("task-36", progress.OperationIngest, progress.PhaseRequestAccepted); err != nil {
			t.Fatal(err)
		}
		if err := reg.Transition("task-36", progress.StatusRunning); err != nil {
			t.Fatal(err)
		}
		_ = reg.SetPhase("task-36", progress.PhaseCandidatesSelected)
		actorM := observability.ActorMetricsFromController(ctrl)
		graphM := observability.GraphMetricsFromStore(store)
		_ = graphM
		_ = actorM
		ledgerM := progress.LedgerMetrics{AcceptedEventCount: 1, CurrentSequence: 1}
		ctxM := progress.ContextMetrics{CandidatesConsidered: 1}
		status := observability.CollectSystemStatus(ledgerM, graphM, actorM, ctxM, 1)
		if status.Workspaces != 1 {
			t.Fatalf("observability not green: %#v", status)
		}
		store2 := graph.NewStore("ws1")
		_ = store2
	})
	t.Run("extra_deterministic_descriptor", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewFileMetadataExecutor())
		desc, _ := reg.GetDescriptor("cap.file.metadata")
		b1, _ := json.Marshal(desc)
		b2, _ := json.Marshal(desc)
		if string(b1) != string(b2) {
			t.Fatal("descriptor not deterministic")
		}
	})
	t.Run("extra_native_membrane", func(t *testing.T) {
		m := native.DefaultMembrane()
		if !m.MembraneDefined() {
			t.Fatal("membrane not defined")
		}
		if m.ArbitraryLibraryLoading() || m.ArbitrarySymbolExecution() {
			t.Fatal("arbitrary loading should be false")
		}
		ctx := context.Background()
		res, err := m.Invoke(ctx, native.InvokeRequest{Library: "libmath", Symbol: "add", Input: json.RawMessage(`{"a":2,"b":3}`)})
		if err != nil {
			t.Fatalf("native invoke failed: %v", err)
		}
		var out map[string]float64
		_ = json.Unmarshal(res.Output, &out)
		if out["result"] != 5 {
			t.Fatalf("native result %v", out)
		}
		_, err = m.Invoke(ctx, native.InvokeRequest{Library: "evil", Symbol: "pwn", Input: json.RawMessage(`{}`)})
		if err == nil || !strings.Contains(err.Error(), "native_unavailable") {
			t.Fatalf("allowlist not enforced native: %v", err)
		}
		reg := newTestRegistry()
		reg.MustRegister(capability.NewNativeExecutor(m))
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-native", CapabilityID: "cap.native.upper", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"library":"libstring","symbol":"upper","input":{"text":"hello"}}`), Timestamp: time.Now().UTC()}
		res2, err := mgr.Execute(context.Background(), req)
		if err != nil || res2.Status != capability.ResultCompleted {
			t.Fatalf("native executor failed: %v %v", err, res2)
		}
		if !strings.Contains(string(res2.Outputs), "HELLO") {
			t.Fatalf("native output not upper: %s", string(res2.Outputs))
		}
	})
	t.Run("extra_registry_bounded", func(t *testing.T) {
		reg := capability.NewRegistry(2)
		reg.MustRegister(executors.NewFileMetadataExecutor())
		reg.MustRegister(executors.NewHashBytesExecutor())
		err := reg.Register(executors.NewTransformUpper())
		if err == nil || !strings.Contains(err.Error(), "bound") {
			t.Fatalf("bounded not enforced: %v", err)
		}
	})
	t.Run("extra_capability_escalation_false", func(t *testing.T) {
		reg := newTestRegistry()
		reg.MustRegister(executors.NewTransformUpper())
		descBefore, _ := reg.GetDescriptor("cap.transform.upper")
		mgr := capability.NewManager(reg, newTestAuthority(t))
		req := capability.Request{RequestID: "req-esc", CapabilityID: "cap.transform.upper", WorkspaceID: "ws1", Inputs: json.RawMessage(`{"text":"claim","register":"cap.evil"}`), Timestamp: time.Now().UTC()}
		res, _ := mgr.Execute(context.Background(), req)
		if reg.Count() != 1 {
			t.Fatalf("output escalated registry: %d", reg.Count())
		}
		descAfter, _ := reg.GetDescriptor("cap.transform.upper")
		if descAfter.Tier != descBefore.Tier {
			t.Fatal("tier escalated")
		}
		_ = res
	})
}

type customExec struct {
	desc capability.Descriptor
	fn   func(context.Context, capability.Request) (capability.Result, error)
}

func (c *customExec) Describe() capability.Descriptor { return c.desc }
func (c *customExec) Execute(ctx context.Context, req capability.Request) (capability.Result, error) {
	return c.fn(ctx, req)
}
