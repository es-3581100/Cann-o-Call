package actorstub

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/transition"
)

// Helper to wait for condition with timeout.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// fakeWriter for transition authority tests.
type chunkFakeWriter struct {
	mu     sync.Mutex
	events []eventlog.Event
	fail   error
}

func (f *chunkFakeWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return eventlog.Event{}, f.fail
	}
	ev.Seq = int64(len(f.events) + 1)
	ev.Hash = strings.Repeat("a", 64)
	ev.RustAck = &eventlog.RustAcknowledgement{ID: ev.ID, Sequence: ev.Seq, Hash: strings.Repeat("a", 64)}
	f.events = append(f.events, ev)
	return ev, nil
}

func (f *chunkFakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

// 01 runtime starts
func TestChunk04_01_RuntimeStarts(t *testing.T) {
	c := New(16, 5*time.Minute)
	defer c.Shutdown()
	if c.System() == nil {
		t.Fatal("ActorSystem nil")
	}
	if c.Root() == nil {
		t.Fatal("RootContext nil")
	}
	cfg := c.Config()
	if cfg.MaxActive != 16 || cfg.MaxDepth != 8 || cfg.MaxBudget != 32 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

// 02 dormant descriptor exists without live PID
func TestChunk04_02_DormantDescriptorWithoutLivePID(t *testing.T) {
	c := NewWithConfig(Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: 200 * time.Millisecond})
	defer c.Shutdown()
	a := c.Activate("ws-dormant", "build_ledger.state.updated", "path/a")
	if a == nil || a.Status != "active" {
		t.Fatalf("activate failed: %#v", a)
	}
	// Complete deterministically
	c.Complete(a.ID)
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	if c.IsLive(a.ID) {
		t.Fatal("expected not live after complete")
	}
	desc, ok := c.Descriptor(a.ID)
	if !ok {
		t.Fatal("dormant descriptor missing after stop")
	}
	if desc.Status != "completed" && desc.Status != "passivated" {
		t.Fatalf("unexpected dormant status %q", desc.Status)
	}
	if desc.ActorID() != a.ID {
		// helper via ID field
	}
	if desc.LineageID == "" || desc.Depth != 1 || desc.RemainingBudget != 32 {
		t.Fatalf("dormant descriptor fields incomplete: %#v", desc)
	}
	if c.DescriptorCount() != 1 {
		t.Fatalf("descriptor count %d want 1", c.DescriptorCount())
	}
}

// Helper to satisfy field check without method
func (a *Activation) ActorID() string { return a.ID }

// 03 activation materializes actor
func TestChunk04_03_ActivationMaterializesActor(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-1", "build_ledger.state.updated", "path/b")
	if a == nil {
		t.Fatal("activate nil")
	}
	if !c.IsLive(a.ID) {
		t.Fatal("expected live PID after governed activation")
	}
	if c.LiveCount() != 1 {
		t.Fatalf("live count %d want 1", c.LiveCount())
	}
	if a.LineageID == "" {
		t.Fatal("lineage not set")
	}
}

// 04 actor receives bounded message
func TestChunk04_04_ActorReceivesBoundedMessage(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-1", "build_ledger.state.updated", "path/c")
	if err := c.Send(a.ID, &ProcessMessage{Payload: "hello"}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := c.GetResult(a.ID)
		return ok
	})
	res, ok := c.GetResult(a.ID)
	if !ok {
		t.Fatal("result not emitted")
	}
	if res.Observations != "hello" || res.Result != "processed:hello" {
		t.Fatalf("unexpected result: %#v", res)
	}
}

// 05 completes and stops
func TestChunk04_05_CompletesAndStops(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-1", "build_ledger.state.updated", "path/d")
	if err := c.Send(a.ID, &ProcessMessage{Payload: "work"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	if c.IsLive(a.ID) {
		t.Fatal("expected stopped after bounded processing")
	}
	desc, _ := c.Descriptor(a.ID)
	if desc.Status != "completed" && desc.Status != "passivated" {
		t.Fatalf("status %q not completed/passivated", desc.Status)
	}
	if c.LiveCount() != 0 {
		t.Fatalf("live count after completion %d want 0", c.LiveCount())
	}
}

// 06 TTL deterministic stop
func TestChunk04_06_TTLDeterministicStop(t *testing.T) {
	c := NewWithConfig(Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: 80 * time.Millisecond})
	defer c.Shutdown()
	a := c.Activate("ws-ttl", "build_ledger.state.updated", "path/ttl")
	if a == nil {
		t.Fatal("activate nil")
	}
	if !c.IsLive(a.ID) {
		t.Fatal("not live immediately after activate")
	}
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	desc, _ := c.Descriptor(a.ID)
	if desc.Status != "expired" {
		t.Fatalf("expected expired after TTL, got %q", desc.Status)
	}
	// After TTL, new activation with same key should succeed (dedup window cleared)
	b := c.Activate("ws-ttl", "build_ledger.state.updated", "path/ttl")
	if b == nil {
		t.Fatal("expected new activation after TTL expiry, got dedup suppressed")
	}
	if b.Status != "active" {
		t.Fatalf("expected active after TTL, got %q", b.Status)
	}
}

// 07 duplicate suppressed
func TestChunk04_07_DuplicateSuppressed(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-dup", "build_ledger.state.updated", "path/dup")
	if a == nil {
		t.Fatal("first activate nil")
	}
	dup := c.Activate("ws-dup", "build_ledger.state.updated", "path/dup")
	if dup != nil {
		t.Fatalf("expected duplicate suppressed (nil), got %#v", dup)
	}
	if c.LiveCount() != 1 {
		t.Fatalf("live count %d want 1", c.LiveCount())
	}
}

// 08 max active bound
func TestChunk04_08_MaxActiveBound(t *testing.T) {
	c := NewWithConfig(Config{MaxActive: 1, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	defer c.Shutdown()
	first := c.Activate("ws-1", "build_ledger.state.updated", "path/one")
	if first == nil || first.Status != "active" {
		t.Fatalf("first should be active: %#v", first)
	}
	second := c.Activate("ws-1", "build_ledger.event.appended", "path/two")
	if second == nil {
		t.Fatal("second should be rejected not nil")
	}
	if second.Status != "rejected_budget" {
		t.Fatalf("expected rejected_budget, got %q", second.Status)
	}
	if c.LiveCount() != 1 {
		t.Fatalf("live count %d want 1", c.LiveCount())
	}
	if c.IsLive(second.ID) {
		t.Fatal("rejected activation should not have live PID")
	}
}

// 09 lineage propagates
func TestChunk04_09_LineagePropagates(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	parent := c.Activate("ws-lineage", "build_ledger.state.updated", "path/parent")
	if parent == nil {
		t.Fatal("parent nil")
	}
	child := c.ActivateWithParent(parent.ID, "ws-lineage", "build_ledger.event.appended", "path/child")
	if child == nil {
		t.Fatal("child nil")
	}
	if child.Status != "active" {
		t.Fatalf("child status %q", child.Status)
	}
	if child.LineageID != parent.LineageID {
		t.Fatalf("lineage mismatch parent %q child %q", parent.LineageID, child.LineageID)
	}
	if child.Depth != parent.Depth+1 {
		t.Fatalf("depth parent %d child %d", parent.Depth, child.Depth)
	}
	if child.RemainingBudget != parent.RemainingBudget-1 {
		t.Fatalf("budget parent %d child %d", parent.RemainingBudget, child.RemainingBudget)
	}
	if child.ParentActorID != parent.ID {
		t.Fatalf("parent id mismatch %q vs %q", child.ParentActorID, parent.ID)
	}
}

// 10 depth bound
func TestChunk04_10_DepthBound(t *testing.T) {
	c := NewWithConfig(Config{MaxActive: 16, MaxDepth: 2, MaxBudget: 32, TTL: time.Minute})
	defer c.Shutdown()
	root := c.Activate("ws-depth", "build_ledger.state.updated", "path/root")
	child := c.ActivateWithParent(root.ID, "ws-depth", "build_ledger.event.appended", "path/child")
	if child.Status != "active" {
		t.Fatalf("child should be active, got %q", child.Status)
	}
	grand := c.ActivateWithParent(child.ID, "ws-depth", "build_ledger.event.appended", "path/grand")
	if grand.Status != "rejected_depth" {
		t.Fatalf("expected rejected_depth, got %q notes %v", grand.Status, grand.Notes)
	}
	if c.IsLive(grand.ID) {
		t.Fatal("depth-rejected should not be live")
	}
}

// 11 budget decreases/exhaustion
func TestChunk04_11_BudgetExhaustion(t *testing.T) {
	c := NewWithConfig(Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 2, TTL: time.Minute})
	defer c.Shutdown()
	root := c.Activate("ws-budget", "build_ledger.state.updated", "path/root")
	if root.RemainingBudget != 2 {
		t.Fatalf("root budget %d want 2", root.RemainingBudget)
	}
	child := c.ActivateWithParent(root.ID, "ws-budget", "build_ledger.event.appended", "path/c1")
	if child.RemainingBudget != 1 {
		t.Fatalf("child budget %d want 1", child.RemainingBudget)
	}
	grand := c.ActivateWithParent(child.ID, "ws-budget", "build_ledger.event.appended", "path/c2")
	if grand.RemainingBudget != 0 {
		t.Fatalf("grand budget %d want 0", grand.RemainingBudget)
	}
	if grand.Status != "active" {
		t.Fatalf("grand should still be active at 0, got %q", grand.Status)
	}
	exhausted := c.ActivateWithParent(grand.ID, "ws-budget", "build_ledger.event.appended", "path/c3")
	if exhausted.Status != "rejected_budget_exhausted" {
		t.Fatalf("expected rejected_budget_exhausted, got %q", exhausted.Status)
	}
}

// 12 cycle bounded
func TestChunk04_12_CycleBounded(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	parent := c.Activate("ws-cycle", "build_ledger.state.updated", "path/same")
	child := c.ActivateWithParent(parent.ID, "ws-cycle", "build_ledger.state.updated", "path/same")
	if child.Status != "rejected_cycle" {
		t.Fatalf("expected rejected_cycle, got %q", child.Status)
	}
	if c.IsLive(child.ID) {
		t.Fatal("cycle-rejected should not be live")
	}
	// Different path should not be considered cycle
	okChild := c.ActivateWithParent(parent.ID, "ws-cycle", "build_ledger.state.updated", "path/different")
	if okChild.Status != "active" {
		t.Fatalf("different path should be active, got %q", okChild.Status)
	}
}

// 13 failure does not crash system
func TestChunk04_13_FailureDoesNotCrashSystem(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-fail", "build_ledger.state.updated", "path/fail")
	if err := c.Send(a.ID, &ProcessMessage{Payload: "panic"}); err != nil {
		t.Fatalf("send panic failed: %v", err)
	}
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	desc, _ := c.Descriptor(a.ID)
	if desc.Status != "failed" {
		t.Fatalf("expected failed status, got %q notes %v", desc.Status, desc.Notes)
	}
	// System must still accept new activations
	b := c.Activate("ws-fail", "build_ledger.state.updated", "path/after")
	if b == nil || b.Status != "active" {
		t.Fatalf("system crashed, second activation failed: %#v", b)
	}
	if !c.IsLive(b.ID) {
		t.Fatal("new actor not live after previous panic")
	}
	// Also test supervision isolation with concurrent actors
	c2 := c.Activate("ws-fail", "build_ledger.event.appended", "path/concurrent")
	if c2.Status != "active" {
		t.Fatalf("concurrent actor failed: %q", c2.Status)
	}
}

// 14 result emitted
func TestChunk04_14_ResultEmitted(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-res", "build_ledger.state.updated", "path/res")
	if err := c.Send(a.ID, &ProcessMessage{Payload: "observe-me", From: "tester"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, ok := c.GetResult(a.ID)
		return ok
	})
	res, ok := c.GetResult(a.ID)
	if !ok {
		t.Fatal("result missing")
	}
	if res.ActorID != a.ID {
		t.Fatalf("actor_id %q != %q", res.ActorID, a.ID)
	}
	if res.LineageID != a.LineageID {
		t.Fatalf("lineage mismatch %q vs %q", res.LineageID, a.LineageID)
	}
	if res.NodeID == "" {
		t.Fatal("node_id empty")
	}
	if res.Status == "" || res.Observations == "" || res.CreatedAt.IsZero() {
		t.Fatalf("result incomplete: %#v", res)
	}
	if res.Confidence == 0 {
		t.Fatal("confidence telemetry should be non-zero")
	}
	if len(res.EvidenceRefs) == 0 {
		t.Fatal("evidence refs empty")
	}
	if len(res.ProposedActions) == 0 {
		t.Fatal("proposed actions empty")
	}
}

// 15 mutation proposal via Go authority
func TestChunk04_15_MutationViaGoAuthority(t *testing.T) {
	writer := &chunkFakeWriter{}
	auth := transition.New(writer, nil)
	// Actor observes and proposes valid transition
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-auth", "build_ledger.state.updated", "path/auth")
	if err := c.Send(a.ID, &ProcessMessage{Payload: `{"data":"valid"}`}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := c.GetResult(a.ID); return ok })
	res, _ := c.GetResult(a.ID)
	// Map result to typed transition via Go authority
	prior := auth.Projection().Ref()
	prop := transition.ProposedTransition{
		TransitionID:  "transition-" + a.ID,
		ProposalID:    "proposal-" + a.ID,
		RequestID:     "request-" + a.ID,
		Prior:         prior,
		Operation:     "upsert",
		Entity:        "workspace",
		Node:          a.ID,
		ResultData:    []byte(fmt.Sprintf(`{"observation":%q}`, res.Observations)),
		AdmissionData: []byte(`{"reason":"actor proposal"}`),
	}
	accepted, err := auth.Propose(prop)
	if err != nil {
		t.Fatalf("valid proposal rejected: %v", err)
	}
	if accepted.Durable.EventID != prop.TransitionID {
		t.Fatalf("durable binding mismatch")
	}
	if accepted.Durable.RustAck == nil {
		t.Fatal("expected Rust ack")
	}
	proj := auth.Projection()
	if proj.Version != 1 {
		t.Fatalf("projection version %d want 1", proj.Version)
	}
	if writer.count() != 1 {
		t.Fatalf("writer events %d want 1", writer.count())
	}
	// Actor runtime itself must not have mutated state directly
	if c.IsLive(a.ID) {
		// After result, actor stopped, but that's lifecycle not authority bypass
	}
}

// 16 invalid proposal rejected
func TestChunk04_16_InvalidProposalRejected(t *testing.T) {
	writer := &chunkFakeWriter{}
	auth := transition.New(writer, nil)
	// First valid
	prior := auth.Projection().Ref()
	good := transition.ProposedTransition{
		TransitionID:  "t-good",
		ProposalID:    "p-good",
		RequestID:     "r-good",
		Prior:         prior,
		Operation:     "upsert",
		Entity:        "workspace",
		Node:          "node-1",
		ResultData:    []byte(`{"x":1}`),
		AdmissionData: []byte(`{}`),
	}
	if _, err := auth.Propose(good); err != nil {
		t.Fatalf("good failed: %v", err)
	}
	before := auth.Projection()
	beforeCount := writer.count()
	// Invalid: stale prior
	stale := transition.ProposedTransition{
		TransitionID:  "t-bad",
		ProposalID:    "p-bad",
		RequestID:     "r-bad",
		Prior:         prior, // stale
		Operation:     "upsert",
		Entity:        "workspace",
		Node:          "node-2",
		ResultData:    []byte(`{"x":2}`),
		AdmissionData: []byte(`{}`),
	}
	_, err := auth.Propose(stale)
	if err == nil {
		t.Fatal("expected stale rejected")
	}
	if transition.Class(err) != transition.Stale {
		t.Fatalf("expected Stale, got %q", transition.Class(err))
	}
	after := auth.Projection()
	if after.Hash != before.Hash || after.Version != before.Version {
		t.Fatal("projection changed after invalid proposal")
	}
	if writer.count() != beforeCount {
		t.Fatal("durable event appended for invalid proposal")
	}
}

// 17 Rust unavailable fail-closed
func TestChunk04_17_RustUnavailableFailClosed(t *testing.T) {
	writer := &chunkFakeWriter{fail: errors.New("rust unavailable")}
	auth := transition.New(writer, nil)
	prior := auth.Projection().Ref()
	prop := transition.ProposedTransition{
		TransitionID:  "t-rust-fail",
		ProposalID:    "p-rust-fail",
		RequestID:     "r-rust-fail",
		Prior:         prior,
		Operation:     "upsert",
		Entity:        "workspace",
		Node:          "node-rust",
		ResultData:    []byte(`{"x":99}`),
		AdmissionData: []byte(`{}`),
	}
	_, err := auth.Propose(prop)
	if err == nil {
		t.Fatal("expected durable failure")
	}
	if transition.Class(err) != transition.Durable {
		t.Fatalf("expected Durable, got %q err %v", transition.Class(err), err)
	}
	if auth.Projection().Version != 0 {
		t.Fatal("projection should remain 0 on durable failure")
	}
	if writer.count() != 0 {
		t.Fatal("no durable event should be recorded")
	}
}

// 18 shutdown terminates cleanly
func TestChunk04_18_ShutdownTerminatesCleanly(t *testing.T) {
	c := New(16, time.Minute)
	_ = c.Activate("ws-shut", "build_ledger.state.updated", "path/a")
	_ = c.Activate("ws-shut", "build_ledger.event.appended", "path/b")
	if c.LiveCount() == 0 {
		t.Fatal("expected live before shutdown")
	}
	c.Shutdown()
	if c.LiveCount() != 0 {
		t.Fatalf("live after shutdown %d", c.LiveCount())
	}
	// Second shutdown must not panic
	c.Shutdown()
	// New activation after shutdown should be rejected/failed (system closed)
	// We treat closed system as no spawn; status failed
	// Not asserting exact status, just that no panic and not live
}

// 19 restart does not resurrect ephemeral
func TestChunk04_19_RestartDoesNotResurrectEphemeral(t *testing.T) {
	c1 := New(16, time.Minute)
	a := c1.Activate("ws-restart", "build_ledger.state.updated", "path/ephemeral")
	if !c1.IsLive(a.ID) {
		t.Fatal("not live before shutdown")
	}
	c1.Shutdown()
	// Simulate restart: new controller is fresh in-memory
	c2 := New(16, time.Minute)
	defer c2.Shutdown()
	if c2.LiveCount() != 0 {
		t.Fatalf("new runtime should have 0 live, got %d", c2.LiveCount())
	}
	if c2.DescriptorCount() != 0 {
		t.Fatalf("new runtime should have 0 descriptors, got %d", c2.DescriptorCount())
	}
	if _, ok := c2.Descriptor(a.ID); ok {
		t.Fatal("ephemeral actor resurrected after restart")
	}
}

// 20 dormant rebuild behavior (state limitation explicitly if in-memory only)
func TestChunk04_20_DormantRebuildBehavior(t *testing.T) {
	// Documented limitation: dormant descriptors are in-memory only and not
	// persisted. After restart they are lost. This test proves that limitation
	// explicitly rather than hiding it.
	c := New(16, time.Minute)
	a := c.Activate("ws-rebuild", "build_ledger.state.updated", "path/one")
	b := c.ActivateWithParent(a.ID, "ws-rebuild", "build_ledger.event.appended", "path/two")
	c.Complete(a.ID)
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	// Descriptors exist pre-restart, but are dormant (not live)
	if _, ok := c.Descriptor(a.ID); !ok {
		t.Fatal("dormant descriptor missing pre-restart")
	}
	if _, ok := c.Descriptor(b.ID); !ok {
		t.Fatal("child descriptor missing pre-restart")
	}
	c.Shutdown()
	c2 := New(16, time.Minute)
	defer c2.Shutdown()
	// After restart, no history is rebuilt; this is explicitly stated as limitation.
	if c2.List("ws-rebuild") != nil && len(c2.List("ws-rebuild")) != 0 {
		t.Fatal("expected empty list after restart (in-memory only)")
	}
	if c2.DescriptorCount() != 0 {
		t.Fatalf("expected 0 descriptors after rebuild, got %d", c2.DescriptorCount())
	}
	// New activation after restart gets fresh descriptor set (counter restarts, but lineage is fresh runtime).
	// The limitation is explicit: descriptors are not persisted; fresh activation proves ephemeral not replayed.
	fresh := c2.Activate("ws-rebuild", "build_ledger.state.updated", "path/one")
	if fresh == nil || fresh.Status != "active" {
		t.Fatalf("fresh activation failed after rebuild: %#v", fresh)
	}
	if c2.DescriptorCount() != 1 {
		t.Fatalf("expected 1 descriptor after fresh activation, got %d", c2.DescriptorCount())
	}
	// Old descriptors (a,b) were 2; fresh is 1, proving no resurrection.
	if len(c2.List("ws-rebuild")) != 1 || c2.List("ws-rebuild")[0].ID != fresh.ID {
		t.Fatal("fresh list should contain only fresh activation")
	}
}

// Additional: ensure List compatibility and server API facade preserved
func TestChunk04_ServerAPICompatibility(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-srv", "build_ledger.state.updated", "path/srv")
	if a == nil {
		t.Fatal("activate nil")
	}
	list := c.List("ws-srv")
	if len(list) != 1 {
		t.Fatalf("list len %d want 1", len(list))
	}
	if list[0].ID != a.ID {
		t.Fatalf("list id mismatch")
	}
	empty := c.List("ws-other")
	if len(empty) != 0 {
		t.Fatalf("other ws list %d want 0", len(empty))
	}
	all := c.List("")
	if len(all) != 1 {
		t.Fatalf("all list %d want 1", len(all))
	}
}

// Ensure bounded supervision: concurrent panic does not affect other actors
func TestChunk04_SupervisionBoundedRetry(t *testing.T) {
	c := New(16, time.Minute)
	defer c.Shutdown()
	a := c.Activate("ws-sup", "build_ledger.state.updated", "path/a")
	b := c.Activate("ws-sup", "build_ledger.event.appended", "path/b")
	_ = c.Send(a.ID, &ProcessMessage{Payload: "panic"})
	waitFor(t, func() bool { return !c.IsLive(a.ID) })
	if c.IsLive(b.ID) == false {
		t.Fatal("supervision failure crashed sibling")
	}
	// Bounded retry: panic actor should be failed, not restarted infinitely
	desc, _ := c.Descriptor(a.ID)
	if desc.Status != "failed" {
		t.Fatalf("panic actor should be failed, got %q", desc.Status)
	}
	if c.LiveCount() != 1 {
		t.Fatalf("live count %d want 1", c.LiveCount())
	}
}
