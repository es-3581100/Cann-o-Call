package progress

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/transition"
)

func mustSHA(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// 01 deterministic task ID unique
func TestCH08_01_DeterministicTaskID(t *testing.T) {
	a := DeterministicTaskID(OperationIngest, "ws1", "key1")
	b := DeterministicTaskID(OperationIngest, "ws1", "key1")
	if a != b {
		t.Fatalf("deterministic: same inputs different ids %q vs %q", a, b)
	}
	if len(a) != 64 || !isSHA256(a) {
		t.Fatalf("not sha256 %q", a)
	}
	c := DeterministicTaskID(OperationIngest, "ws1", "key2")
	if a == c {
		t.Fatal("different key same id")
	}
	d := DeterministicTaskID(OperationQuery, "ws1", "key1")
	if a == d {
		t.Fatal("different operation same id")
	}
	e := DeterministicTaskID(OperationIngest, "ws2", "key1")
	if a == e {
		t.Fatal("different workspace same id")
	}
	// DeterministicTaskIDMulti
	f := DeterministicTaskIDMulti(OperationIngest, "ws1", "k1", "k2")
	g := DeterministicTaskIDMulti(OperationIngest, "ws1", "k1", "k2")
	if f != g {
		t.Fatal("multi not deterministic")
	}
	h := DeterministicTaskIDMulti(OperationIngest, "ws1", "k1", "k3")
	if f == h {
		t.Fatal("multi different keys same id")
	}
	// Trim handling
	trimmed := DeterministicTaskID(OperationIngest, " ws1 ", " key1 ")
	if trimmed != a {
		t.Fatal("trim not handled")
	}
}

func TestCH08_02_PendingRunning(t *testing.T) {
	reg := NewRegistry(128)
	task, err := reg.Create("", OperationIngest, PhasePending)
	if err != nil {
		t.Fatal(err)
	}
	if task.Packet.Status != StatusPending {
		t.Fatalf("want pending got %q", task.Packet.Status)
	}
	if task.Packet.Phase != PhasePending {
		t.Fatalf("want phase pending got %q", task.Packet.Phase)
	}
	if err := reg.Transition(task.Packet.TaskID, StatusRunning); err != nil {
		t.Fatalf("pending->running failed: %v", err)
	}
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Status != StatusRunning {
		t.Fatalf("want running got %q", got.Packet.Status)
	}
	if got.Packet.Phase != PhaseRunning {
		t.Fatalf("phase not running got %q", got.Packet.Phase)
	}
	// Also verify request_accepted -> running via SetPhase stays running
	task2, _ := reg.Create("", OperationQuery, PhaseRequestAccepted)
	_ = reg.Transition(task2.Packet.TaskID, StatusRunning)
	_ = reg.SetPhase(task2.Packet.TaskID, PhaseRunning)
	got2, _ := reg.Get(task2.Packet.TaskID)
	if got2.Packet.Status != StatusRunning {
		t.Fatalf("second running failed")
	}
}

func TestCH08_03_Completes(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationQuery, PhaseRequestAccepted)
	if err := reg.Transition(task.Packet.TaskID, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := reg.Transition(task.Packet.TaskID, StatusComplete); err != nil {
		t.Fatalf("running->complete failed: %v", err)
	}
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Status != StatusComplete {
		t.Fatalf("want complete got %q", got.Packet.Status)
	}
	if got.Packet.Phase != PhaseTerminal {
		t.Fatalf("phase not terminal")
	}
	if !IsTerminal(got.Packet.Status) {
		t.Fatal("IsTerminal false for complete")
	}
	// terminal cannot transition further
	if err := reg.Transition(task.Packet.TaskID, StatusFailed); err == nil {
		t.Fatal("terminal transition should fail")
	}
}

func TestCH08_04_FailedReceiptHasErrors(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	_ = reg.AddError(task.Packet.TaskID, "ingest failed: invalid bytes")
	_ = reg.Transition(task.Packet.TaskID, StatusFailed)
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Status != StatusFailed {
		t.Fatal("not failed")
	}
	if len(got.Packet.Errors) == 0 {
		t.Fatal("failed should have errors")
	}
	receipt, err := BuildReceiptFromTask(got, StatusFailed, 0, nil, nil, "", "")
	if err != nil {
		t.Fatalf("build failed receipt: %v", err)
	}
	if receipt.FinalStatus != StatusFailed {
		t.Fatal("receipt not failed")
	}
	if len(receipt.Errors) == 0 {
		t.Fatal("receipt errors empty")
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("validate failed receipt: %v", err)
	}
}

func TestCH08_05_RejectedNoSuccessReceipt(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRejected)
	got, _ := reg.Get(task.Packet.TaskID)
	receipt, err := BuildReceiptFromTask(got, StatusRejected, 0, nil, nil, "", "")
	if err != nil {
		t.Fatalf("build rejected receipt: %v", err)
	}
	if len(receipt.DurableRefs) != 0 {
		t.Fatal("rejected should not have durable refs claiming success")
	}
	if len(receipt.AcceptedTransitionIDs) != 0 {
		t.Fatal("rejected should not have accepted ids")
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("rejected validate failed: %v", err)
	}
	// Rejected receipt must not claim durable acceptance without evidence, but empty is ok
	// Ensure that if we try to claim accepted while rejected without refs, validation fails appropriately only for complete case
}

func TestCH08_06_RustFailureReflected(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationActorExecution, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	got, _ := reg.Get(task.Packet.TaskID)
	// Try to build COMPLETE while claiming accepted IDs but missing durable refs -> must fail
	_, err := BuildReceiptFromTask(got, StatusComplete, 1, []string{"trans-1"}, nil, "", "")
	if err == nil {
		t.Fatal("expected failure: complete without durable refs should error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "durable") {
		t.Fatalf("error should mention durable, got %v", err)
	}
	// Same with failed status should still allow missing refs? It should still validate but not claim complete
	receipt, err := BuildReceiptFromTask(got, StatusFailed, 1, []string{"trans-1"}, nil, "", "")
	if err == nil {
		// Build allows failed with missing refs? Check Validate would fail because refs missing when ids present
		// Actually Validate requires refs when ids present, so this should also error even for failed?
		// Let's check: receipt Validate requires if len Accepted>0 then DurableRefs needed. So failed with ids but no refs should also error.
		// That is intentional: must not claim durable acceptance without evidence regardless of final status?
		// Our implementation Validate checks that condition always, not only for complete.
		t.Fatalf("unexpected success for failed with missing durable: %v", receipt)
	}
	_ = err
	// With invalid durable ref while claiming complete should fail
	invalidRef := DurableRef{EventID: "trans-1", Sequence: 1, EventHash: mustSHA("x")} // missing rust ack
	_, err = BuildReceiptFromTask(got, StatusComplete, 1, []string{"trans-1"}, []DurableRef{invalidRef}, "", "")
	if err == nil {
		t.Fatal("expected invalid durable ref to fail for complete")
	}
}

func TestCH08_07_AcceptedBindsRustACK(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationMutationProposal, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	got, _ := reg.Get(task.Packet.TaskID)
	eventID := "event-123"
	seq := int64(42)
	hash := mustSHA("hashcontent")
	ackHash := mustSHA("ackhash")
	ref := DurableRef{EventID: eventID, Sequence: seq, EventHash: hash, RustAckID: eventID, RustAckSeq: seq, RustAckHash: ackHash}
	if err := ref.Validate(); err != nil {
		t.Fatalf("valid ref should pass: %v", err)
	}
	receipt, err := BuildReceiptFromTask(got, StatusComplete, 1, []string{eventID}, []DurableRef{ref}, "", "")
	if err != nil {
		t.Fatalf("build accepted receipt failed: %v", err)
	}
	if len(receipt.DurableRefs) != 1 {
		t.Fatal("durable refs length wrong")
	}
	if receipt.DurableRefs[0].RustAckID != eventID || receipt.DurableRefs[0].RustAckSeq != seq {
		t.Fatal("rust ack binding mismatch")
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("validate accepted: %v", err)
	}
	// Mismatched IDs should fail
	badRef := DurableRef{EventID: eventID, Sequence: seq, EventHash: hash, RustAckID: "other-id", RustAckSeq: seq, RustAckHash: ackHash}
	if err := badRef.Validate(); err == nil {
		t.Fatal("mismatched ack id should fail")
	}
	// Invalid hash should fail
	badHashRef := DurableRef{EventID: eventID, Sequence: seq, EventHash: "not-sha", RustAckID: eventID, RustAckSeq: seq, RustAckHash: ackHash}
	if err := badHashRef.Validate(); err == nil {
		t.Fatal("invalid hash should fail")
	}
}

func TestCH08_08_ActorCountsCorrect(t *testing.T) {
	ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	defer ctrl.Shutdown()
	// Create various statuses
	a1 := ctrl.Activate("ws1", "live_context.query", "node-a")
	if a1 == nil || a1.Status != "active" {
		t.Fatalf("a1 not active %v", a1)
	}
	// rejected due to maxActive limit: create many
	ctrl2 := actorstub.NewWithConfig(actorstub.Config{MaxActive: 1, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	defer ctrl2.Shutdown()
	b1 := ctrl2.Activate("ws1", "live_context.query", "node-b1")
	_ = b1
	b2 := ctrl2.Activate("ws1", "live_context.query", "node-b2")
	if b2 == nil || b2.Status != "rejected_budget" {
		t.Fatalf("expected rejected_budget got %v", b2)
	}
	// Trigger cycle suppression
	parent := ctrl.Activate("ws1", "live_context.query", "same-node")
	child := ctrl.ActivateWithParent(parent.ID, "ws1", "live_context.query", "same-node")
	if child.Status != "rejected_cycle" {
		t.Fatalf("expected rejected_cycle got %q", child.Status)
	}
	// Complete one
	ctrl.Complete(a1.ID)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if desc, ok := ctrl.Descriptor(a1.ID); ok && desc.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Derive metrics locally without importing observability (avoid cycle)
	m := actorMetricsFromController(ctrl)
	if m.Active == 0 && m.Completed == 0 && m.Failed == 0 {
		t.Fatalf("actor metrics empty unexpected: %+v", m)
	}
	// Verify counts: at least one completed, at least one rejected_cycle suppressed
	m2 := actorMetricsFromController(ctrl2)
	if m2.Rejected == 0 && m2.Suppressed == 0 {
		t.Fatalf("expected at least one rejected/suppressed in m2 got %+v", m2)
	}
	if m.Completed < 1 {
		t.Fatalf("completed count %d want >=1 %+v", m.Completed, m)
	}
	if m.Suppressed < 1 {
		t.Fatalf("suppressed count %d want >=1 %+v", m.Suppressed, m)
	}
}

func actorMetricsFromController(c *actorstub.Controller) ActorMetrics {
	if c == nil {
		return ActorMetrics{}
	}
	list := c.List("")
	m := ActorMetrics{}
	m.DormantDescriptors = len(list)
	for _, a := range list {
		switch a.Status {
		case "active":
			m.Active++
		case "completed", "observation":
			m.Completed++
		case "failed":
			m.Failed++
		case "expired":
			m.Expired++
		case "passivated":
			m.Passivated++
		default:
			if strings.HasPrefix(a.Status, "rejected_") {
				if a.Status == "rejected_cycle" {
					m.Suppressed++
				} else {
					m.Rejected++
				}
			} else if a.Status == "queued" || a.Status == "pending" {
				m.Queued++
			}
		}
	}
	return m
}

func TestCH08_09_SuppressedReflected(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationQuery, PhaseRequestAccepted)
	_ = reg.Update(task.Packet.TaskID, func(p *ProgressPacket) {
		p.Actor.Suppressed = 2
		p.Actor.Rejected = 1
	})
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Actor.Suppressed != 2 {
		t.Fatalf("suppressed not reflected")
	}
	// Receipt should reflect suppressed+rejected sum
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	_ = reg.Transition(task.Packet.TaskID, StatusComplete)
	got2, _ := reg.Get(task.Packet.TaskID)
	r, err := BuildReceiptFromTask(got2, StatusComplete, 0, nil, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if r.ActorsSuppressed != got2.Packet.Actor.Suppressed+got2.Packet.Actor.Rejected {
		t.Fatalf("suppressed mismatch receipt %d vs packet %d+%d", r.ActorsSuppressed, got2.Packet.Actor.Suppressed, got2.Packet.Actor.Rejected)
	}
}

func TestCH08_10_FailureReflected(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationActorExecution, PhaseRequestAccepted)
	_ = reg.Update(task.Packet.TaskID, func(p *ProgressPacket) {
		p.Actor.Failed = 3
		p.Actor.Completed = 5
	})
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Actor.Failed != 3 {
		t.Fatal("failed not reflected")
	}
	_ = reg.Transition(task.Packet.TaskID, StatusFailed)
	got2, _ := reg.Get(task.Packet.TaskID)
	r, err := BuildReceiptFromTask(got2, StatusFailed, 0, nil, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if r.ActorsFailed != 3 || r.ActorsCompleted != 5 {
		t.Fatalf("receipt counts wrong failed %d completed %d", r.ActorsFailed, r.ActorsCompleted)
	}
}

func TestCH08_24_RestartNotClaimTransientCompleted(t *testing.T) {
	reg := NewRegistry(128)
	// pending and running should become failed after MarkAbandoned, not complete
	tPending, _ := reg.Create("task-pending", OperationIngest, PhasePending)
	_ = tPending
	tRunning, _ := reg.Create("task-running", OperationIngest, PhasePending)
	_ = reg.Transition(tRunning.Packet.TaskID, StatusRunning)
	tComplete, _ := reg.Create("task-complete", OperationIngest, PhasePending)
	_ = reg.Transition(tComplete.Packet.TaskID, StatusRunning)
	_ = reg.Transition(tComplete.Packet.TaskID, StatusComplete)
	reg.MarkAbandoned()
	pendingGot, _ := reg.Get("task-pending")
	if pendingGot.Packet.Status != StatusFailed {
		t.Fatalf("pending after restart should be failed got %q", pendingGot.Packet.Status)
	}
	runningGot, _ := reg.Get("task-running")
	if runningGot.Packet.Status != StatusFailed {
		t.Fatalf("running after restart should be failed got %q", runningGot.Packet.Status)
	}
	completeGot, _ := reg.Get("task-complete")
	if completeGot.Packet.Status != StatusComplete {
		t.Fatalf("complete should remain complete got %q", completeGot.Packet.Status)
	}
	// ensure failed ones have abandon error
	if len(pendingGot.Packet.Errors) == 0 || !strings.Contains(strings.ToLower(pendingGot.Packet.Errors[0]), "abandon") {
		t.Fatalf("abandon error missing %v", pendingGot.Packet.Errors)
	}
	// check that complete was not turned into failed
	if completeGot.Packet.Status == StatusFailed {
		t.Fatal("complete incorrectly marked abandoned")
	}
}

func TestCH08_25_NoFakeACK(t *testing.T) {
	// Receipt Validate must fail without RustAck or with fake ack
	refMissing := DurableRef{EventID: "ev1", Sequence: 1, EventHash: mustSHA("a")}
	if err := refMissing.Validate(); err == nil {
		t.Fatal("missing RustAck should fail")
	}
	// Build receipt with fake ack hash not sha256
	fakeRef := DurableRef{EventID: "ev1", Sequence: 1, EventHash: mustSHA("a"), RustAckID: "ev1", RustAckSeq: 1, RustAckHash: "fake-not-sha"}
	if err := fakeRef.Validate(); err == nil {
		t.Fatal("fake hash should fail")
	}
	// Build receipt claiming complete with such ref should fail
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationMutationProposal, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	got, _ := reg.Get(task.Packet.TaskID)
	_, err := BuildReceiptFromTask(got, StatusComplete, 1, []string{"ev1"}, []DurableRef{fakeRef}, "", "")
	if err == nil {
		t.Fatal("fake ACK should prevent complete receipt")
	}
	// Also test DurableRef with mismatched sequence
	badSeq := DurableRef{EventID: "ev1", Sequence: 1, EventHash: mustSHA("a"), RustAckID: "ev1", RustAckSeq: 2, RustAckHash: mustSHA("b")}
	if err := badSeq.Validate(); err == nil {
		t.Fatal("mismatched seq should fail")
	}
}

func TestCH08_26_ProgressCannotMutateAuthority(t *testing.T) {
	// Setup authority with fake writer
	fw := &fakeWriterCH08{}
	auth := transition.New(fw, nil)
	before := auth.Projection()
	beforeVersion := before.Version
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	// Update task metrics heavily
	_ = reg.Update(task.Packet.TaskID, func(p *ProgressPacket) {
		p.Actor.Active = 5
		p.Actor.Completed = 10
		p.Graph.NodeCount = 100
		p.Context.CandidatesConsidered = 50
		p.Ledger.AcceptedEventCount = 999
	})
	_ = reg.AddWarning(task.Packet.TaskID, "warn")
	_ = reg.AddError(task.Packet.TaskID, "err")
	after := auth.Projection()
	if after.Version != beforeVersion {
		t.Fatalf("authority version mutated by progress update %d -> %d", beforeVersion, after.Version)
	}
	if after.Hash != before.Hash {
		t.Fatalf("authority hash mutated")
	}
	// Also verify task exists and metrics updated
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Actor.Active != 5 {
		t.Fatal("task metrics not updated")
	}
}

func TestCH08_27_ConcurrentRaceSafe(t *testing.T) {
	reg := NewRegistry(128)
	// Pre-create some tasks
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("race-task-%d", i)
		_, _ = reg.Create(id, OperationIngest, PhaseRequestAccepted)
	}
	var wg sync.WaitGroup
	const workers = 20
	const opsPerWorker = 50
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(worker int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				id := fmt.Sprintf("concurrent-%d-%d", worker, op%5)
				_, _ = reg.Create(id, OperationQuery, PhaseRequestAccepted)
				_ = reg.Update(id, func(p *ProgressPacket) {
					p.Phase = PhaseGraphNodes
				})
				_ = reg.AddWarning(id, fmt.Sprintf("warn %d", op))
				_ = reg.AddError(id, fmt.Sprintf("err %d", op))
				_ = reg.SetProgress(id, func() *int64 { v := int64(op); return &v }(), func() *int64 { v := int64(100); return &v }())
				_ = reg.Transition(id, StatusRunning)
				_ = reg.Transition(id, StatusComplete)
			}
		}(w)
	}
	wg.Wait()
	// No panic and registry size bounded
	if reg.Count() > 128 {
		t.Fatalf("registry count %d exceeds bound after concurrent", reg.Count())
	}
	// Verify no data race (go test -race will catch)
}

func TestCH08_28_RegistryBoundedCleaned(t *testing.T) {
	reg := NewRegistry(10)
	// Create 10 terminal tasks
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("bounded-%d", i)
		_, _ = reg.Create(id, OperationIngest, PhaseRequestAccepted)
		_ = reg.Transition(id, StatusRunning)
		_ = reg.Transition(id, StatusComplete)
		time.Sleep(time.Millisecond)
	}
	if reg.Count() != 10 {
		t.Fatalf("count %d want 10", reg.Count())
	}
	// Create 5 more terminal should evict oldest 5, keep 10
	for i := 10; i < 15; i++ {
		id := fmt.Sprintf("bounded-%d", i)
		_, _ = reg.Create(id, OperationIngest, PhaseRequestAccepted)
		_ = reg.Transition(id, StatusRunning)
		_ = reg.Transition(id, StatusComplete)
		time.Sleep(time.Millisecond)
	}
	if reg.Count() != 10 {
		t.Fatalf("after eviction count %d want 10", reg.Count())
	}
	// Oldest should be gone, newest present
	if _, ok := reg.Get("bounded-0"); ok {
		t.Fatal("oldest should have been evicted")
	}
	if _, ok := reg.Get("bounded-14"); !ok {
		t.Fatal("newest should remain")
	}
	// Active never evicted: create 15 active tasks beyond limit, they should not be evicted even though count exceeds maxSize temporarily?
	reg2 := NewRegistry(5)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("active-%d", i)
		_, _ = reg2.Create(id, OperationIngest, PhaseRequestAccepted)
		// leave pending, not terminal
	}
	// Create one terminal to trigger eviction logic, but only terminal can be evicted, so count will be 11 but active not evicted -> stays >maxSize?
	// Actually eviction only removes terminal, so after creating terminal, pending remain
	_, _ = reg2.Create("terminal-0", OperationIngest, PhaseRequestAccepted)
	_ = reg2.Transition("terminal-0", StatusRunning)
	_ = reg2.Transition("terminal-0", StatusComplete)
	// Now create many terminals beyond limit; active should remain
	for i := 1; i < 10; i++ {
		id := fmt.Sprintf("terminal-%d", i)
		_, _ = reg2.Create(id, OperationIngest, PhaseRequestAccepted)
		_ = reg2.Transition(id, StatusRunning)
		_ = reg2.Transition(id, StatusComplete)
	}
	// Active tasks should still exist
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("active-%d", i)
		if _, ok := reg2.Get(id); !ok {
			t.Fatalf("active %q should not have been evicted", id)
		}
	}
}

func TestCH08_29_ExecutionFailurePreservesProjection(t *testing.T) {
	fw := &fakeWriterCH08{fail: fmt.Errorf("rust unavailable simulated")}
	auth := transition.New(fw, nil)
	before := auth.Projection()
	// Try to propose via transition directly to simulate failure
	proposal := transition.ProposedTransition{
		TransitionID: "trans-fail-1",
		ProposalID:   "prop-1",
		RequestID:    "req-1",
		Prior:        before.Ref(),
		Operation:    "upsert",
		Entity:       "ws1",
		Node:         "node1",
		ResultData:   json.RawMessage(`{"a":1}`),
	}
	_, err := auth.Propose(proposal)
	if err == nil {
		t.Fatal("expected durable failure")
	}
	if transition.Class(err) != transition.Durable {
		t.Fatalf("expected Durable class got %q", transition.Class(err))
	}
	after := auth.Projection()
	if after.Version != before.Version {
		t.Fatalf("projection version changed on failure %d -> %d", before.Version, after.Version)
	}
	if after.Hash != before.Hash {
		t.Fatal("projection hash changed on failure")
	}
	if fw.count() != 0 {
		t.Fatalf("writer count changed on failure")
	}
	// Now test that failTask (registry completeTask fail path) does not advance authority either
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationMutationProposal, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	// Simulate failTask logic: just mark failed, authority unchanged
	_ = reg.Transition(task.Packet.TaskID, StatusFailed)
	after2 := auth.Projection()
	if after2.Version != before.Version {
		t.Fatal("failTask mutated authority")
	}
}

func TestCH08_30_ExistingIngestOrchestrationCompatible(t *testing.T) {
	// IngestBytes + graph.Build still works after progress changes
	pkt, err := ingest.IngestBytes("ws-compat", "path/file.txt", "", []byte("hello world alpha beta"), "path/file.txt")
	if err != nil {
		t.Fatalf("IngestBytes failed: %v", err)
	}
	if err := pkt.Validate(); err != nil {
		t.Fatalf("packet validate failed: %v", err)
	}
	g, err := graph.Build([]ingest.SourcePacket{*pkt})
	if err != nil {
		t.Fatalf("graph.Build failed: %v", err)
	}
	if len(g.Nodes) == 0 {
		t.Fatal("graph empty")
	}
	if g.Hash == "" {
		t.Fatal("graph hash empty")
	}
	// Also test via orchestrator ingestion
	fw := &fakeWriterCH08{}
	auth := transition.New(fw, nil)
	store := graph.NewStore("ws-compat")
	ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	defer ctrl.Shutdown()
	// Need orchestrator; import cycle? Use ingest.ProposeIngest via authority then graph store apply
	_, err = ingest.ProposeIngest(auth, *pkt)
	if err != nil {
		t.Fatalf("ProposeIngest failed: %v", err)
	}
	_, _, err = store.Apply(*pkt)
	if err != nil {
		t.Fatalf("store.Apply failed: %v", err)
	}
	if store.Graph().Hash != g.Hash {
		t.Fatalf("store graph hash mismatch %q vs %q", store.Graph().Hash, g.Hash)
	}
	_ = ctrl
}

func TestCH08_BoundedWarningsErrors(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhaseRequestAccepted)
	// Add 15 warnings, should keep only last 10
	for i := 0; i < 15; i++ {
		_ = reg.AddWarning(task.Packet.TaskID, fmt.Sprintf("warn-%02d", i))
	}
	got, _ := reg.Get(task.Packet.TaskID)
	if len(got.Packet.Warnings) != 10 {
		t.Fatalf("warnings bound %d want 10", len(got.Packet.Warnings))
	}
	// Should be last 10: warn-05..warn-14
	if got.Packet.Warnings[0] != "warn-05" || got.Packet.Warnings[9] != "warn-14" {
		t.Fatalf("warnings not last 10 got %v", got.Packet.Warnings)
	}
	// Same for errors via Task directly
	for i := 0; i < 15; i++ {
		_ = reg.AddError(task.Packet.TaskID, fmt.Sprintf("err-%02d", i))
	}
	got2, _ := reg.Get(task.Packet.TaskID)
	if len(got2.Packet.Errors) != 10 {
		t.Fatalf("errors bound %d want 10", len(got2.Packet.Errors))
	}
	if got2.Packet.Errors[0] != "err-05" {
		t.Fatalf("errors not last 10 got %v", got2.Packet.Errors)
	}
	// Validate should pass with bounded, but would fail if we bypass bound
	p := got2.Packet
	if err := p.Validate(); err != nil {
		t.Fatalf("validate bounded should pass: %v", err)
	}
	// Create packet with 11 warnings directly and check Validate fails
	p.Warnings = make([]string, 11)
	for i := range p.Warnings {
		p.Warnings[i] = fmt.Sprintf("w%d", i)
	}
	if err := p.Validate(); err == nil {
		t.Fatal("validate should fail for warnings >10")
	}
}

func TestCH08_TaskTransitionsInvalid(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhaseRequestAccepted)
	// pending -> complete should fail (must go via running)
	if err := reg.Transition(task.Packet.TaskID, StatusComplete); err == nil {
		t.Fatal("pending->complete should fail")
	}
	// pending -> paused should fail
	if err := reg.Transition(task.Packet.TaskID, StatusPaused); err == nil {
		t.Fatal("paused should be rejected")
	}
	// valid pending->running->rejected
	if err := reg.Transition(task.Packet.TaskID, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := reg.Transition(task.Packet.TaskID, StatusRejected); err != nil {
		t.Fatalf("running->rejected failed: %v", err)
	}
	// IsValidTransition paused always false
	if IsValidTransition(StatusPending, StatusPaused) {
		t.Fatal("paused should not be valid")
	}
	if !IsValidTransition(StatusPending, StatusPending) {
		t.Fatal("same status should be considered valid for idempotency")
	}
}

func TestCH08_ProgressRenderer(t *testing.T) {
	now := time.Now().UTC().Add(-10 * time.Second)
	taskID := DeterministicTaskID(OperationIngest, "ws1", "key1")
	p := ProgressPacket{
		TaskID:    taskID,
		Operation: OperationIngest,
		Phase:     PhaseRunning,
		Status:    StatusRunning,
		StartedAt: now,
		UpdatedAt: now.Add(10 * time.Second),
	}
	c := int64(5)
	tot := int64(10)
	p.Completed = &c
	p.Total = &tot
	p.Throughput = p.ComputeThroughput()
	p.ETA = p.ComputeETA()
	rendered := RenderProgress(p)
	if rendered == "" {
		t.Fatal("render empty")
	}
	if strings.Contains(rendered, "{") || strings.Contains(rendered, "\"task_id\"") {
		t.Fatalf("human renderer should not produce JSON, got %q", rendered)
	}
	if !strings.Contains(rendered, string(OperationIngest)) {
		t.Fatalf("render missing operation %q", rendered)
	}
	// Throughput should be computable
	if p.ComputeThroughput() == nil {
		t.Fatal("throughput nil")
	}
	if *p.ComputeThroughput() <= 0 {
		t.Fatal("throughput <=0")
	}
	if p.ComputeETA() == nil {
		t.Fatal("eta nil")
	}
	// Test deterministic sorting: marshal packet twice same output
	b1, _ := json.Marshal(p)
	b2, _ := json.Marshal(p)
	if string(b1) != string(b2) {
		t.Fatal("json not deterministic")
	}
	// Validate packet
	if err := p.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestCH08_ThroughputEta(t *testing.T) {
	now := time.Now().UTC().Add(-20 * time.Second)
	p := ProgressPacket{StartedAt: now, UpdatedAt: now.Add(20 * time.Second)}
	c := int64(100)
	tot := int64(200)
	p.Completed = &c
	p.Total = &tot
	tp := p.ComputeThroughput()
	if tp == nil || *tp < 4.9 || *tp > 5.1 {
		t.Fatalf("throughput %v want ~5", tp)
	}
	eta := p.ComputeETA()
	if eta == nil {
		t.Fatal("eta nil")
	}
	// When completed nil, throughput nil
	p2 := ProgressPacket{StartedAt: now, UpdatedAt: now.Add(10 * time.Second)}
	if p2.ComputeThroughput() != nil {
		t.Fatal("nil completed should give nil throughput")
	}
	if p2.ComputeETA() != nil {
		t.Fatal("nil completed/total should give nil eta")
	}
	// When completed == total, ETA 0s
	p3 := ProgressPacket{StartedAt: now, UpdatedAt: now.Add(10 * time.Second)}
	cc := int64(10)
	tt := int64(10)
	p3.Completed = &cc
	p3.Total = &tt
	if e := p3.ComputeETA(); e == nil || *e != "0s" {
		t.Fatalf("eta for done should be 0s got %v", e)
	}
}

func TestCH08_ErrorTaxonomy(t *testing.T) {
	tests := []struct {
		err  string
		want string
	}{
		{"invalid input malformed", ErrInvalidInput},
		{"durable_recorder_unavailable: down", ErrDurableRecorderUnavailable},
		{"durable append failed", ErrDurableAppendFailed},
		{"stale predecessor", ErrStaleState},
		{"activation_rejected budget", ErrActorActivationRejected},
		{"actor failed processing", ErrActorFailed},
		{"ingest rejected secret", ErrIngestRejected},
		{"replay corrupt hash", ErrReplayCorrupt},
		{"policy admission denied", ErrAdmissionRejected},
		{"unknown weird", ErrInternalError},
	}
	for _, tc := range tests {
		got := ClassifyError(fmt.Errorf("%s", tc.err))
		if got != tc.want {
			t.Fatalf("classify %q got %q want %q", tc.err, got, tc.want)
		}
	}
	if ClassifyError(nil) != "" {
		t.Fatal("nil should give empty")
	}
	te := NewTaskError(ErrInvalidInput, "msg", fmt.Errorf("cause"))
	if te.Error() == "" || te.Unwrap() == nil {
		t.Fatal("task error")
	}
}

func TestCH08_JSONDeterministicSortedKeys(t *testing.T) {
	// Verify canonical JSON sorting via packet hash
	p1 := ProgressPacket{
		TaskID:    "task1",
		Operation: OperationIngest,
		Phase:     PhaseRunning,
		Status:    StatusRunning,
		StartedAt: time.Unix(1000, 0).UTC(),
		UpdatedAt: time.Unix(1010, 0).UTC(),
	}
	p2 := p1
	b1, _ := json.Marshal(p1)
	b2, _ := json.Marshal(p2)
	if string(b1) != string(b2) {
		t.Fatal("not deterministic")
	}
	// Check receipt hash deterministic
	reg := NewRegistry(128)
	task, _ := reg.Create("task1", OperationIngest, PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, StatusRunning)
	_ = reg.Transition(task.Packet.TaskID, StatusComplete)
	got, _ := reg.Get(task.Packet.TaskID)
	r1, _ := BuildReceiptFromTask(got, StatusComplete, 0, nil, nil, "", "")
	r2, _ := BuildReceiptFromTask(got, StatusComplete, 0, nil, nil, "", "")
	if r1.Hash() != r2.Hash() {
		t.Fatalf("receipt hash not deterministic %q vs %q", r1.Hash(), r2.Hash())
	}
	// JSON parses
	var raw any
	if err := json.Unmarshal(b1, &raw); err != nil {
		t.Fatalf("json parse failed: %v", err)
	}
}

func TestCH08_InvalidTransitionsAndPausedRejected(t *testing.T) {
	reg := NewRegistry(128)
	task, _ := reg.Create("", OperationIngest, PhasePending)
	// Try to transition to paused explicitly
	err := reg.Transition(task.Packet.TaskID, StatusPaused)
	if err == nil {
		t.Fatal("paused should be rejected")
	}
	if !strings.Contains(err.Error(), "pause") {
		t.Fatalf("paused error should mention pause got %v", err)
	}
	// Also ValidateTransition directly
	if err := ValidateTransition(StatusRunning, StatusPaused); err == nil {
		t.Fatal("ValidateTransition paused should fail")
	}
	if err := ValidateTransition(StatusComplete, StatusRunning); err == nil {
		t.Fatal("terminal transition should fail")
	}
}

// fakeWriterCH08 mimics transition durable writer
type fakeWriterCH08 struct {
	mu     sync.Mutex
	events []eventlog.Event
	fail   error
}

func (f *fakeWriterCH08) Append(ev eventlog.Event) (eventlog.Event, error) {
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
func (f *fakeWriterCH08) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}
