package observability

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/orchestrator"
	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/transition"
)

type fakeWriterObs struct {
	events []eventlog.Event
	fail   error
}

func (f *fakeWriterObs) Append(ev eventlog.Event) (eventlog.Event, error) {
	if f.fail != nil {
		return eventlog.Event{}, f.fail
	}
	ev.Seq = int64(len(f.events) + 1)
	ev.Hash = strings.Repeat("a", 64)
	ev.RustAck = &eventlog.RustAcknowledgement{ID: ev.ID, Sequence: ev.Seq, Hash: strings.Repeat("a", 64)}
	f.events = append(f.events, ev)
	return ev, nil
}

func TestCH08_11_IngestProgress(t *testing.T) {
	// Ingest progress observable via registry and graph metrics
	wsID := "ws-ingest-progress"
	reg := progress.NewRegistry(128)
	task, err := reg.CreateDeterministic(progress.OperationIngest, wsID, "path/file.txt", progress.PhaseRequestAccepted)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Transition(task.Packet.TaskID, progress.StatusRunning); err != nil {
		t.Fatal(err)
	}
	// Simulate ingest via ingest.IngestBytes and graph store
	store := graph.NewStore(wsID)
	pkt, err := ingest.IngestBytes(wsID, "path/file.txt", "", []byte("alpha beta gamma ingest progress"), "path/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Apply(*pkt)
	if err != nil {
		t.Fatal(err)
	}
	// Update task with graph metrics
	gm := GraphMetricsFromStore(store)
	_ = reg.Update(task.Packet.TaskID, func(p *progress.ProgressPacket) {
		p.Graph = gm
		c := int64(1)
		tot := int64(1)
		p.Completed = &c
		p.Total = &tot
		p.Phase = progress.PhaseDurableAppend
	})
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Graph.NodeCount == 0 {
		t.Fatalf("graph metrics not reflected %+v", got.Packet.Graph)
	}
	if got.Packet.Completed == nil || *got.Packet.Completed != 1 {
		t.Fatalf("completed not reflected")
	}
	_ = reg.Transition(task.Packet.TaskID, progress.StatusComplete)
	got2, _ := reg.Get(task.Packet.TaskID)
	if got2.Packet.Status != progress.StatusComplete {
		t.Fatal("ingest task should complete")
	}
}

func TestCH08_12_QueryCandidateSelectedCounts(t *testing.T) {
	wsID := "ws-query-counts"
	store := graph.NewStore(wsID)
	fw := &fakeWriterObs{}
	auth := transition.New(fw, nil)
	ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	defer ctrl.Shutdown()
	cfg := scoring.Config{}.WithDefaults()
	orch := orchestrator.New(wsID, store, auth, ctrl, cfg)
	// Ingest two files
	for i, txt := range []string{"alpha beta gamma", "alpha beta"} {
		pkt, err := ingest.IngestBytes(wsID, fmt.Sprintf("path/file%d.txt", i), "", []byte(txt), fmt.Sprintf("path/file%d.txt", i))
		if err != nil {
			t.Fatal(err)
		}
		_, err = orch.Ingest(*pkt)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Query
	qr, err := orch.Query("req-12", "alpha beta gamma", "")
	if err != nil {
		t.Fatal(err)
	}
	if qr.CandidatesConsidered != 2 {
		t.Fatalf("candidatesConsidered %d want 2", qr.CandidatesConsidered)
	}
	// Create progress task for query and bind context metrics
	reg := progress.NewRegistry(128)
	task, _ := reg.CreateDeterministic(progress.OperationQuery, wsID, "alpha beta gamma", progress.PhaseRequestAccepted)
	_ = reg.Transition(task.Packet.TaskID, progress.StatusRunning)
	ctxM := ContextMetricsFromOrchestrator(orch)
	_ = reg.Update(task.Packet.TaskID, func(p *progress.ProgressPacket) {
		p.Context = ctxM
		c := int64(len(qr.Selected))
		tot := int64(qr.CandidatesConsidered)
		p.Completed = &c
		p.Total = &tot
	})
	got, _ := reg.Get(task.Packet.TaskID)
	if got.Packet.Context.CandidatesConsidered != qr.CandidatesConsidered {
		t.Fatalf("context candidates mismatch %d vs %d", got.Packet.Context.CandidatesConsidered, qr.CandidatesConsidered)
	}
	if got.Packet.Context.SelectedCount != len(qr.Selected) {
		t.Fatalf("selected count mismatch %d vs %d", got.Packet.Context.SelectedCount, len(qr.Selected))
	}
	if got.Packet.Context.Coverage != qr.Coverage {
		t.Fatalf("coverage mismatch %v vs %v", got.Packet.Context.Coverage, qr.Coverage)
	}
	// Verify via aggregate
	orchs := map[string]*orchestrator.Orchestrator{wsID: orch}
	agg := ContextMetricsAggregate(orchs)
	if agg.CandidatesConsidered != qr.CandidatesConsidered {
		t.Fatalf("aggregate mismatch")
	}
}

func TestCH08_13_EmptyStatusSensible(t *testing.T) {
	// LedgerMetricsOffline on empty dir should give zero counts but valid structure
	dir := t.TempDir()
	m := LedgerMetricsOffline(dir)
	if m.AcceptedEventCount != 0 || m.CurrentSequence != 0 {
		t.Fatalf("empty ledger metrics should be zero got %+v", m)
	}
	// CollectSystemStatus with empty metrics should return sensible defaults
	status := CollectSystemStatus(m, progress.GraphMetrics{}, progress.ActorMetrics{}, progress.ContextMetrics{}, 0)
	if status.EventCount != 0 {
		t.Fatalf("event count not zero")
	}
	if status.Replay != "unknown" {
		t.Fatalf("replay should be unknown for empty got %q", status.Replay)
	}
	if status.Workspaces != 0 {
		t.Fatalf("workspaces not zero")
	}
	// JSON should parse
	b, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("status json not parseable: %v", err)
	}
	// Actor metrics from nil controller should be zero
	am := ActorMetricsFromController(nil)
	if am.Active != 0 || am.Completed != 0 {
		t.Fatalf("nil controller metrics not zero %+v", am)
	}
	// Graph metrics from nil store zero
	gm := GraphMetricsFromStore(nil)
	if gm.NodeCount != 0 {
		t.Fatalf("nil store metrics not zero")
	}
}

func TestCH08_14_LedgerHeadCount(t *testing.T) {
	dir := t.TempDir()
	svc, err := eventlog.New(filepath.Join(dir, "events"), "")
	if err != nil {
		t.Fatal(err)
	}
	// Initially zero
	m := LedgerMetricsFromService(svc, dir)
	if m.AcceptedEventCount != 0 {
		t.Fatalf("initial count not zero %+v", m)
	}
	// Append events via authority (needs writer)
	fw := &fakeWriterObs{}
	// Use svc directly: need to test Append? Use transition authority with svc as writer? svc requires Rust URL for accepted events
	// Use fake writer to simulate, but also test LedgerMetricsFromService with manual events file
	// Instead manually append via svc using non-accepted type (no Rust required)
	for i := 1; i <= 3; i++ {
		ev := eventlog.Event{ID: fmt.Sprintf("ev-%d", i), Type: "test.event", Action: "test", Status: "accepted"}
		saved, err := svc.Append(ev)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if saved.Seq != int64(i) {
			t.Fatalf("seq mismatch %d vs %d", saved.Seq, i)
		}
		_ = fw
	}
	m2 := LedgerMetricsFromService(svc, dir)
	if m2.AcceptedEventCount != 3 {
		t.Fatalf("count %d want 3", m2.AcceptedEventCount)
	}
	if m2.CurrentSequence != 3 {
		t.Fatalf("seq %d want 3", m2.CurrentSequence)
	}
	if m2.HeadEventID != "ev-3" {
		t.Fatalf("head id %q want ev-3", m2.HeadEventID)
	}
	if m2.HeadHash == "" {
		t.Fatal("head hash empty")
	}
	// Offline fallback should also give head/count
	offline := LedgerMetricsOffline(dir)
	if offline.AcceptedEventCount != 3 {
		t.Fatalf("offline count %d want 3", offline.AcceptedEventCount)
	}
	if offline.HeadEventID != "ev-3" {
		t.Fatalf("offline head %q", offline.HeadEventID)
	}
}

func TestCH08_15_VerifyFailureVisible(t *testing.T) {
	dir := t.TempDir()
	svc, err := eventlog.New(filepath.Join(dir, "events"), "")
	if err != nil {
		t.Fatal(err)
	}
	// Append two valid events
	for i := 1; i <= 2; i++ {
		ev := eventlog.Event{ID: fmt.Sprintf("ev-%d", i), Type: "test.event", Action: "test", Status: "accepted"}
		if _, err := svc.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	mOk := LedgerMetricsFromService(svc, dir)
	if mOk.VerificationStatus != "ok" {
		t.Fatalf("verify should be ok got %q", mOk.VerificationStatus)
	}
	// Tamper ledger jsonl: flip last hash
	ledgerPath := filepath.Join(dir, "events", "ledger.jsonl")
	b, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) < 2 {
		t.Fatal("not enough lines")
	}
	// Corrupt second line: change Seq
	var ev2 map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &ev2); err != nil {
		t.Fatal(err)
	}
	ev2["seq"] = float64(999)
	corrupted, _ := json.Marshal(ev2)
	lines[1] = string(corrupted)
	_ = os.WriteFile(ledgerPath, []byte(strings.Join(lines, "\n")+"\n"), 0644)
	// Need new service that loads corrupted file; but Verify will check seq mismatch
	svc2, err := eventlog.New(filepath.Join(dir, "events2"), "")
	if err != nil {
		t.Fatal(err)
	}
	// We need to verify original svc's Verify sees corrupted? svc caches seq, but we can directly call Verify on svc after tamper? It will List and Verify freshly.
	// Instead attach new service to same path via manual List reading? Simpler directly call svc.Verify which reads ledger.jsonl each time
	v, err := svc.Verify()
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if ok, _ := v["ok"].(bool); ok {
		t.Fatalf("tampered ledger should fail verification, got ok")
	}
	if v["reason"] == nil {
		t.Fatalf("reason missing on failure")
	}
	mFail := LedgerMetricsFromService(svc, dir)
	if mFail.VerificationStatus != "failed" {
		t.Fatalf("metrics verification status should be failed got %q", mFail.VerificationStatus)
	}
	if mFail.ReplayStatus != "corrupt" {
		t.Fatalf("replay status should be corrupt got %q", mFail.ReplayStatus)
	}
	_ = svc2
}

func TestCH08_16_ReplayStatusVisible(t *testing.T) {
	dir := t.TempDir()
	svc, err := eventlog.New(filepath.Join(dir, "events"), "")
	if err != nil {
		t.Fatal(err)
	}
	ev := eventlog.Event{ID: "ev-1", Type: "test.event", Action: "test", Status: "accepted"}
	if _, err := svc.Append(ev); err != nil {
		t.Fatal(err)
	}
	m := LedgerMetricsFromService(svc, dir)
	if m.ReplayStatus == "" {
		t.Fatal("replay status empty")
	}
	if m.ReplayStatus != "ok" {
		t.Fatalf("replay status want ok got %q", m.ReplayStatus)
	}
	status := CollectSystemStatus(m, progress.GraphMetrics{}, progress.ActorMetrics{}, progress.ContextMetrics{}, 1)
	if status.Replay != m.ReplayStatus {
		t.Fatalf("collect replay mismatch %q vs %q", status.Replay, m.ReplayStatus)
	}
	// JSON should contain replay_status
	b, _ := json.Marshal(status)
	var raw map[string]any
	_ = json.Unmarshal(b, &raw)
	if raw["replay_status"] == nil {
		t.Fatal("json missing replay_status")
	}
}

func TestCH08_BoundedMetrics(t *testing.T) {
	// Ensure ledger metrics respects checkpoint bounded info
	dir := t.TempDir()
	// No checkpoints initially
	m := LedgerMetricsFromService(nil, dir)
	if m.CheckpointPresent {
		t.Fatal("unexpected checkpoint")
	}
	// Create checkpoint dir and file
	cpDir := filepath.Join(dir, "checkpoints")
	_ = os.MkdirAll(cpDir, 0755)
	_ = os.WriteFile(filepath.Join(cpDir, "checkpoint-000001-test.json"), []byte(`{"seq":1}`), 0644)
	time.Sleep(10 * time.Millisecond)
	svc, _ := eventlog.New(filepath.Join(dir, "events"), "")
	m2 := LedgerMetricsFromService(svc, dir)
	if !m2.CheckpointPresent {
		t.Fatal("checkpoint should be present")
	}
	if m2.CheckpointSequence == nil || *m2.CheckpointSequence != 1 {
		t.Fatalf("checkpoint seq %+v", m2.CheckpointSequence)
	}
}

func TestCH08_SystemStatusDeterministicJSON(t *testing.T) {
	m := progress.LedgerMetrics{AcceptedEventCount: 5, CurrentSequence: 5, HeadEventID: "ev-5", HeadHash: strings.Repeat("b", 64), VerificationStatus: "ok", ReplayStatus: "ok"}
	status := CollectSystemStatus(m, progress.GraphMetrics{NodeCount: 10}, progress.ActorMetrics{Active: 1}, progress.ContextMetrics{CandidatesConsidered: 3}, 2)
	b1, _ := json.Marshal(status)
	b2, _ := json.Marshal(status)
	if string(b1) != string(b2) {
		t.Fatal("not deterministic")
	}
	if !json.Valid(b1) {
		t.Fatal("not valid json")
	}
}
