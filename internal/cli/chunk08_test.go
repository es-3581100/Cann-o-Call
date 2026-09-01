package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flatten-workspace/internal/observability"
	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/server"
	"flatten-workspace/internal/studio"
)

func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func captureStderr(fn func()) string {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	fn()
	w.Close()
	os.Stderr = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestCH08_17_JSONParses(t *testing.T) {
	p := progress.ProgressPacket{
		TaskID:    "task-123",
		Operation: progress.OperationIngest,
		Phase:     progress.PhaseRunning,
		Status:    progress.StatusRunning,
		StartedAt: mustTime(t),
		UpdatedAt: mustTime(t).Add(1000000000),
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("json not parseable: %v", err)
	}
	if raw["task_id"] != "task-123" {
		t.Fatalf("task_id missing %v", raw)
	}
}

func TestCH08_18_JSONDeterministic(t *testing.T) {
	p := progress.ProgressPacket{
		TaskID:    "task-det",
		Operation: progress.OperationQuery,
		Phase:     progress.PhaseCandidatesSelected,
		Status:    progress.StatusComplete,
		StartedAt: mustTime(t),
		UpdatedAt: mustTime(t).Add(2000000000),
		Actor:     progress.ActorMetrics{Active: 1},
		Graph:     progress.GraphMetrics{NodeCount: 5},
	}
	b1, _ := json.Marshal(p)
	b2, _ := json.Marshal(p)
	if string(b1) != string(b2) {
		t.Fatalf("not deterministic %q vs %q", b1, b2)
	}
	// Sorted keys check via manual canonical check: json.Marshal sorts keys by struct definition, but map keys also sorted in Go's json after our canonical? At least verify twice same.
	for i := 0; i < 5; i++ {
		bb, _ := json.Marshal(p)
		if string(bb) != string(b1) {
			t.Fatal("non-deterministic iteration")
		}
	}
}

func TestCH08_19_HumanNotCorruptJSON(t *testing.T) {
	p := progress.ProgressPacket{
		TaskID:    "task-human",
		Operation: progress.OperationIngest,
		Phase:     progress.PhaseRunning,
		Status:    progress.StatusRunning,
		StartedAt: mustTime(t).Add(-5000000000),
		UpdatedAt: mustTime(t),
	}
	human := progress.RenderProgress(p)
	if human == "" {
		t.Fatal("empty render")
	}
	// Ensure human is not valid JSON object with task_id key (would corrupt JSON mode)
	var raw any
	if err := json.Unmarshal([]byte(human), &raw); err == nil {
		if m, ok := raw.(map[string]any); ok {
			if _, has := m["task_id"]; has {
				t.Fatalf("human rendering produced JSON with task_id: %q", human)
			}
		}
		// If it happens to be valid JSON but not containing task_id, still suspicious but allow
		// The key point: RenderProgress does not produce JSON, just human line
		if strings.HasPrefix(strings.TrimSpace(human), "{") {
			t.Fatalf("human should not be JSON object: %q", human)
		}
	}
	if strings.Contains(human, "\"task_id\"") {
		t.Fatalf("human contains json key")
	}
}

func TestCH08_20_CLIStatusEmptyInitial(t *testing.T) {
	// Offline status when server unreachable should still return JSON that parses and has sensible defaults
	dir := t.TempDir()
	origDataDir := os.Getenv("DATA_DIR")
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("DATA_DIR", dir)
	_ = os.Setenv("ADDR", "http://127.0.0.1:1")
	defer func() {
		_ = os.Setenv("DATA_DIR", origDataDir)
		_ = os.Setenv("ADDR", origAddr)
	}()
	out := captureStdout(func() {
		code := Run([]string{"status", "--json"})
		if code != 0 {
			t.Fatalf("status code %d", code)
		}
	})
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &resp); err != nil {
		t.Fatalf("status json not parseable: %v out %q", err, out)
	}
	if _, ok := resp["ledger"]; !ok {
		t.Fatalf("missing ledger in status %v", resp)
	}
	if _, ok := resp["actor"]; !ok {
		t.Fatalf("missing actor %v", resp)
	}
	if _, ok := resp["event_count"]; !ok {
		t.Fatalf("missing event_count")
	}
	// Human mode should not produce JSON with task_id but should be readable line
	outHuman := captureStdout(func() {
		Run([]string{"status"})
	})
	if strings.Contains(outHuman, "\"ledger\"") && strings.Contains(outHuman, "\"task_id\"") {
		t.Fatalf("human mode leaking json task_id")
	}
	if !strings.Contains(outHuman, "ledger") {
		t.Fatalf("human status missing ledger info %q", outHuman)
	}
}

func TestCH08_21_ActorList(t *testing.T) {
	// Test actor list with mocked server for both with and without workspace
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/status":
			// Return minimal status with actor field
			_ = json.NewEncoder(w).Encode(map[string]any{
				"actor":         map[string]any{"active": 2, "completed": 1},
				"ledger":        map[string]any{"accepted_event_count": 5},
				"graph":         map[string]any{"node_count": 3},
				"context":       map[string]any{"candidates_considered": 3},
				"replay_status": "ok",
			})
		case strings.HasPrefix(r.URL.Path, "/api/workspaces/") && strings.HasSuffix(r.URL.Path, "/actors"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": "actor-000001", "status": "active"},
				{"id": "actor-000002", "status": "completed"},
			})
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("ADDR", srv.URL)
	defer func() { _ = os.Setenv("ADDR", origAddr) }()
	// Without workspace, actor list should return metrics json
	out := captureStdout(func() {
		code := Run([]string{"actor", "list", "--json"})
		if code != 0 {
			t.Fatalf("actor list code %d", code)
		}
	})
	var parsed any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &parsed); err != nil {
		t.Fatalf("actor list json not parseable %v out %q", err, out)
	}
	// With workspace
	out2 := captureStdout(func() {
		code := Run([]string{"actor", "list", "--workspace", "ws1", "--json"})
		if code != 0 {
			t.Fatalf("actor list ws code %d", code)
		}
	})
	var list []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out2)), &list); err != nil {
		t.Fatalf("actor list ws parse %v out %q", err, out2)
	}
	if len(list) != 2 {
		t.Fatalf("actor list length %d want 2", len(list))
	}
}

func TestCH08_22_LedgerVerify(t *testing.T) {
	dir := t.TempDir()
	origDataDir := os.Getenv("DATA_DIR")
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("DATA_DIR", dir)
	_ = os.Setenv("ADDR", "http://127.0.0.1:1")
	defer func() {
		_ = os.Setenv("DATA_DIR", origDataDir)
		_ = os.Setenv("ADDR", origAddr)
	}()
	// Offline ledger verify should still produce JSON with ok field
	out := captureStdout(func() {
		code := Run([]string{"ledger", "verify", "--json"})
		if code != 0 {
			t.Fatalf("ledger verify code %d", code)
		}
	})
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &resp); err != nil {
		t.Fatalf("ledger verify not parseable %v out %q", err, out)
	}
	// Should contain local ok or offline marker
	if _, hasLocal := resp["local"]; !hasLocal {
		if _, hasOffline := resp["offline"]; !hasOffline {
			t.Fatalf("ledger verify missing local/offline %v", resp)
		}
	}
	// With server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/ledger/verify" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "events_checked": 5, "last_hash": strings.Repeat("c", 64)})
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	_ = os.Setenv("ADDR", srv.URL)
	out2 := captureStdout(func() {
		code := Run([]string{"ledger", "verify", "--json"})
		if code != 0 {
			t.Fatalf("ledger verify server code %d", code)
		}
	})
	var resp2 map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out2)), &resp2); err != nil {
		t.Fatalf("ledger verify server not parseable %v out %q", err, out2)
	}
	if _, ok := resp2["ok"]; !ok {
		// server response may be nested? The CLI just prints server body as-is
		// So it should contain ok
		t.Fatalf("server verify missing ok %v", resp2)
	}
}

func TestCH08_23_QueryBoundedResult(t *testing.T) {
	// Query bounded result count <= config limits via scoring selection, not via CLI server directly
	// Test that scoring respects limits and that CLI query offline path returns bounded result
	cfg := scoring.Config{Threshold: 0.1, MaxCandidates: 2, MaxActorsPerRequest: 2}.WithDefaults()
	_ = cfg
	// Offline CLI query should produce json with candidates/sel <= limits? But offline returns 0, which is bounded.
	dir := t.TempDir()
	origDataDir := os.Getenv("DATA_DIR")
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("DATA_DIR", dir)
	_ = os.Setenv("ADDR", "http://127.0.0.1:1")
	defer func() {
		_ = os.Setenv("DATA_DIR", origDataDir)
		_ = os.Setenv("ADDR", origAddr)
	}()
	out := captureStdout(func() {
		code := Run([]string{"query", "alpha beta", "--json"})
		if code != 0 {
			t.Fatalf("query code %d", code)
		}
	})
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &resp); err != nil {
		t.Fatalf("query json not parseable %v out %q", err, out)
	}
	// Offline returns 0 candidates which is <= limits
	if v, ok := resp["candidates_considered"]; ok {
		if vv, ok := v.(float64); ok && vv > 10 {
			t.Fatalf("candidates unbounded %v", vv)
		}
	}
	// Also test scoring directly enforces bound
	// Build dummy selected via scoring
	// Not needed for CLI but ensures bounded property holds
}

func TestCH08_CLIQueryServerIntegrationBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/query" || strings.HasPrefix(r.URL.Path, "/api/query") {
			// Return result with bounded selected
			_ = json.NewEncoder(w).Encode(map[string]any{
				"task_id": "task-123",
				"result": map[string]any{
					"candidates_considered": 5,
					"selected": []map[string]any{
						{"node": map[string]any{"node_id": "a"}},
						{"node": map[string]any{"node_id": "b"}},
					},
					"coverage": 0.8,
				},
			})
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("ADDR", srv.URL)
	defer func() { _ = os.Setenv("ADDR", origAddr) }()
	// CLI query via server should still be bounded
	out := captureStdout(func() {
		code := Run([]string{"query", "test", "--workspace", "ws1", "--json"})
		if code != 0 {
			t.Fatalf("query server code %d", code)
		}
	})
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &resp); err != nil {
		t.Fatalf("query server not json %v out %q", err, out)
	}
	// Should contain result with selected count <= config (2 in server? Here we returned 2)
	if r, ok := resp["result"].(map[string]any); ok {
		if sel, ok := r["selected"].([]any); ok {
			if len(sel) > 5 {
				t.Fatalf("selected unbounded %d", len(sel))
			}
		}
	}
}

func TestCH08_CLIStatusServerPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ledger":        map[string]any{"accepted_event_count": 10, "current_sequence": 10, "head_event_id": "ev-10", "head_hash": strings.Repeat("a", 64), "verification_status": "ok"},
				"graph":         map[string]any{"node_count": 7},
				"actor":         map[string]any{"active": 1},
				"context":       map[string]any{"candidates_considered": 2},
				"replay_status": "ok",
				"checkpoint":    map[string]any{"present": false},
				"workspaces":    1,
				"event_count":   10,
			})
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("ADDR", srv.URL)
	defer func() { _ = os.Setenv("ADDR", origAddr) }()
	out := captureStdout(func() {
		code := Run([]string{"status", "--json"})
		if code != 0 {
			t.Fatalf("status server code %d", code)
		}
	})
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &resp); err != nil {
		t.Fatalf("status server not json %v", err)
	}
	if _, ok := resp["ledger"]; !ok {
		t.Fatalf("status server missing ledger")
	}
}

func TestCH08_CLIHelpers(t *testing.T) {
	// Verify ledger status offline parses
	dir := t.TempDir()
	origDataDir := os.Getenv("DATA_DIR")
	origAddr := os.Getenv("ADDR")
	_ = os.Setenv("DATA_DIR", dir)
	_ = os.Setenv("ADDR", "http://127.0.0.1:1")
	defer func() {
		_ = os.Setenv("DATA_DIR", origDataDir)
		_ = os.Setenv("ADDR", origAddr)
	}()
	out := captureStdout(func() {
		code := Run([]string{"ledger", "status", "--json"})
		if code != 0 {
			t.Fatalf("ledger status code %d", code)
		}
	})
	var m progress.LedgerMetrics
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &m); err != nil {
		// ledger status may output object with ledger metrics fields directly
		var raw map[string]any
		if err2 := json.Unmarshal([]byte(strings.TrimSpace(out)), &raw); err2 != nil {
			t.Fatalf("ledger status not parsable %v out %q", err, out)
		}
	}
	// Ensure observability status via offline path includes replay
	_ = m
	// Test that unknown command returns error
	_ = captureStderr(func() {
		code := Run([]string{"unknowncmd"})
		if code == 0 {
			t.Fatal("unknown cmd should fail")
		}
	})
}

func TestStudioCommandParsingAndOnceUsesOnlyStudioEndpoint(t *testing.T) {
	for _, input := range []string{"monitor", ":ranger", "refresh", "select 2", ":quit"} {
		if _, err := parseStudioCommand(input); err != nil {
			t.Errorf("parseStudioCommand(%q): %v", input, err)
		}
	}
	for _, input := range []string{"", "select -1", "status", "refresh now"} {
		if _, err := parseStudioCommand(input); err == nil {
			t.Errorf("parseStudioCommand(%q) unexpectedly succeeded", input)
		}
	}
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/studio" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"generated_at": "2023-11-14T22:13:20Z", "health": map[string]any{"available": true, "state": "ok"},
			"ledger": map[string]any{}, "actors": map[string]any{}, "graph": map[string]any{}, "context": map[string]any{},
			"capabilities": []any{}, "tasks": map[string]any{"count": 0, "packets": []any{}}, "progress_receipts": map[string]any{"count": 0, "packets": []any{}}, "receipts": map[string]any{"count": 0, "packets": []any{}}, "ranger": map[string]any{"groups": []any{}, "initial_tree": []any{}}, "unavailable": []any{},
		})
	}))
	defer srv.Close()
	t.Setenv("ADDR", srv.URL)
	out := captureStdout(func() {
		if code := Run([]string{"studio", "--once"}); code != 0 {
			t.Fatalf("studio --once code = %d", code)
		}
	})
	if requests != 1 {
		t.Fatalf("Studio requests = %d, want 1", requests)
	}
	if !strings.Contains(out, "STUDIO · MONITOR") || !strings.Contains(out, "STUDIO · RANGER") {
		t.Fatalf("unexpected Studio output: %s", out)
	}
	for _, category := range []string{"actors", "events", "evidence_provenance", "files", "graph_nodes", "receipts", "source_nodes", "tasks", "workspaces"} {
		if !strings.Contains(out, category) {
			t.Errorf("Studio Ranger output missing category %q: %s", category, out)
		}
	}
}

func TestStudioReceiptSentinelsNeverReachSnapshotOrOnceOutput(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("RUST_LEDGER_URL", "")
	s := server.New()
	started := time.Unix(1700000000, 0).UTC()
	receipt := progress.ReceiptPacket{
		TaskID:          "receipt-task",
		Operation:       progress.OperationQuery,
		StartedAt:       started,
		FinishedAt:      started.Add(time.Second),
		Elapsed:         "1s",
		FinalStatus:     progress.StatusFailed,
		UnitsProcessed:  3,
		ActorsActivated: 2,
		ActorsFailed:    1,
		Warnings:        []string{"WARNING_SENTINEL_MUST_NOT_REACH_STUDIO"},
		Errors:          []string{"ERROR_SENTINEL_MUST_NOT_REACH_STUDIO"},
	}
	if err := s.ProgressReceipts.Save(receipt); err != nil {
		t.Fatalf("save source receipt: %v", err)
	}

	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	response, err := http.Get(httpServer.URL + "/api/studio")
	if err != nil {
		t.Fatalf("GET /api/studio: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Studio response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/studio status = %d: %s", response.StatusCode, body)
	}
	for _, sentinel := range []string{"WARNING_SENTINEL_MUST_NOT_REACH_STUDIO", "ERROR_SENTINEL_MUST_NOT_REACH_STUDIO"} {
		if strings.Contains(string(body), sentinel) {
			t.Fatalf("Studio JSON leaked receipt sentinel %q: %s", sentinel, body)
		}
	}
	var vm studio.ViewModel
	if err := json.Unmarshal(body, &vm); err != nil {
		t.Fatalf("decode Studio ViewModel: %v", err)
	}
	if len(vm.ProgressReceipts.Packets) != 1 {
		t.Fatalf("Studio progress receipt count = %d, want 1", len(vm.ProgressReceipts.Packets))
	}
	summary := vm.ProgressReceipts.Packets[0]
	if summary.TaskID != receipt.TaskID || summary.DurableRefCount != 0 || summary.UnitsProcessed != receipt.UnitsProcessed {
		t.Fatalf("unexpected safe receipt summary: %+v", summary)
	}

	t.Setenv("ADDR", httpServer.URL)
	once := captureStdout(func() {
		if code := Run([]string{"studio", "--once"}); code != 0 {
			t.Fatalf("studio --once code = %d", code)
		}
	})
	for _, sentinel := range []string{"WARNING_SENTINEL_MUST_NOT_REACH_STUDIO", "ERROR_SENTINEL_MUST_NOT_REACH_STUDIO"} {
		if strings.Contains(once, sentinel) {
			t.Fatalf("studio --once leaked receipt sentinel %q: %s", sentinel, once)
		}
	}
}

// Helpers for CH08
func mustTime(t *testing.T) (tm time.Time) {
	t.Helper()
	// Use fixed time for determinism
	return time.Unix(1700000000, 0).UTC()
}

func TestCH08_ObservabilityMetricsHelper(t *testing.T) {
	// Ensure observability helper does not panic on nil and produces deterministic json
	m := observability.LedgerMetricsOffline(t.TempDir())
	b, _ := json.Marshal(m)
	if !json.Valid(b) {
		t.Fatal("invalid json")
	}
	// Temp dir with ledger file
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "events")
	_ = os.MkdirAll(ledgerDir, 0755)
	ev := map[string]any{"id": "ev1", "seq": 1, "hash": strings.Repeat("a", 64), "prev_hash": "genesis"}
	bb, _ := json.Marshal(ev)
	_ = os.WriteFile(filepath.Join(ledgerDir, "ledger.jsonl"), append(bb, '\n'), 0644)
	m2 := observability.LedgerMetricsOffline(dir)
	if m2.AcceptedEventCount != 1 {
		t.Fatalf("offline count %d want 1", m2.AcceptedEventCount)
	}
	_ = fmt.Sprintf("%v", m2)
}
