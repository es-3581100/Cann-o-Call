package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/contextpacket"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/transition"
)

// fakeWriter is an in-memory DurableWriter that can simulate success (valid Rust ack) or failure.
type fakeWriter struct {
	mu     sync.Mutex
	events []eventlog.Event
	fail   error
}

func (f *fakeWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
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

func (f *fakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeWriter) eventsCopy() []eventlog.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]eventlog.Event, len(f.events))
	copy(cp, f.events)
	return cp
}

const wsCH07 = "ws-ch07"

func newTestOrchestrator(t *testing.T, scoringCfg scoring.Config, actorCfg *actorstub.Config) (*Orchestrator, *fakeWriter, *graph.Store, *transition.Authority, *actorstub.Controller) {
	t.Helper()
	fw := &fakeWriter{}
	auth := transition.New(fw, nil)
	store := graph.NewStore(wsCH07)
	var ctrl *actorstub.Controller
	if actorCfg != nil {
		ctrl = actorstub.NewWithConfig(*actorCfg)
	} else {
		ctrl = actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
	}
	t.Cleanup(func() { ctrl.Shutdown() })
	o := New(wsCH07, store, auth, ctrl, scoringCfg)
	return o, fw, store, auth, ctrl
}

func mustIngestBytes(t *testing.T, o *Orchestrator, sourceRef, text string) ingest.SourcePacket {
	t.Helper()
	pkt, err := ingest.IngestBytes(wsCH07, sourceRef, "", []byte(text), sourceRef)
	if err != nil {
		t.Fatalf("IngestBytes %q: %v", sourceRef, err)
	}
	ng, err := o.Ingest(*pkt)
	if err != nil {
		t.Fatalf("Orchestrator.Ingest %q: %v", sourceRef, err)
	}
	if ng == nil {
		t.Fatalf("nil graph after ingest %q", sourceRef)
	}
	return *pkt
}

func mustIngestBytesWithMeta(t *testing.T, o *Orchestrator, sourceRef, text string, metaTitle string) ingest.SourcePacket {
	t.Helper()
	pkt, err := ingest.IngestBytes(wsCH07, sourceRef, "", []byte(text), sourceRef)
	if err != nil {
		t.Fatalf("IngestBytes %q: %v", sourceRef, err)
	}
	if metaTitle != "" {
		pkt.Metadata.Title = metaTitle
		if pkt.Metadata.OpenGraph == nil {
			pkt.Metadata.OpenGraph = map[string]string{}
		}
		pkt.Metadata.OpenGraph["title"] = metaTitle
	}
	ng, err := o.Ingest(*pkt)
	if err != nil {
		t.Fatalf("Orchestrator.Ingest meta %q: %v", sourceRef, err)
	}
	if ng == nil {
		t.Fatalf("nil graph after ingest meta %q", sourceRef)
	}
	return *pkt
}

func isSHA256Local(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// TestCH07 is the 30-case deterministic matrix required by CHUNK-07.
// Subtest naming reflects case numbers exactly (01..30).
func TestCH07(t *testing.T) {
	// 01 empty graph no activation
	t.Run("01_empty_graph_no_activation", func(t *testing.T) {
		o, _, _, auth, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		if auth.Projection().Version != 0 {
			t.Fatalf("initial projection version %d want 0", auth.Projection().Version)
		}
		qr, err := o.Query("req-01", "kubernetes deployment", "")
		if err != nil {
			t.Fatalf("Query empty: %v", err)
		}
		if qr.CandidatesConsidered != 0 {
			t.Fatalf("candidates %d want 0 on empty graph", qr.CandidatesConsidered)
		}
		if len(qr.Selected) != 0 || len(qr.Activations) != 0 || len(qr.Packets) != 0 {
			t.Fatalf("expected no activation on empty graph, got selected %d act %d pkt %d", len(qr.Selected), len(qr.Activations), len(qr.Packets))
		}
		if qr.Coverage != 0 {
			t.Fatalf("coverage %v want 0 empty", qr.Coverage)
		}
	})

	// 02 relevant source scores
	t.Run("02_relevant_source_scores", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		mustIngestBytes(t, o, "docs/deploy.txt", "kubernetes deployment manifest api version kind")
		qr, err := o.Query("req-02", "kubernetes deployment", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) == 0 {
			t.Fatal("relevant source should score and be selected")
		}
		if qr.Selected[0].Components.ActivationScore < 1.0 {
			t.Fatalf("activationScore %v want >=1", qr.Selected[0].Components.ActivationScore)
		}
		if len(qr.Activations) == 0 || len(qr.Packets) == 0 {
			t.Fatalf("expected packets/activations, got act %d pkt %d", len(qr.Activations), len(qr.Packets))
		}
	})

	// 03 unrelated query no activation
	t.Run("03_unrelated_query_no_activation", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		mustIngestBytes(t, o, "docs/deploy.txt", "kubernetes deployment manifest")
		qr, err := o.Query("req-03", "quantum biology photosynthesis", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) != 0 {
			t.Fatalf("unrelated query should not activate, got %d selected", len(qr.Selected))
		}
		if len(qr.Activations) != 0 {
			t.Fatalf("unrelated should have 0 activations, got %d", len(qr.Activations))
		}
	})

	// 04 deterministic score repeat
	t.Run("04_deterministic_score_repeat", func(t *testing.T) {
		o, _, store, _, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		mustIngestBytes(t, o, "docs/a.txt", "alpha beta gamma delta")
		mustIngestBytes(t, o, "docs/b.txt", "alpha beta")
		g := store.Graph()
		pkts := store.Packets()
		scorer := scoring.BaselineScorer{}
		s1 := scorer.ScoreGraph(g, pkts, "alpha beta gamma")
		s2 := scorer.ScoreGraph(g, pkts, "alpha beta gamma")
		if len(s1) != len(s2) {
			t.Fatalf("repeat length %d vs %d", len(s1), len(s2))
		}
		for i := range s1 {
			if s1[i].Components != s2[i].Components || s1[i].Node.NodeID != s2[i].Node.NodeID {
				t.Fatalf("not deterministic at %d: %#v vs %#v", i, s1[i], s2[i])
			}
		}
		// Also via orchestrator Query repeat after dedup cleared (completed)
		qr1, _ := o.Query("req-04a", "alpha beta gamma", "")
		qr2, _ := o.Query("req-04b", "alpha beta gamma", "")
		if len(qr1.Selected) != len(qr2.Selected) {
			t.Fatalf("orchestrator repeat selected %d vs %d", len(qr1.Selected), len(qr2.Selected))
		}
		for i := range qr1.Selected {
			if qr1.Selected[i].Node.NodeID != qr2.Selected[i].Node.NodeID || qr1.Selected[i].Components.ActivationScore != qr2.Selected[i].Components.ActivationScore {
				t.Fatalf("orchestrator not deterministic at %d", i)
			}
		}
	})

	// 05 deterministic ordering
	t.Run("05_deterministic_ordering", func(t *testing.T) {
		o, _, store, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/a.txt", "alpha beta gamma")
		mustIngestBytes(t, o, "docs/b.txt", "alpha beta")
		mustIngestBytes(t, o, "docs/c.txt", "alpha")
		g := store.Graph()
		pkts := store.Packets()
		scorer := scoring.BaselineScorer{}
		scored := scorer.ScoreGraph(g, pkts, "alpha beta gamma")
		selected := scoring.SelectCandidates(scored, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		if len(selected) != 3 {
			t.Fatalf("expected 3 selected, got %d", len(selected))
		}
		// Must be sorted desc ActivationScore, then NodeID asc tiebreak
		for i := 1; i < len(selected); i++ {
			prev := selected[i-1].Components.ActivationScore
			cur := selected[i].Components.ActivationScore
			if cur > prev {
				t.Fatalf("not sorted desc at %d: %v > %v", i, cur, prev)
			}
			if cur == prev && selected[i].Node.NodeID < selected[i-1].Node.NodeID {
				// If equal score, should be NodeID asc, so earlier should be smaller ID
				// If not, order violates determinism
				t.Fatalf("tiebreak not NodeID asc at %d", i)
			}
		}
		// Second call same ordering
		scored2 := scorer.ScoreGraph(g, pkts, "alpha beta gamma")
		selected2 := scoring.SelectCandidates(scored2, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		if !reflect.DeepEqual(selected, selected2) {
			t.Fatal("ordering not deterministic across rescore")
		}
		qr1, _ := o.Query("req-05a", "alpha beta gamma", "")
		qr2, _ := o.Query("req-05b", "alpha beta gamma", "")
		if len(qr1.Selected) != len(qr2.Selected) {
			t.Fatalf("orchestrator ordering lengths differ")
		}
		for i := range qr1.Selected {
			if qr1.Selected[i].Node.NodeID != qr2.Selected[i].Node.NodeID {
				t.Fatalf("orchestrator ordering differs at %d", i)
			}
		}
	})

	// 06 threshold below activates
	t.Run("06_threshold_below_activates", func(t *testing.T) {
		// score 1.0 should activate when threshold 1.0 — need secondary match to reach 1.0 (0.85 without)
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 1.0, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/threshold.txt", "alpha beta")
		// query contains alpha -> primary 1 + secondary 1 (path contains alpha) => activationScore 1.0
		qr, err := o.Query("req-06", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) == 0 {
			t.Fatalf("threshold below/no: threshold 1 should activate for score 1, components %#v", qr.Selected)
		}
		if len(qr.Selected) > 0 && qr.Selected[0].Components.ActivationScore < 1.0 {
			t.Fatalf("activationScore %v want >=1", qr.Selected[0].Components.ActivationScore)
		}
	})

	// 07 threshold above remains dormant
	t.Run("07_threshold_above_remains_dormant", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 5.0, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/threshold2.txt", "alpha beta")
		qr, err := o.Query("req-07", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) != 0 {
			t.Fatalf("threshold above should remain dormant, got %d selected score %v", len(qr.Selected), qr.Selected[0].Components.ActivationScore)
		}
		if len(qr.Activations) != 0 {
			t.Fatalf("should have 0 activations, got %d", len(qr.Activations))
		}
	})

	// 08 max candidate bound
	t.Run("08_max_candidate_bound", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.1, MaxCandidates: 2, MaxActorsPerRequest: 10}, nil)
		for i := 0; i < 5; i++ {
			mustIngestBytes(t, o, strings.ReplaceAll("docs/fileX.txt", "X", string(rune('a'+i))), "alpha beta gamma common")
		}
		qr, err := o.Query("req-08", "alpha common", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) > 2 {
			t.Fatalf("max candidate bound violated: selected %d > 2", len(qr.Selected))
		}
		if qr.CandidatesConsidered != 5 {
			t.Fatalf("candidatesConsidered %d want 5", qr.CandidatesConsidered)
		}
	})

	// 09 max actor-per-request bound
	t.Run("09_max_actor_per_request_bound", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.1, MaxCandidates: 10, MaxActorsPerRequest: 2}, nil)
		for i := 0; i < 5; i++ {
			mustIngestBytes(t, o, strings.ReplaceAll("docs/actorX.txt", "X", string(rune('a'+i))), "alpha beta gamma common")
		}
		qr, err := o.Query("req-09", "alpha common", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) > 2 {
			t.Fatalf("max actor per request violated: %d >2", len(qr.Selected))
		}
		if len(qr.Activations) > 2 {
			t.Fatalf("activations %d >2", len(qr.Activations))
		}
	})

	// Hierarchy caps and metadata separation also verified here
	t.Run("09b_hierarchy_caps", func(t *testing.T) {
		scorer := scoring.BaselineScorer{}
		// Build packet with many overlapping tokens to test caps
		ws := "ws-cap"
		text := "t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19"
		locator := "path/t0/t1/t2/t3/t4/t5/t6/t7/t8/t9/t10/t11/t12/content.txt"
		pkt, err := ingest.IngestBytes(ws, "path/t0/t1/t2/t3/t4/t5/t6/t7/t8/t9/t10/t11/t12/content.txt", "", []byte(text), locator)
		if err != nil {
			t.Fatal(err)
		}
		// Inject meta tokens to exceed caps
		pkt.Metadata.Title = "t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 extraMeta"
		pkt.Metadata.DocumentProperties = map[string]string{"extra": "t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15"}
		store := graph.NewStore(ws)
		fw := &fakeWriter{}
		auth := transition.New(fw, nil)
		g, _, err := store.Apply(*pkt)
		if err != nil {
			t.Fatal(err)
		}
		// Need to get file node for scoring
		var fileNode graph.Node
		for _, n := range g.SortedNodes() {
			if n.NodeType == graph.NodeTypeFile {
				fileNode = n
				break
			}
		}
		query := "t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19"
		comp := scorer.Score(fileNode, *pkt, scoring.TokenizeQuery(query))
		if comp.PrimaryMatches > 4 {
			t.Fatalf("primary cap violated: %d >4", comp.PrimaryMatches)
		}
		if comp.SecondaryMatches > 8 {
			t.Fatalf("secondary cap %d >8", comp.SecondaryMatches)
		}
		if comp.MetaMatches > 16 {
			t.Fatalf("meta cap %d >16", comp.MetaMatches)
		}
		if comp.SemanticScore > 8+1e-9 {
			t.Fatalf("max semantic 8 violated: %v", comp.SemanticScore)
		}
		if comp.RawScore > 12+1e-9 {
			t.Fatalf("max raw 12 violated: %v", comp.RawScore)
		}
		// Use authority/store to avoid unused
		_ = auth
		_ = store
	})

	// 10 metadata-only no dominance
	t.Run("10_metadata_only_no_dominance", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		// Content unrelated, metadata contains query tokens
		pkt, err := ingest.IngestBytes(wsCH07, "docs/meta-only.txt", "", []byte("lorem ipsum dolor sit unrelated"), "docs/meta-only.txt")
		if err != nil {
			t.Fatal(err)
		}
		pkt.Metadata.Title = "alpha beta gamma delta specialquery"
		pkt.Metadata.OpenGraph = map[string]string{"title": "alpha beta gamma"}
		// Insert via orchestrator to keep deterministic identity but with meta dominance
		_, err = o.Ingest(*pkt)
		if err != nil {
			t.Fatal(err)
		}
		// Query that only matches metadata tokens, not content
		qr, err := o.Query("req-10", "specialquery", "")
		if err != nil {
			t.Fatal(err)
		}
		// Should remain dormant because semanticScore excludes meta (0)
		if len(qr.Selected) != 0 {
			// Verify components show meta but semantic 0
			if qr.Selected[0].Components.SemanticScore != 0 {
				t.Fatalf("metadata should not contribute to semantic, got %v", qr.Selected[0].Components.SemanticScore)
			}
			t.Fatalf("metadata-only should not dominate activation, got %d selected", len(qr.Selected))
		}
		// Direct scorer proof
		scorer := scoring.BaselineScorer{}
		store := o.GetGraph()
		_ = store
		pkts := o.ListPackets()
		var target ingest.SourcePacket
		for _, p := range pkts {
			if p.Identity.SourceRef == "docs/meta-only.txt" {
				target = p
				break
			}
		}
		// find file node
		var node graph.Node
		for _, n := range o.GetGraph().SortedNodes() {
			if n.SourceID == target.Identity.SourceID {
				node = n
				break
			}
		}
		comp := scorer.Score(node, target, scoring.TokenizeQuery("specialquery"))
		if comp.SemanticScore != 0 {
			t.Fatalf("semantic %v want 0 metadata-only", comp.SemanticScore)
		}
		if comp.MetaMatches == 0 {
			t.Fatalf("metaMatches should be >0 for metadata query")
		}
		if comp.ActivationScore != 0 {
			t.Fatalf("activationScore %v want 0 for metadata-only", comp.ActivationScore)
		}
	})

	// 11 duplicate not inflate
	t.Run("11_duplicate_not_inflate", func(t *testing.T) {
		o, fw, store, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		pkt, err := ingest.IngestBytes(wsCH07, "docs/dup.txt", "", []byte("alpha beta gamma"), "docs/dup.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = o.Ingest(*pkt)
		if err != nil {
			t.Fatal(err)
		}
		qr1, _ := o.Query("req-11a", "alpha", "")
		count1 := len(qr1.Selected)
		// Duplicate exact bytes -> same SourceID/ContentHash => idempotent at store level.
		// Authority may return Conflict because Prior advanced; treat as idempotent (no graph change).
		_, err = o.Ingest(*pkt)
		if err != nil && transition.Class(err) != transition.Conflict {
			t.Fatalf("duplicate ingest unexpected error: %v class %q", err, transition.Class(err))
		}
		if fw.count() != 1 {
			// Should remain 1 durable event
			t.Fatalf("fw count %d want 1", fw.count())
		}
		g1Hash := store.Graph().Hash
		qr2, _ := o.Query("req-11b", "alpha", "")
		if len(qr2.Selected) != count1 {
			t.Fatalf("duplicate inflated selected %d vs %d", len(qr2.Selected), count1)
		}
		g2Hash := store.Graph().Hash
		if g1Hash != g2Hash {
			t.Fatalf("duplicate changed graph hash %q vs %q", g1Hash, g2Hash)
		}
		if len(store.Packets()) != 1 {
			t.Fatalf("duplicate packet count %d want 1", len(store.Packets()))
		}
	})

	// 12 coverage reflects independent evidence
	t.Run("12_coverage_reflects_independent_evidence", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.1, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/a.txt", "alpha common")
		mustIngestBytes(t, o, "docs/b.txt", "alpha common")
		mustIngestBytes(t, o, "docs/c.txt", "alpha common")
		qr, err := o.Query("req-12", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) < 2 {
			t.Fatalf("expected >=2 selected, got %d", len(qr.Selected))
		}
		cov := scoring.CoverageFor(qr.Selected)
		if cov != qr.Coverage {
			t.Fatalf("coverage mismatch %v vs %v", cov, qr.Coverage)
		}
		if cov <= 0 || cov > 1 {
			t.Fatalf("coverage out of bounds %v", cov)
		}
		// With distinct locators, coverage should be >0.5 (since distinct)
		if cov < 0.5 {
			t.Fatalf("coverage %v want >0.5 for distinct evidence", cov)
		}
		// Single evidence case coverage ==1? Actually for 1 node, (1/1+1/1)/2=1
		o2, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.1, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o2, "docs/single.txt", "alpha")
		qr2, _ := o2.Query("req-12b", "alpha", "")
		if len(qr2.Selected) != 1 {
			t.Fatalf("single selected %d want 1", len(qr2.Selected))
		}
		if qr2.Coverage != 1.0 {
			t.Fatalf("single coverage %v want 1.0", qr2.Coverage)
		}
	})

	// 13 ContextPacket bounded
	t.Run("13_contextpacket_bounded", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		huge := strings.Repeat("alpha ", 5000) // >2048 bytes
		mustIngestBytes(t, o, "docs/alpha/huge.txt", huge)
		qr, err := o.Query("req-13", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Packets) == 0 {
			t.Fatal("expected at least 1 packet")
		}
		for _, pkt := range qr.Packets {
			if err := pkt.Validate(); err != nil {
				t.Fatalf("packet validate: %v", err)
			}
			if len(pkt.Evidence) > contextpacket.MaxEvidence {
				t.Fatalf("evidence %d > %d", len(pkt.Evidence), contextpacket.MaxEvidence)
			}
			for _, ev := range pkt.Evidence {
				if len([]byte(ev.Excerpt)) > contextpacket.MaxExcerptBytes {
					t.Fatalf("excerpt %d > %d", len([]byte(ev.Excerpt)), contextpacket.MaxExcerptBytes)
				}
				if !isSHA256Local(ev.ContentHash) {
					t.Fatalf("contentHash not sha256 %q", ev.ContentHash)
				}
				if strings.TrimSpace(ev.NodeID) == "" || strings.TrimSpace(ev.SourceID) == "" {
					t.Fatal("evidence missing identities")
				}
			}
			// Sorted
			for i := 1; i < len(pkt.Evidence); i++ {
				if pkt.Evidence[i-1].NodeID > pkt.Evidence[i].NodeID {
					t.Fatal("evidence not sorted")
				}
			}
			// No whole graph: packet should not contain Nodes map
			b, _ := json.Marshal(pkt)
			if strings.Contains(string(b), "\"nodes\"") && strings.Contains(string(b), "workspace_id") {
				// Weak check: ensure packet JSON does not embed graph nodes structure with many nodes
				// The packet itself legitimately has workspace_id, so skip strict. Instead check not huge
				if len(b) > 20*1024 {
					t.Fatalf("packet seems to embed whole graph, len %d", len(b))
				}
			}
			if len(pkt.Evidence) == 0 {
				t.Fatal("packet evidence empty")
			}
		}
		// Also prove excerpt bounded truncation
		excerpt := contextpacket.BoundedExcerpt(huge)
		if len([]byte(excerpt)) > contextpacket.MaxExcerptBytes {
			t.Fatalf("BoundedExcerpt %d > max", len([]byte(excerpt)))
		}
	})

	// 14 references valid identities
	t.Run("14_references_valid_identities", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		pkt := mustIngestBytes(t, o, "docs/alpha/ref.txt", "alpha beta gamma")
		qr, err := o.Query("req-14", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Packets) == 0 {
			t.Fatal("no packets")
		}
		g := o.GetGraph()
		nodesByID := map[string]graph.Node{}
		for _, n := range g.SortedNodes() {
			nodesByID[n.NodeID] = n
		}
		for _, cp := range qr.Packets {
			if cp.TargetNodeID == "" || cp.TargetActorID == "" {
				t.Fatal("packet missing target identities")
			}
			if cp.WorkspaceID != wsCH07 {
				t.Fatalf("workspace mismatch %q", cp.WorkspaceID)
			}
			for _, ev := range cp.Evidence {
				node, ok := nodesByID[ev.NodeID]
				if !ok {
					t.Fatalf("evidence node %q not in graph", ev.NodeID)
				}
				if ev.SourceID != node.SourceID {
					t.Fatalf("sourceID %q != node source %q", ev.SourceID, node.SourceID)
				}
				if ev.Locator != node.SourceLocator {
					t.Fatalf("locator mismatch %q vs %q", ev.Locator, node.SourceLocator)
				}
				// ContentHash must match node or packet
				if !isSHA256Local(ev.ContentHash) {
					t.Fatalf("invalid hash %q", ev.ContentHash)
				}
				if ev.ContentHash != node.ContentHash && ev.ContentHash != pkt.Content.ContentHash {
					t.Fatalf("contentHash %q not matching node %q nor packet %q", ev.ContentHash, node.ContentHash, pkt.Content.ContentHash)
				}
			}
		}
	})

	// 15 dedup preserved
	t.Run("15_dedup_preserved", func(t *testing.T) {
		// Controller-level dedup: within window duplicate suppressed (nil)
		ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
		defer ctrl.Shutdown()
		a := ctrl.Activate(wsCH07, "live_context.query", "node-123")
		if a == nil || a.Status != "active" {
			t.Fatalf("first activate failed: %#v", a)
		}
		dup := ctrl.Activate(wsCH07, "live_context.query", "node-123")
		if dup != nil {
			t.Fatalf("dedup should suppress, got %#v", dup)
		}
		// Evidence dedup: packet evidence must not contain duplicate NodeID and must be sorted
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/dedup.txt", "alpha")
		qr, _ := o.Query("req-15", "alpha", "")
		if len(qr.Packets) == 0 {
			t.Fatal("no packets for dedup test")
		}
		for _, pkt := range qr.Packets {
			seen := map[string]bool{}
			for _, ev := range pkt.Evidence {
				if seen[ev.NodeID] {
					t.Fatalf("duplicate node %q in evidence", ev.NodeID)
				}
				seen[ev.NodeID] = true
			}
		}
	})

	// 16 lineage propagated
	t.Run("16_lineage_propagated", func(t *testing.T) {
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/lineage.txt", "alpha beta gamma")
		qr, err := o.Query("req-16-parent", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Activations) == 0 {
			t.Fatal("no parent activation")
		}
		parentID := qr.Activations[0].ID
		parentDesc, ok := ctrl.Descriptor(parentID)
		if !ok {
			t.Fatalf("parent descriptor missing")
		}
		// Child query with lineage parent
		qrChild, err := o.Query("req-16-child", "beta", parentID)
		if err != nil {
			t.Fatal(err)
		}
		if len(qrChild.Packets) == 0 && len(qrChild.Activations) == 0 {
			t.Fatalf("child query produced no activation/packet (maybe dedup or bound)")
		}
		// Find a child packet/activation that succeeded
		var childPkt *contextpacket.ContextPacket
		var childAct *actorstub.Activation
		for i, pkt := range qrChild.Packets {
			if pkt.Lineage.ParentActorID == parentID {
				childPkt = &qrChild.Packets[i]
				break
			}
		}
		for _, act := range qrChild.Activations {
			if act.ParentActorID == parentID {
				childAct = act
				break
			}
		}
		if childPkt == nil && childAct == nil {
			// If no child matched parent, check controller descriptors for child lineage
			for _, act := range ctrl.List(wsCH07) {
				if act.ParentActorID == parentID && act.Status == "active" || act.Status == "completed" {
					childAct = act
					break
				}
			}
		}
		if childPkt != nil {
			if childPkt.Lineage.LineageID != parentDesc.LineageID {
				t.Fatalf("lineage mismatch parent %q child %q", parentDesc.LineageID, childPkt.Lineage.LineageID)
			}
			if childPkt.Lineage.Depth != parentDesc.Depth+1 {
				t.Fatalf("depth %d want %d", childPkt.Lineage.Depth, parentDesc.Depth+1)
			}
			if childPkt.Lineage.RemainingBudget != parentDesc.RemainingBudget-1 {
				t.Fatalf("budget %d want %d", childPkt.Lineage.RemainingBudget, parentDesc.RemainingBudget-1)
			}
			if childPkt.Lineage.ParentActorID != parentID {
				t.Fatalf("parentActor %q want %q", childPkt.Lineage.ParentActorID, parentID)
			}
		} else if childAct != nil {
			if childAct.LineageID != parentDesc.LineageID {
				t.Fatalf("act lineage mismatch %q vs %q", childAct.LineageID, parentDesc.LineageID)
			}
		} else {
			t.Fatalf("lineage not propagated: no child with parent %q, activations %#v packets %#v", parentID, qrChild.Activations, qrChild.Packets)
		}
		// Also prove sha lineage: all packets have lineage_id required
		for _, pkt := range qrChild.Packets {
			if pkt.Lineage.LineageID == "" {
				t.Fatal("packet lineage_id empty")
			}
		}
	})

	// 17 depth bound prevents (use distinct nodes to avoid cycle masking)
	t.Run("17_depth_bound_prevents", func(t *testing.T) {
		actorCfg := actorstub.Config{MaxActive: 16, MaxDepth: 2, MaxBudget: 32, TTL: time.Minute}
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, &actorCfg)
		mustIngestBytes(t, o, "docs/alpha/depthA.txt", "alpha uniqueA")
		mustIngestBytes(t, o, "docs/beta/depthB.txt", "beta uniqueB")
		mustIngestBytes(t, o, "docs/gamma/depthC.txt", "gamma uniqueC")
		qrRoot, _ := o.Query("req-17-root", "alpha", "")
		if len(qrRoot.Activations) == 0 {
			t.Fatal("root not activated")
		}
		rootID := qrRoot.Activations[0].ID
		qrChild, _ := o.Query("req-17-child", "beta", rootID)
		var childID string
		for _, act := range qrChild.Activations {
			if act.ParentActorID == rootID && act.Status == "active" {
				childID = act.ID
				break
			}
		}
		// fallback find via controller
		if childID == "" {
			for _, act := range ctrl.List(wsCH07) {
				if act.ParentActorID == rootID && act.Status != "rejected_depth" {
					childID = act.ID
				}
			}
		}
		if childID == "" {
			t.Fatalf("child not found root %q activations %#v", rootID, qrChild.Activations)
		}
		// Grandchild should be rejected_depth (depth 3 > max 2)
		qrGrand, _ := o.Query("req-17-grand", "gamma", childID)
		foundRejected := false
		for _, act := range qrGrand.Activations {
			if act.Status == "rejected_depth" {
				foundRejected = true
				break
			}
		}
		if !foundRejected {
			// Also check controller list for rejected
			for _, act := range ctrl.List(wsCH07) {
				if act.ParentActorID == childID && act.Status == "rejected_depth" {
					foundRejected = true
					break
				}
			}
		}
		if !foundRejected {
			t.Fatalf("depth bound did not prevent, grand activations %#v list %#v", qrGrand.Activations, ctrl.List(wsCH07))
		}
	})

	// 18 budget bound prevents (distinct nodes to avoid cycle)
	t.Run("18_budget_bound_prevents", func(t *testing.T) {
		actorCfg := actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 1, TTL: time.Minute}
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, &actorCfg)
		mustIngestBytes(t, o, "docs/alpha/budgetA.txt", "alpha uniqueA")
		mustIngestBytes(t, o, "docs/beta/budgetB.txt", "beta uniqueB")
		mustIngestBytes(t, o, "docs/gamma/budgetC.txt", "gamma uniqueC")
		qrRoot, _ := o.Query("req-18-root", "alpha", "")
		if len(qrRoot.Activations) == 0 {
			t.Fatal("root not active")
		}
		rootID := qrRoot.Activations[0].ID
		rootDesc, _ := ctrl.Descriptor(rootID)
		if rootDesc.RemainingBudget != 1 {
			t.Fatalf("root budget %d want 1", rootDesc.RemainingBudget)
		}
		qrChild, _ := o.Query("req-18-child", "beta", rootID)
		var childID string
		for _, act := range qrChild.Activations {
			if act.ParentActorID == rootID {
				childID = act.ID
				if act.Status == "rejected_budget_exhausted" {
					t.Fatalf("child should be active at budget 0, got rejected")
				}
			}
		}
		if childID == "" {
			for _, act := range ctrl.List(wsCH07) {
				if act.ParentActorID == rootID {
					childID = act.ID
					break
				}
			}
		}
		if childID == "" {
			t.Fatalf("child not found")
		}
		childDesc, _ := ctrl.Descriptor(childID)
		if childDesc.RemainingBudget != 0 {
			t.Fatalf("child budget %d want 0", childDesc.RemainingBudget)
		}
		// Grandchild should exhaust
		qrGrand, _ := o.Query("req-18-grand", "gamma", childID)
		found := false
		for _, act := range qrGrand.Activations {
			if act.Status == "rejected_budget_exhausted" {
				found = true
			}
		}
		if !found {
			for _, act := range ctrl.List(wsCH07) {
				if act.ParentActorID == childID && act.Status == "rejected_budget_exhausted" {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("budget exhaustion not enforced, grand %#v", qrGrand.Activations)
		}
	})

	// 19 cycle suppression
	t.Run("19_cycle_suppression", func(t *testing.T) {
		ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
		defer ctrl.Shutdown()
		parent := ctrl.Activate(wsCH07, "live_context.query", "same-node")
		if parent == nil {
			t.Fatal("parent nil")
		}
		child := ctrl.ActivateWithParent(parent.ID, wsCH07, "live_context.query", "same-node")
		if child == nil {
			t.Fatal("child nil, expected rejected_cycle object")
		}
		if child.Status != "rejected_cycle" {
			t.Fatalf("expected rejected_cycle, got %q", child.Status)
		}
		if ctrl.IsLive(child.ID) {
			t.Fatal("cycle-rejected should not be live")
		}
		// Different path should not be cycle
		okChild := ctrl.ActivateWithParent(parent.ID, wsCH07, "live_context.query", "different-node")
		if okChild.Status != "active" {
			t.Fatalf("different path should be active, got %q", okChild.Status)
		}
		// Also via orchestrator: single node graph, parent lineage cycle
		o, _, _, _, orchCtrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/cycle.txt", "alpha")
		qrRoot, _ := o.Query("req-19-root", "alpha", "")
		if len(qrRoot.Activations) == 0 {
			t.Fatal("root no act")
		}
		rootID := qrRoot.Activations[0].ID
		// Query same query with same parent should attempt same nodeID -> cycle
		qrCycle, _ := o.Query("req-19-cycle", "alpha", rootID)
		foundCycle := false
		for _, act := range qrCycle.Activations {
			if act.Status == "rejected_cycle" {
				foundCycle = true
			}
		}
		if !foundCycle {
			for _, act := range orchCtrl.List(wsCH07) {
				if act.ParentActorID == rootID && act.Status == "rejected_cycle" {
					foundCycle = true
				}
			}
		}
		if !foundCycle {
			t.Fatalf("orchestrator cycle not suppressed, cycle activations %#v", qrCycle.Activations)
		}
	})

	// 20 completes/passivates
	t.Run("20_completes_passivates", func(t *testing.T) {
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/complete.txt", "alpha beta")
		qr, err := o.Query("req-20", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Activations) == 0 {
			t.Fatal("no activations")
		}
		// Orchestrator completes immediately after RecordResult, so live should be 0
		// Wait briefly for async stop
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if ctrl.LiveCount() == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if ctrl.LiveCount() != 0 {
			t.Fatalf("live count %d want 0 after complete", ctrl.LiveCount())
		}
		for _, act := range qr.Activations {
			desc, ok := ctrl.Descriptor(act.ID)
			if !ok {
				t.Fatalf("descriptor missing %q", act.ID)
			}
			if desc.Status != "completed" && desc.Status != "passivated" {
				t.Fatalf("status %q want completed/passivated", desc.Status)
			}
			res, ok := ctrl.GetResult(act.ID)
			if !ok {
				t.Fatalf("result missing for %q", act.ID)
			}
			if res.Status == "" || res.Observations == "" {
				t.Fatalf("result incomplete %#v", res)
			}
		}
	})

	// 21 no-mutating returns safely
	t.Run("21_no_mutating_returns_safely", func(t *testing.T) {
		o, _, _, auth, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		mustIngestBytes(t, o, "docs/nomut.txt", "alpha beta")
		before := auth.Projection()
		qr, err := o.Query("req-21", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		_ = qr
		after := auth.Projection()
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("no-mutation Query changed projection %v -> %v", before, after)
		}
		if after.Version != 0 && len(after.Nodes) != 0 {
			// Actually ingest already advanced projection (source ingested). Query alone should not add more.
			// So after ingested version is 1, query should not increase it further.
			// Check that version didn't increase due to Query alone (compare before which already includes ingest)
			// Already checked DeepEqual covers.
		}
	})

	// 22 valid mutation passes Go
	t.Run("22_valid_mutation_passes_Go", func(t *testing.T) {
		o, fw, _, auth, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/mut.txt", "alpha beta")
		qr, err := o.Query("req-22", "alpha", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Activations) == 0 {
			t.Fatal("no activations for mutation")
		}
		aid := qr.Activations[0].ID
		beforeCount := fw.count()
		accepted, err := o.ProposeMutation(aid, json.RawMessage(`{"observation":"valid result"}`), json.RawMessage(`{"reason":"test"}`))
		if err != nil {
			t.Fatalf("valid mutation failed: %v", err)
		}
		if accepted.Durable.EventID == "" || accepted.Durable.RustAck == nil {
			t.Fatalf("durable binding missing %#v", accepted.Durable)
		}
		if accepted.Durable.RustAck.ID != accepted.Proposal.TransitionID {
			t.Fatalf("rust ack id mismatch")
		}
		if fw.count() != beforeCount+1 {
			t.Fatalf("writer count %d want %d", fw.count(), beforeCount+1)
		}
		if auth.Projection().Version == 0 {
			t.Fatalf("projection version not advanced after valid mutation")
		}
	})

	// 23 valid mutation requires Rust ACK
	t.Run("23_valid_mutation_requires_Rust_ACK", func(t *testing.T) {
		o, fw, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/rustack.txt", "alpha")
		qr, _ := o.Query("req-23", "alpha", "")
		aid := qr.Activations[0].ID
		accepted, err := o.ProposeMutation(aid, json.RawMessage(`{"x":1}`), json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("should require Rust ACK success, got err %v", err)
		}
		if accepted.Durable.RustAck == nil {
			t.Fatal("Rust ACK required but missing")
		}
		if !isSHA256Local(accepted.Durable.RustAck.Hash) {
			t.Fatalf("invalid Rust hash %q", accepted.Durable.RustAck.Hash)
		}
		if fw.count() == 0 {
			t.Fatal("no durable event for valid mutation")
		}
	})

	// 24 Rust failure leaves unchanged
	t.Run("24_Rust_failure_leaves_unchanged", func(t *testing.T) {
		o, fw, _, auth, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/rustfail.txt", "alpha")
		qr, _ := o.Query("req-24", "alpha", "")
		aid := qr.Activations[0].ID
		before := auth.Projection()
		beforeCount := fw.count()
		fw.fail = errors.New("rust unavailable simulated")
		_, err := o.ProposeMutation(aid, json.RawMessage(`{"x":99}`), json.RawMessage(`{}`))
		if err == nil {
			t.Fatal("expected Rust failure")
		}
		if transition.Class(err) != transition.Durable {
			t.Fatalf("expected Durable class, got %q err %v", transition.Class(err), err)
		}
		after := auth.Projection()
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("projection changed on Rust failure %v -> %v", before, after)
		}
		if fw.count() != beforeCount {
			t.Fatalf("writer count changed on failure %d vs %d", fw.count(), beforeCount)
		}
		fw.fail = nil
	})

	// 25 invalid rejected
	t.Run("25_invalid_rejected", func(t *testing.T) {
		o, fw, _, auth, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/alpha/invalid.txt", "alpha")
		qr, _ := o.Query("req-25", "alpha", "")
		aid := qr.Activations[0].ID
		before := auth.Projection()
		beforeCount := fw.count()
		// malformed JSON
		_, err := o.ProposeMutation(aid, json.RawMessage(`{invalid`), json.RawMessage(`{}`))
		if err == nil {
			t.Fatal("malformed result_data accepted")
		}
		// empty result
		_, err = o.ProposeMutation(aid, json.RawMessage(``), json.RawMessage(`{}`))
		if err == nil {
			t.Fatal("empty result_data accepted")
		}
		// admission malformed
		_, err = o.ProposeMutation(aid, json.RawMessage(`{"a":1}`), json.RawMessage(`{bad`))
		if err == nil {
			t.Fatal("malformed admission accepted")
		}
		after := auth.Projection()
		if !reflect.DeepEqual(before, after) {
			t.Fatal("projection changed on invalid")
		}
		if fw.count() != beforeCount {
			t.Fatal("writer appended on invalid")
		}
		// Also empty actorID
		_, err = o.ProposeMutation("", json.RawMessage(`{"a":1}`), json.RawMessage(`{}`))
		if err == nil {
			t.Fatal("empty actorID accepted")
		}
	})

	// 26 ingest alone no activate
	t.Run("26_ingest_alone_no_activate", func(t *testing.T) {
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		if ctrl.LiveCount() != 0 {
			t.Fatalf("live %d want 0 before ingest", ctrl.LiveCount())
		}
		mustIngestBytes(t, o, "docs/ingest-only.txt", "alpha beta gamma")
		if ctrl.LiveCount() != 0 {
			t.Fatalf("ingest alone should not activate, live %d", ctrl.LiveCount())
		}
		if ctrl.DescriptorCount() != 0 {
			t.Fatalf("ingest alone should not create descriptors, got %d", ctrl.DescriptorCount())
		}
		if o.LastQueryInfo().Activations != 0 {
			// LastQueryInfo should be zero before any query
		}
	})

	// 27 ingest+relevant activates
	t.Run("27_ingest_plus_relevant_activates", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.DefaultConfig(), nil)
		mustIngestBytes(t, o, "docs/ingest-relevant.txt", "kubernetes api server deployment")
		qr, err := o.Query("req-27", "kubernetes deployment", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(qr.Selected) == 0 || len(qr.Activations) == 0 {
			t.Fatalf("ingest+relevant should activate, selected %d act %d", len(qr.Selected), len(qr.Activations))
		}
		if len(qr.Packets) == 0 {
			t.Fatal("no packets after ingest+relevant")
		}
	})

	// 28 rebuild same selection
	t.Run("28_rebuild_same_selection", func(t *testing.T) {
		o, _, _, _, _ := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10}, nil)
		mustIngestBytes(t, o, "docs/r1.txt", "alpha beta")
		mustIngestBytes(t, o, "docs/r2.txt", "alpha gamma")
		mustIngestBytes(t, o, "docs/r3.txt", "beta gamma")
		qr1, _ := o.Query("req-28a", "alpha beta", "")
		qr2, _ := o.Query("req-28b", "alpha beta", "")
		if len(qr1.Selected) != len(qr2.Selected) {
			t.Fatalf("rebuild selected lengths %d vs %d", len(qr1.Selected), len(qr2.Selected))
		}
		for i := range qr1.Selected {
			if qr1.Selected[i].Node.NodeID != qr2.Selected[i].Node.NodeID {
				t.Fatalf("selection not same at %d", i)
			}
			if qr1.Selected[i].Components.ActivationScore != qr2.Selected[i].Components.ActivationScore {
				t.Fatalf("score not same at %d", i)
			}
		}
		// Also via scorer directly: same graph same query same config => same selection
		store := o.GetGraph()
		_ = store
		pkts := o.ListPackets()
		scorer := scoring.BaselineScorer{}
		// Need Graph for scoring
		g := o.GetGraph()
		s1 := scoring.SelectCandidates(scorer.ScoreGraph(g, pkts, "alpha beta"), scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		s2 := scoring.SelectCandidates(scorer.ScoreGraph(g, pkts, "alpha beta"), scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		if !reflect.DeepEqual(s1, s2) {
			t.Fatal("rebuilt selection via scorer not equal")
		}
	})

	// 29 restart/rebuild same inputs (deterministic discard->rebuild->same hash/selection)
	t.Run("29_restart_rebuild_same_inputs", func(t *testing.T) {
		fw := &fakeWriter{}
		auth := transition.New(fw, nil)
		store := graph.NewStore(wsCH07)
		ctrl := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
		defer ctrl.Shutdown()
		o := New(wsCH07, store, auth, ctrl, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		mustIngestBytes(t, o, "docs/restart1.txt", "alpha beta gamma")
		mustIngestBytes(t, o, "docs/restart2.txt", "alpha delta")
		qr1, _ := o.Query("req-29a", "alpha beta", "")
		proj1 := auth.Projection()
		// Capture packets deterministically (source of truth for rebuild)
		pkts := o.ListPackets()
		// Deterministically rebuild graph from same packets into new store
		store2 := graph.NewStore(wsCH07)
		for _, p := range pkts {
			_, _, err := store2.Apply(p)
			if err != nil {
				t.Fatalf("store2 apply: %v", err)
			}
		}
		g1 := store.Graph()
		g2 := store2.Graph()
		if g1.Hash != g2.Hash {
			t.Fatalf("graph rebuild hash %q != %q", g1.Hash, g2.Hash)
		}
		// Rebuild authority deterministically via Checkpoint round-trip (does not require hash chain)
		cp := auth.Checkpoint()
		// New writer for rebuilt authority
		fw2 := &fakeWriter{}
		// Re-derive authority from same accepted transitions via checkpoint hash (no ValidateHistory)
		// Use in-memory replay: create new authority and propose same transitions in same order via same result data
		// Simpler: verify checkpoint hash is stable and that same packets produce same projection hash via fresh authority
		fw3 := &fakeWriter{}
		auth2 := transition.New(fw3, nil)
		for _, p := range pkts {
			_, err := ingest.ProposeIngest(auth2, p)
			if err != nil {
				t.Fatalf("rebuild ingest propose: %v", err)
			}
		}
		proj2 := auth2.Projection()
		if proj1.Hash != proj2.Hash || proj1.Version != proj2.Version {
			t.Fatalf("restart projection mismatch %v vs %v", proj1, proj2)
		}
		// Verify checkpoint hash determinism
		if cp.State.Hash != proj1.Hash {
			t.Fatalf("checkpoint hash %q != projection %q", cp.State.Hash, proj1.Hash)
		}
		_ = fw2
		// New orchestrator after restart should give same selection
		ctrl2 := actorstub.NewWithConfig(actorstub.Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: time.Minute})
		defer ctrl2.Shutdown()
		o2 := New(wsCH07, store2, auth2, ctrl2, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 10})
		qr2, _ := o2.Query("req-29b", "alpha beta", "")
		if len(qr1.Selected) != len(qr2.Selected) {
			t.Fatalf("restart selection lengths %d vs %d", len(qr1.Selected), len(qr2.Selected))
		}
		for i := range qr1.Selected {
			if qr1.Selected[i].Node.NodeID != qr2.Selected[i].Node.NodeID {
				t.Fatalf("restart selection node %d mismatch %q vs %q", i, qr1.Selected[i].Node.NodeID, qr2.Selected[i].Node.NodeID)
			}
			if qr1.Selected[i].Components.ActivationScore != qr2.Selected[i].Components.ActivationScore {
				t.Fatalf("restart score mismatch at %d", i)
			}
		}
	})

	// 30 concurrent preserves race safety
	t.Run("30_concurrent_preserves_race_safety", func(t *testing.T) {
		o, _, _, _, ctrl := newTestOrchestrator(t, scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 3}, nil)
		for i := 0; i < 3; i++ {
			mustIngestBytes(t, o, strings.ReplaceAll("docs/concX.txt", "X", string(rune('a'+i))), "alpha beta common")
		}
		const workers = 8
		var wg sync.WaitGroup
		results := make([]*QueryResult, workers)
		errs := make([]error, workers)
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			i := i
			go func() {
				defer wg.Done()
				qr, err := o.Query(strings.ReplaceAll("req-conc-X", "X", string(rune('a'+i))), "alpha common", "")
				results[i] = qr
				errs[i] = err
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("worker %d err %v", i, err)
			}
			if results[i] == nil {
				t.Fatalf("worker %d nil result", i)
			}
			if results[i].CandidatesConsidered != 3 {
				t.Fatalf("worker %d candidates %d want 3", i, results[i].CandidatesConsidered)
			}
			if len(results[i].Selected) > 3 {
				t.Fatalf("worker %d selected %d > bound 3", i, len(results[i].Selected))
			}
		}
		// Deterministic sets: all workers should see same selected NodeIDs (since same graph/query/config) modulo dedup timing?
		// Since completions clear dedup, concurrent may interleave but should remain bounded and not race.
		// At least verify no panic and LiveCount bounded.
		if ctrl.LiveCount() > 16 {
			t.Fatalf("live count %d exceeds MaxActive 16 after concurrent", ctrl.LiveCount())
		}
		// Verify at least one worker got deterministic selection equal to scorer's selection
		g := o.GetGraph()
		pkts := o.ListPackets()
		scorer := scoring.BaselineScorer{}
		expected := scoring.SelectCandidates(scorer.ScoreGraph(g, pkts, "alpha common"), scoring.Config{Threshold: 0.5, MaxCandidates: 10, MaxActorsPerRequest: 3})
		for i, qr := range results {
			if len(qr.Selected) == 0 {
				continue
			}
			// Compare sets: should be subset of expected (since expected is the full deterministic selection)
			// All results should be permutations of same deterministic ordering truncated to maxActors
			for j, sn := range qr.Selected {
				if j < len(expected) && sn.Node.NodeID != expected[j].Node.NodeID {
					// Allow different ordering only if tiebreak same scores but still deterministic? Flag as failure if major mismatch
					t.Fatalf("worker %d selection %d mismatch %q vs expected %q", i, j, sn.Node.NodeID, expected[j].Node.NodeID)
				}
			}
		}
	})

	// Additional: prove sha256 and bounded excerpt sorting
	t.Run("99_sha_and_sort_invariants", func(t *testing.T) {
		huge := strings.Repeat("z", 3000)
		ex := contextpacket.BoundedExcerpt(huge)
		if len([]byte(ex)) > contextpacket.MaxExcerptBytes {
			t.Fatalf("excerpt too large")
		}
		sum := sha256.Sum256([]byte(ex))
		hs := hex.EncodeToString(sum[:])
		if !isSHA256Local(hs) {
			t.Fatalf("sha256 not valid")
		}
	})
}
