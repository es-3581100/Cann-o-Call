package observability

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/orchestrator"
	"flatten-workspace/internal/progress"
)

// ActorMetricsFromController derives read-only metrics without mutating controller.
// It respects ephemeral nature: descriptors are in-memory only.
func ActorMetricsFromController(c *actorstub.Controller) progress.ActorMetrics {
	if c == nil {
		return progress.ActorMetrics{}
	}
	list := c.List("")
	m := progress.ActorMetrics{}
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

// LedgerMetricsFromService derives metrics read-only from eventlog service.
// It does not create second ledger, only snapshots.
func LedgerMetricsFromService(svc *eventlog.Service, dataDir string) progress.LedgerMetrics {
	m := progress.LedgerMetrics{}
	if svc == nil {
		return m
	}
	events, err := svc.List()
	if err != nil {
		events = nil
	}
	m.AcceptedEventCount = int64(len(events))
	if len(events) > 0 {
		last := events[len(events)-1]
		m.CurrentSequence = last.Seq
		m.HeadEventID = last.ID
		m.HeadHash = last.Hash
	}
	verify, err := svc.Verify()
	if err == nil {
		if ok, _ := verify["ok"].(bool); ok {
			m.VerificationStatus = "ok"
		} else {
			m.VerificationStatus = "failed"
		}
	} else {
		m.VerificationStatus = "error"
	}
	// Checkpoint presence/age
	checkpointDir := filepath.Join(dataDir, "checkpoints")
	entries, err := os.ReadDir(checkpointDir)
	if err == nil {
		var files []os.DirEntry
		for _, e := range entries {
			if !e.IsDir() && strings.HasPrefix(e.Name(), "checkpoint-") {
				files = append(files, e)
			}
		}
		if len(files) > 0 {
			m.CheckpointPresent = true
			// Find most recent by mod time
			var latest os.DirEntry
			var latestTime time.Time
			var latestSeq *int64
			for _, f := range files {
				info, ierr := f.Info()
				if ierr != nil {
					continue
				}
				mod := info.ModTime()
				if latest == nil || mod.After(latestTime) {
					latest = f
					latestTime = mod
					// Try parse seq from filename checkpoint-XXXXXX-*
					seq := parseCheckpointSeq(f.Name())
					if seq != nil {
						cpy := *seq
						latestSeq = &cpy
					} else {
						latestSeq = nil
					}
				}
			}
			if latest != nil {
				age := time.Since(latestTime).Truncate(time.Millisecond).String()
				m.CheckpointAge = &age
				if latestSeq != nil {
					m.CheckpointSequence = latestSeq
				} else {
					// Try read checkpoint file for seq field
					p := filepath.Join(checkpointDir, latest.Name())
					if b, rerr := os.ReadFile(p); rerr == nil {
						var obj map[string]any
						if jerr := json.Unmarshal(b, &obj); jerr == nil {
							if v, ok := obj["seq"]; ok {
								switch vv := v.(type) {
								case float64:
									iv := int64(vv)
									m.CheckpointSequence = &iv
								case int64:
									m.CheckpointSequence = &vv
								}
							}
						}
					}
				}
			}
		}
	}
	// Replay status: if verification ok then ok else corrupt; also try checked events
	if m.VerificationStatus == "ok" {
		m.ReplayStatus = "ok"
	} else if m.VerificationStatus == "failed" {
		m.ReplayStatus = "corrupt"
	} else {
		m.ReplayStatus = "unknown"
	}
	return m
}

func parseCheckpointSeq(name string) *int64 {
	parts := strings.Split(name, "-")
	if len(parts) < 2 {
		return nil
	}
	seqStr := parts[1]
	seqStr = strings.TrimSpace(seqStr)
	if seqStr == "" {
		return nil
	}
	for _, ch := range seqStr {
		if ch < '0' || ch > '9' {
			return nil
		}
	}
	var v int64
	for _, ch := range seqStr {
		v = v*10 + int64(ch-'0')
	}
	return &v
}

// LedgerMetricsOffline fallback when service unavailable.
func LedgerMetricsOffline(dataDir string) progress.LedgerMetrics {
	m := progress.LedgerMetrics{}
	ledgerPath := filepath.Join(dataDir, "events", "ledger.jsonl")
	b, err := os.ReadFile(ledgerPath)
	if err != nil {
		return m
	}
	lines := strings.Split(string(b), "\n")
	var count int64
	var lastID, lastHash string
	var lastSeq int64
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		var ev eventlog.Event
		if err := json.Unmarshal([]byte(ln), &ev); err != nil {
			continue
		}
		count++
		lastID = ev.ID
		lastHash = ev.Hash
		lastSeq = ev.Seq
	}
	m.AcceptedEventCount = count
	m.CurrentSequence = lastSeq
	m.HeadEventID = lastID
	m.HeadHash = lastHash
	m.VerificationStatus = "unknown"
	m.ReplayStatus = "unknown"
	// checkpoint
	checkpointDir := filepath.Join(dataDir, "checkpoints")
	entries, err := os.ReadDir(checkpointDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasPrefix(e.Name(), "checkpoint-") {
				m.CheckpointPresent = true
				break
			}
		}
	}
	return m
}

// GraphMetricsFromStore derives read-only graph metrics.
func GraphMetricsFromStore(s *graph.Store) progress.GraphMetrics {
	if s == nil {
		return progress.GraphMetrics{}
	}
	g := s.Graph()
	if g == nil {
		return progress.GraphMetrics{}
	}
	m := progress.GraphMetrics{}
	m.NodeCount = len(g.Nodes)
	m.EdgeCount = len(g.Edges)
	// source count derived from packets length if store available
	packets := s.Packets()
	m.SourceCount = len(packets)
	return m
}

// GraphMetricsAggregate aggregates across multiple stores.
func GraphMetricsAggregate(stores map[string]*graph.Store) progress.GraphMetrics {
	total := progress.GraphMetrics{}
	for _, s := range stores {
		m := GraphMetricsFromStore(s)
		total.NodeCount += m.NodeCount
		total.EdgeCount += m.EdgeCount
		total.SourceCount += m.SourceCount
	}
	return total
}

// ContextMetricsFromOrchestrator derives context metrics from last query info.
func ContextMetricsFromOrchestrator(o *orchestrator.Orchestrator) progress.ContextMetrics {
	if o == nil {
		return progress.ContextMetrics{}
	}
	info := o.LastQueryInfo()
	return progress.ContextMetrics{
		CandidatesConsidered: info.CandidatesConsidered,
		CandidatesSelected:   info.Selected,
		SelectedCount:        info.Selected,
		Coverage:             info.Coverage,
	}
}

// ContextMetricsAggregate aggregates across orchestrators, picks max coverage.
func ContextMetricsAggregate(orchs map[string]*orchestrator.Orchestrator) progress.ContextMetrics {
	var agg progress.ContextMetrics
	if len(orchs) == 0 {
		return agg
	}
	// Pick most recent info by max candidates considered
	var best orchestrator.QueryInfo
	first := true
	for _, o := range orchs {
		info := o.LastQueryInfo()
		if first || info.CandidatesConsidered > best.CandidatesConsidered {
			best = info
			first = false
		}
	}
	agg.CandidatesConsidered = best.CandidatesConsidered
	agg.CandidatesSelected = best.Selected
	agg.SelectedCount = best.Selected
	agg.Coverage = best.Coverage
	return agg
}

// SystemStatus is unified status projection.
type SystemStatus struct {
	Ledger      progress.LedgerMetrics  `json:"ledger"`
	Graph       progress.GraphMetrics   `json:"graph"`
	Actor       progress.ActorMetrics   `json:"actor"`
	Context     progress.ContextMetrics `json:"context"`
	Replay      string                  `json:"replay_status"`
	Checkpoint  CheckpointInfo          `json:"checkpoint"`
	Workspaces  int                     `json:"workspaces"`
	EventCount  int64                   `json:"event_count"`
	HeadEventID string                  `json:"head_event_id,omitempty"`
	HeadHash    string                  `json:"head_hash,omitempty"`
}

type CheckpointInfo struct {
	Present  bool    `json:"present"`
	Age      *string `json:"age,omitempty"`
	Sequence *int64  `json:"sequence,omitempty"`
}

// CollectSystemStatus builds deterministic unified status without mutating authorities.
func CollectSystemStatus(
	ledger progress.LedgerMetrics,
	graphM progress.GraphMetrics,
	actorM progress.ActorMetrics,
	ctxM progress.ContextMetrics,
	workspaces int,
) SystemStatus {
	s := SystemStatus{
		Ledger:      ledger,
		Graph:       graphM,
		Actor:       actorM,
		Context:     ctxM,
		Replay:      ledger.ReplayStatus,
		Workspaces:  workspaces,
		EventCount:  ledger.AcceptedEventCount,
		HeadEventID: ledger.HeadEventID,
		HeadHash:    ledger.HeadHash,
		Checkpoint: CheckpointInfo{
			Present:  ledger.CheckpointPresent,
			Age:      ledger.CheckpointAge,
			Sequence: ledger.CheckpointSequence,
		},
	}
	if s.Replay == "" {
		s.Replay = "unknown"
	}
	return s
}

// SortedKeys helper for deterministic JSON if needed elsewhere.
func SortedKeys(m map[string]*graph.Store) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
