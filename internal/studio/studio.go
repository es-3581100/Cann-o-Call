// Package studio defines the read-only projection shared by the terminal and
// browser Studio surfaces. It intentionally has no server dependency.
package studio

import (
	"sort"
	"time"

	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

// ViewModel is a presentation-safe, read-only snapshot of live projections.
// It contains file metadata and tree shape, never workspace file content.
type ViewModel struct {
	GeneratedAt      time.Time              `json:"generated_at"`
	Health           Health                 `json:"health"`
	Capabilities     []Capability           `json:"capabilities"`
	Ledger           progress.LedgerMetrics `json:"ledger"`
	Actors           progress.ActorMetrics  `json:"actors"`
	Graph            progress.GraphMetrics  `json:"graph"`
	Context          ContextSummary         `json:"context"`
	Tasks            TaskData               `json:"tasks"`
	ProgressReceipts ProgressReceiptData    `json:"progress_receipts"`
	Receipts         ReceiptData            `json:"receipts"`
	Ranger           Ranger                 `json:"ranger"`
	Unavailable      []Unavailable          `json:"unavailable"`
}

type Health struct {
	Available bool   `json:"available"`
	State     string `json:"state"`
}

// Capability exposes only a registered descriptor's safe identity fields.
type Capability struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Enabled bool   `json:"enabled"`
}

type TaskData struct {
	Count   int           `json:"count"`
	Packets []TaskSummary `json:"packets"`
}

// TaskSummary is the bounded task state needed by Monitor and Ranger. It
// deliberately omits packet warnings, errors, and nested diagnostic details.
type TaskSummary struct {
	TaskID    string              `json:"task_id"`
	Operation progress.Operation  `json:"operation"`
	Phase     progress.Phase      `json:"phase"`
	Status    progress.TaskStatus `json:"status"`
	StartedAt time.Time           `json:"started_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

type ProgressReceiptData struct {
	Count   int                      `json:"count"`
	Packets []ProgressReceiptSummary `json:"packets"`
}

// ProgressReceiptSummary is the presentation-safe terminal receipt view for
// Monitor and Ranger. The source receipt remains intact in progress.Store;
// warnings, errors, result content, accepted transition IDs, and durable refs
// are intentionally not projected here.
type ProgressReceiptSummary struct {
	TaskID           string              `json:"task_id"`
	Operation        progress.Operation  `json:"operation"`
	StartedAt        time.Time           `json:"started_at"`
	FinishedAt       time.Time           `json:"finished_at"`
	Elapsed          string              `json:"elapsed"`
	FinalStatus      progress.TaskStatus `json:"final_status"`
	UnitsProcessed   int64               `json:"units_processed"`
	ActorsActivated  int                 `json:"actors_activated"`
	ActorsCompleted  int                 `json:"actors_completed"`
	ActorsFailed     int                 `json:"actors_failed"`
	ActorsSuppressed int                 `json:"actors_suppressed"`
	DurableRefCount  int                 `json:"durable_ref_count"`
	CheckpointID     string              `json:"checkpoint_id,omitempty"`
	HeadEventID      string              `json:"head_event_id,omitempty"`
	HeadSequence     int64               `json:"head_sequence,omitempty"`
	HeadHash         string              `json:"head_hash,omitempty"`
	ResultID         string              `json:"result_id,omitempty"`
	ResultHash       string              `json:"result_hash,omitempty"`
}

// ContextSummary retains aggregate selection counters without exposing an
// arbitrary score-summary string from the source progress contract.
type ContextSummary struct {
	CandidatesConsidered int     `json:"candidates_considered"`
	CandidatesSelected   int     `json:"candidates_selected"`
	SelectedCount        int     `json:"selected_count"`
	Coverage             float64 `json:"coverage"`
	ScoreMin             float64 `json:"score_min,omitempty"`
	ScoreMax             float64 `json:"score_max,omitempty"`
	ScoreAvg             float64 `json:"score_avg,omitempty"`
}

// ReceiptSummary deliberately excludes receipt Details, which may contain
// arbitrary data. It is enough for a monitor without exposing file contents.
type ReceiptSummary struct {
	ID          string    `json:"id"`
	Seq         int64     `json:"seq"`
	CreatedAt   time.Time `json:"created_at"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	Action      string    `json:"action"`
	Status      string    `json:"status"`
	EventID     string    `json:"event_id,omitempty"`
}

type ReceiptData struct {
	Count   int              `json:"count"`
	Packets []ReceiptSummary `json:"packets"`
}

// Ranger is the shared hierarchical workspace navigator projection.
type Ranger struct {
	Groups             []RangerGroup        `json:"groups"`
	InitialWorkspaceID string               `json:"initial_workspace_id,omitempty"`
	InitialTree        []workspace.TreeNode `json:"initial_tree"`
}

type RangerGroup struct {
	Category   string        `json:"category"`
	State      string        `json:"state"`
	Diagnostic string        `json:"diagnostic,omitempty"`
	Entries    []RangerEntry `json:"entries"`
}

const (
	RangerCategoryActors             = "actors"
	RangerCategoryEvidenceProvenance = "evidence_provenance"
	RangerCategoryEvents             = "events"
	RangerCategoryFiles              = "files"
	RangerCategoryGraphNodes         = "graph_nodes"
	RangerCategoryReceipts           = "receipts"
	RangerCategorySourceNodes        = "source_nodes"
	RangerCategoryTasks              = "tasks"
	RangerCategoryWorkspaces         = "workspaces"
)

// RangerMetadata contains only scalar identifiers and source metadata that is
// safe to present. It deliberately has no arbitrary map or content field.
type RangerMetadata struct {
	Path           string `json:"path,omitempty"`
	SHA256         string `json:"sha256,omitempty"`
	Size           int64  `json:"size,omitempty"`
	Format         string `json:"format,omitempty"`
	FileCount      int    `json:"file_count,omitempty"`
	DirectoryCount int    `json:"directory_count,omitempty"`
	Sequence       int64  `json:"sequence,omitempty"`
	Action         string `json:"action,omitempty"`
	Status         string `json:"status,omitempty"`
	EventType      string `json:"event_type,omitempty"`
	ReferenceID    string `json:"reference_id,omitempty"`
}

// RangerEntry is the generic, presentation-safe item used by every Ranger
// category. It never embeds workspace files, ingest text, or arbitrary maps.
type RangerEntry struct {
	ID          string         `json:"id"`
	Label       string         `json:"label"`
	Kind        string         `json:"kind"`
	Category    string         `json:"category"`
	WorkspaceID string         `json:"workspace_id,omitempty"`
	Metadata    RangerMetadata `json:"metadata,omitempty"`
}

type Unavailable struct {
	Category   string `json:"category"`
	Diagnostic string `json:"diagnostic"`
}

// Normalize sorts presentation slices, fills their counts, and makes empty
// slices JSON arrays. Collectors may call it before publishing a snapshot.
func (v *ViewModel) Normalize() {
	sort.Slice(v.Capabilities, func(i, j int) bool {
		if v.Capabilities[i].ID == v.Capabilities[j].ID {
			return v.Capabilities[i].Name < v.Capabilities[j].Name
		}
		return v.Capabilities[i].ID < v.Capabilities[j].ID
	})
	sort.Slice(v.Tasks.Packets, func(i, j int) bool { return v.Tasks.Packets[i].TaskID < v.Tasks.Packets[j].TaskID })
	v.Tasks.Count = len(v.Tasks.Packets)
	sort.Slice(v.ProgressReceipts.Packets, func(i, j int) bool {
		if v.ProgressReceipts.Packets[i].FinishedAt.Equal(v.ProgressReceipts.Packets[j].FinishedAt) {
			return v.ProgressReceipts.Packets[i].TaskID < v.ProgressReceipts.Packets[j].TaskID
		}
		return v.ProgressReceipts.Packets[i].FinishedAt.Before(v.ProgressReceipts.Packets[j].FinishedAt)
	})
	v.ProgressReceipts.Count = len(v.ProgressReceipts.Packets)
	sort.Slice(v.Receipts.Packets, func(i, j int) bool {
		if v.Receipts.Packets[i].CreatedAt.Equal(v.Receipts.Packets[j].CreatedAt) {
			return v.Receipts.Packets[i].ID < v.Receipts.Packets[j].ID
		}
		return v.Receipts.Packets[i].CreatedAt.Before(v.Receipts.Packets[j].CreatedAt)
	})
	v.Receipts.Count = len(v.Receipts.Packets)
	v.Ranger.Groups = completeRangerGroups(v.Ranger.Groups)
	sort.Slice(v.Ranger.Groups, func(i, j int) bool { return v.Ranger.Groups[i].Category < v.Ranger.Groups[j].Category })
	for i := range v.Ranger.Groups {
		sort.Slice(v.Ranger.Groups[i].Entries, func(a, b int) bool {
			return v.Ranger.Groups[i].Entries[a].ID < v.Ranger.Groups[i].Entries[b].ID
		})
		if v.Ranger.Groups[i].Entries == nil {
			v.Ranger.Groups[i].Entries = []RangerEntry{}
		}
		if len(v.Ranger.Groups[i].Entries) == 0 {
			if v.Ranger.Groups[i].State == "" || v.Ranger.Groups[i].State == "available" {
				v.Ranger.Groups[i].State = "empty"
			}
			if v.Ranger.Groups[i].Diagnostic == "" {
				v.Ranger.Groups[i].Diagnostic = "no records are currently available"
			}
		} else if v.Ranger.Groups[i].State == "" {
			v.Ranger.Groups[i].State = "available"
		}
	}
	sort.Slice(v.Unavailable, func(i, j int) bool { return v.Unavailable[i].Category < v.Unavailable[j].Category })
	if v.Capabilities == nil {
		v.Capabilities = []Capability{}
	}
	if v.Tasks.Packets == nil {
		v.Tasks.Packets = []TaskSummary{}
	}
	if v.ProgressReceipts.Packets == nil {
		v.ProgressReceipts.Packets = []ProgressReceiptSummary{}
	}
	if v.Receipts.Packets == nil {
		v.Receipts.Packets = []ReceiptSummary{}
	}
	if v.Ranger.Groups == nil {
		v.Ranger.Groups = []RangerGroup{}
	}
	if v.Ranger.InitialTree == nil {
		v.Ranger.InitialTree = []workspace.TreeNode{}
	}
	if v.Unavailable == nil {
		v.Unavailable = []Unavailable{}
	}
}

func completeRangerGroups(groups []RangerGroup) []RangerGroup {
	required := []string{
		RangerCategoryActors,
		RangerCategoryEvidenceProvenance,
		RangerCategoryEvents,
		RangerCategoryFiles,
		RangerCategoryGraphNodes,
		RangerCategoryReceipts,
		RangerCategorySourceNodes,
		RangerCategoryTasks,
		RangerCategoryWorkspaces,
	}
	byCategory := make(map[string]RangerGroup, len(groups))
	for _, group := range groups {
		if existing, ok := byCategory[group.Category]; ok {
			existing.Entries = append(existing.Entries, group.Entries...)
			if existing.State == "" {
				existing.State = group.State
			}
			if existing.Diagnostic == "" {
				existing.Diagnostic = group.Diagnostic
			}
			byCategory[group.Category] = existing
			continue
		}
		byCategory[group.Category] = group
	}
	for _, category := range required {
		if _, ok := byCategory[category]; !ok {
			byCategory[category] = RangerGroup{Category: category, State: "empty", Diagnostic: "no records are currently available", Entries: []RangerEntry{}}
		}
	}
	out := make([]RangerGroup, 0, len(byCategory))
	for _, group := range byCategory {
		out = append(out, group)
	}
	return out
}

func ReceiptSummaries(items []receipts.Receipt) []ReceiptSummary {
	out := make([]ReceiptSummary, 0, len(items))
	for _, item := range items {
		out = append(out, ReceiptSummary{ID: item.ID, Seq: item.Seq, CreatedAt: item.CreatedAt, WorkspaceID: item.WorkspaceID, Action: item.Action, Status: item.Status, EventID: item.EventID})
	}
	return out
}

// TaskSummaries reduces live progress packets to presentation-safe task state.
func TaskSummaries(items []progress.ProgressPacket) []TaskSummary {
	out := make([]TaskSummary, 0, len(items))
	for _, item := range items {
		out = append(out, TaskSummary{
			TaskID: item.TaskID, Operation: item.Operation, Phase: item.Phase,
			Status: item.Status, StartedAt: item.StartedAt, UpdatedAt: item.UpdatedAt,
		})
	}
	return out
}

// ProgressReceiptSummaries reduces stored terminal receipts to the bounded
// metadata required by Studio without changing stored receipts or other APIs.
func ProgressReceiptSummaries(items []progress.ReceiptPacket) []ProgressReceiptSummary {
	out := make([]ProgressReceiptSummary, 0, len(items))
	for _, item := range items {
		out = append(out, ProgressReceiptSummary{
			TaskID: item.TaskID, Operation: item.Operation, StartedAt: item.StartedAt,
			FinishedAt: item.FinishedAt, Elapsed: item.Elapsed, FinalStatus: item.FinalStatus,
			UnitsProcessed: item.UnitsProcessed, ActorsActivated: item.ActorsActivated,
			ActorsCompleted: item.ActorsCompleted, ActorsFailed: item.ActorsFailed,
			ActorsSuppressed: item.ActorsSuppressed, DurableRefCount: len(item.DurableRefs),
			CheckpointID: item.CheckpointID, HeadEventID: item.HeadEventID,
			HeadSequence: item.HeadSequence, HeadHash: item.HeadHash, ResultID: item.ResultID,
			ResultHash: item.ResultHash,
		})
	}
	return out
}

// ContextSummaryFromMetrics reduces progress context metrics for Studio.
func ContextSummaryFromMetrics(item progress.ContextMetrics) ContextSummary {
	return ContextSummary{
		CandidatesConsidered: item.CandidatesConsidered,
		CandidatesSelected:   item.CandidatesSelected,
		SelectedCount:        item.SelectedCount,
		Coverage:             item.Coverage,
		ScoreMin:             item.ScoreMin,
		ScoreMax:             item.ScoreMax,
		ScoreAvg:             item.ScoreAvg,
	}
}
