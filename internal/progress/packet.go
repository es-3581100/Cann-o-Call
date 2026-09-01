package progress

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Operation is the stable identity for each long-running operation.
// Tiny sync ops should not be forced into this machinery; use only for
// ingest, query/orchestration, actor execution, mutation proposal,
// replay/rebuild, snapshot/checkpoint.
type Operation string

const (
	OperationIngest           Operation = "ingest"
	OperationQuery            Operation = "query"
	OperationOrchestration    Operation = "orchestration"
	OperationActorExecution   Operation = "actor_execution"
	OperationMutationProposal Operation = "mutation_proposal"
	OperationReplay           Operation = "replay"
	OperationRebuild          Operation = "rebuild"
	OperationSnapshot         Operation = "snapshot"
	OperationCheckpoint       Operation = "checkpoint"
)

// Phase is the observable step within an operation. It exposes system
// events/state only, not hidden model reasoning. For CHUNK-07 query flow
// the ordered phases are: request_accepted -> graph_nodes / candidates
// considered -> scoring_complete -> candidates_selected -> actor activations
// accepted/suppressed/rejected -> actors_completed -> results_produced ->
// proposals_attempted -> transitions accepted/rejected -> rust ack -> terminal.
type Phase string

const (
	PhaseRequestAccepted      Phase = "request_accepted"
	PhaseGraphLoaded          Phase = "graph_loaded"
	PhaseGraphNodes           Phase = "graph_nodes"
	PhaseCandidatesConsidered Phase = "candidates_considered"
	PhaseScoringComplete      Phase = "scoring_complete"
	PhaseCandidatesSelected   Phase = "candidates_selected"
	PhaseActorActivations     Phase = "actor_activations"
	PhaseActorsCompleted      Phase = "actors_completed"
	PhaseResultsProduced      Phase = "results_produced"
	PhaseProposalsAttempted   Phase = "proposals_attempted"
	PhaseTransitionsAccepted  Phase = "transitions_accepted"
	PhaseTransitionsRejected  Phase = "transitions_rejected"
	PhaseRustACK              Phase = "rust_ack_received"
	PhaseTerminal             Phase = "terminal"

	PhaseValidated         Phase = "validated"
	PhaseAdmissionPending  Phase = "admission_pending"
	PhaseDurableAppend     Phase = "durable_append"
	PhaseProjectionApplied Phase = "projection_applied"

	PhasePending Phase = "pending"
	PhaseRunning Phase = "running"
)

// TaskStatus is the bounded lifecycle status. Paused is defined but
// transitions to paused are rejected unless the runtime can honor it; the
// current dormant Proto.Actor runtime does not support pause/resume so
// paused transitions fail-closed.
type TaskStatus string

const (
	StatusPending  TaskStatus = "pending"
	StatusRunning  TaskStatus = "running"
	StatusPaused   TaskStatus = "paused"
	StatusComplete TaskStatus = "complete"
	StatusFailed   TaskStatus = "failed"
	StatusRejected TaskStatus = "rejected"
)

// IsTerminal reports whether status is terminal.
func IsTerminal(s TaskStatus) bool {
	return s == StatusComplete || s == StatusFailed || s == StatusRejected
}

// ActorMetrics captures bounded actor observability without exposing payloads.
// It respects CHUNK-04 ephemeral limitation: descriptors are in-memory only and
// do not survive restart.
type ActorMetrics struct {
	DormantDescriptors int `json:"dormant_descriptors"`
	Queued             int `json:"queued"`
	Active             int `json:"active"`
	Completed          int `json:"completed"`
	Failed             int `json:"failed"`
	Expired            int `json:"expired"`
	Passivated         int `json:"passivated"`
	Suppressed         int `json:"suppressed"`
	Rejected           int `json:"rejected"`
}

// GraphMetrics is bounded graph observability without exposing payloads.
type GraphMetrics struct {
	NodeCount   int `json:"node_count"`
	EdgeCount   int `json:"edge_count"`
	SourceCount int `json:"source_count"`
}

// ContextMetrics is bounded context/scoring observability.
type ContextMetrics struct {
	CandidatesConsidered int     `json:"candidates_considered"`
	CandidatesSelected   int     `json:"candidates_selected"`
	SelectedCount        int     `json:"selected_count"`
	Coverage             float64 `json:"coverage"`
	ScoreMin             float64 `json:"score_min,omitempty"`
	ScoreMax             float64 `json:"score_max,omitempty"`
	ScoreAvg             float64 `json:"score_avg,omitempty"`
	ScoreSummaries       string  `json:"score_summaries,omitempty"`
}

// LedgerMetrics is safe derived ledger observability. It does not turn metrics
// into a second ledger: accepted counts and heads are snapshots, not authority.
type LedgerMetrics struct {
	AcceptedEventCount int64   `json:"accepted_event_count"`
	CurrentSequence    int64   `json:"current_sequence"`
	HeadEventID        string  `json:"head_event_id,omitempty"`
	HeadHash           string  `json:"head_hash,omitempty"`
	VerificationStatus string  `json:"verification_status,omitempty"`
	CheckpointPresent  bool    `json:"checkpoint_present"`
	CheckpointAge      *string `json:"checkpoint_age,omitempty"`
	CheckpointSequence *int64  `json:"checkpoint_sequence,omitempty"`
	ReplayStatus       string  `json:"replay_status,omitempty"`
}

// ProgressPacket is the single typed bounded progress contract. It is
// observation-only: it never mutates projection, authorizes transitions,
// fakes Rust ACK, extends budget/depth, or bypasses admission.
type ProgressPacket struct {
	TaskID     string         `json:"task_id"`
	Operation  Operation      `json:"operation"`
	Phase      Phase          `json:"phase"`
	Status     TaskStatus     `json:"status"`
	Completed  *int64         `json:"completed,omitempty"`
	Total      *int64         `json:"total,omitempty"`
	StartedAt  time.Time      `json:"started_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	Throughput *float64       `json:"throughput,omitempty"`
	ETA        *string        `json:"eta,omitempty"`
	Actor      ActorMetrics   `json:"actor"`
	Graph      GraphMetrics   `json:"graph"`
	Context    ContextMetrics `json:"context"`
	Ledger     LedgerMetrics  `json:"ledger"`
	Warnings   []string       `json:"warnings,omitempty"`
	Errors     []string       `json:"errors,omitempty"`
}

// Elapsed returns duration since StartedAt.
func (p ProgressPacket) Elapsed() time.Duration {
	if p.StartedAt.IsZero() {
		return 0
	}
	end := p.UpdatedAt
	if end.IsZero() {
		end = time.Now().UTC()
	}
	return end.Sub(p.StartedAt)
}

// ComputeThroughput returns units per second if completed and elapsed are known.
func (p ProgressPacket) ComputeThroughput() *float64 {
	if p.Completed == nil {
		return nil
	}
	elapsed := p.Elapsed().Seconds()
	if elapsed <= 0 {
		return nil
	}
	v := float64(*p.Completed) / elapsed
	return &v
}

// ComputeETA returns estimated time to complete remaining units if throughput known.
func (p ProgressPacket) ComputeETA() *string {
	if p.Completed == nil || p.Total == nil {
		return nil
	}
	if *p.Total <= *p.Completed {
		s := "0s"
		return &s
	}
	tp := p.ComputeThroughput()
	if tp == nil || *tp <= 0 {
		return nil
	}
	remaining := float64(*p.Total - *p.Completed)
	secs := remaining / *tp
	d := time.Duration(secs * float64(time.Second))
	s := d.Truncate(time.Millisecond).String()
	return &s
}

// Validate checks bounded invariants.
func (p ProgressPacket) Validate() error {
	if strings.TrimSpace(p.TaskID) == "" {
		return fmt.Errorf("task_id required")
	}
	if strings.TrimSpace(string(p.Operation)) == "" {
		return fmt.Errorf("operation required")
	}
	if strings.TrimSpace(string(p.Status)) == "" {
		return fmt.Errorf("status required")
	}
	switch p.Status {
	case StatusPending, StatusRunning, StatusPaused, StatusComplete, StatusFailed, StatusRejected:
	default:
		return fmt.Errorf("unknown status %q", p.Status)
	}
	if p.StartedAt.IsZero() {
		return fmt.Errorf("started_at required")
	}
	if p.UpdatedAt.IsZero() {
		return fmt.Errorf("updated_at required")
	}
	if p.Completed != nil && *p.Completed < 0 {
		return fmt.Errorf("completed must be >=0")
	}
	if p.Total != nil && *p.Total < 0 {
		return fmt.Errorf("total must be >=0")
	}
	if p.Completed != nil && p.Total != nil && *p.Completed > *p.Total {
		return fmt.Errorf("completed %d exceeds total %d", *p.Completed, *p.Total)
	}
	if len(p.Warnings) > 10 {
		return fmt.Errorf("warnings exceeds bound 10: %d", len(p.Warnings))
	}
	if len(p.Errors) > 10 {
		return fmt.Errorf("errors exceeds bound 10: %d", len(p.Errors))
	}
	if p.Context.Coverage < 0 || p.Context.Coverage > 1 {
		if p.Context.Coverage != 0 {
			return fmt.Errorf("coverage must be 0..1")
		}
	}
	return nil
}

// DeterministicTaskID derives a stable task identity from operation + workspace + key.
// Uses sha256 hex; deterministic where inputs deterministic (ingest/query).
func DeterministicTaskID(op Operation, workspaceID, key string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	key = strings.TrimSpace(key)
	payload := strings.Join([]string{string(op), workspaceID, key}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// DeterministicTaskIDMulti derives stable ID from operation + multiple keys joined.
func DeterministicTaskIDMulti(op Operation, workspaceID string, keys ...string) string {
	parts := make([]string, 0, len(keys)+2)
	parts = append(parts, string(op), workspaceID)
	for _, k := range keys {
		parts = append(parts, strings.TrimSpace(k))
	}
	payload := strings.Join(parts, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
