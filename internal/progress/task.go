package progress

import (
	"fmt"
	"strings"
	"time"
)

// Error taxonomy codes. Underlying typed error is preserved via Cause.
const (
	ErrInvalidInput               = "invalid_input"
	ErrAdmissionRejected          = "admission_rejected"
	ErrStaleState                 = "stale_state"
	ErrDurableRecorderUnavailable = "durable_recorder_unavailable"
	ErrDurableAppendFailed        = "durable_append_failed"
	ErrActorActivationRejected    = "actor_activation_rejected"
	ErrActorFailed                = "actor_failed"
	ErrIngestRejected             = "ingest_rejected"
	ErrReplayCorrupt              = "replay_corrupt"
	ErrInternalError              = "internal_error"
)

// TaskError is the normalized error for task/receipt failures.
type TaskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *TaskError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *TaskError) Unwrap() error { return e.Cause }

// NewTaskError creates a normalized error.
func NewTaskError(code, message string, cause error) *TaskError {
	return &TaskError{Code: code, Message: message, Cause: cause}
}

// ClassifyError maps known errors to taxonomy codes. Unknown becomes internal_error.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "malformed") || strings.Contains(msg, "required"):
		return ErrInvalidInput
	case strings.Contains(msg, "durable_recorder_unavailable"):
		return ErrDurableRecorderUnavailable
	case strings.Contains(msg, "durable") && strings.Contains(msg, "append"):
		return ErrDurableAppendFailed
	case strings.Contains(msg, "stale"):
		return ErrStaleState
	case strings.Contains(msg, "rejected_budget") || strings.Contains(msg, "rejected_cycle") || strings.Contains(msg, "activation_rejected"):
		return ErrActorActivationRejected
	case strings.Contains(msg, "actor") && strings.Contains(msg, "failed"):
		return ErrActorFailed
	case strings.Contains(msg, "ingest"):
		return ErrIngestRejected
	case strings.Contains(msg, "replay") || strings.Contains(msg, "corruption") || strings.Contains(msg, "corrupt"):
		return ErrReplayCorrupt
	case strings.Contains(msg, "policy") || strings.Contains(msg, "admission"):
		return ErrAdmissionRejected
	default:
		return ErrInternalError
	}
}

// Task is the live task state. It is transient: in-flight progress does NOT
// survive restart unless the underlying durable history does. After restart
// canonical state derives from durable history; transient tasks become
// unknown/interrupted/abandoned. Task never mutates projection directly.
type Task struct {
	Packet ProgressPacket `json:"packet"`
	// internal: creation order for bounded eviction.
	createdAt time.Time `json:"-"`
}

// validTransitions enforces bounded lifecycle. Paused is defined but never
// allowed because runtime cannot honor it.
var validTransitions = map[TaskStatus]map[TaskStatus]bool{
	StatusPending: {
		StatusRunning:  true,
		StatusFailed:   true,
		StatusRejected: true,
	},
	StatusRunning: {
		StatusComplete: true,
		StatusFailed:   true,
		StatusRejected: true,
	},
}

// IsValidTransition reports whether from -> to is allowed.
// Paused is always rejected.
func IsValidTransition(from, to TaskStatus) bool {
	if to == StatusPaused {
		return false
	}
	if from == to {
		return true
	}
	if m, ok := validTransitions[from]; ok {
		return m[to]
	}
	return false
}

// ValidateTransition returns error if transition invalid.
func ValidateTransition(from, to TaskStatus) error {
	if to == StatusPaused {
		return NewTaskError(ErrInvalidInput, "pause not supported by current runtime", nil)
	}
	if IsTerminal(from) {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("terminal status %q cannot transition to %q", from, to), nil)
	}
	if !IsValidTransition(from, to) {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("invalid status transition %q -> %q", from, to), nil)
	}
	return nil
}

// boundedCopy limits warnings/errors to max 10 each, keeping most recent.
func boundedCopy(in []string, max int) []string {
	if len(in) <= max {
		out := make([]string, len(in))
		copy(out, in)
		return out
	}
	out := make([]string, max)
	copy(out, in[len(in)-max:])
	return out
}

// AddWarning appends warning bounded to 10, dropping oldest beyond limit.
func (t *Task) AddWarning(msg string) {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return
	}
	if len(t.Packet.Warnings) >= 10 {
		// drop oldest
		t.Packet.Warnings = append(t.Packet.Warnings[1:], msg)
	} else {
		t.Packet.Warnings = append(t.Packet.Warnings, msg)
	}
	t.Packet.UpdatedAt = time.Now().UTC()
}

// AddError appends error bounded to 10, dropping oldest beyond limit.
func (t *Task) AddError(msg string) {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return
	}
	if len(t.Packet.Errors) >= 10 {
		t.Packet.Errors = append(t.Packet.Errors[1:], msg)
	} else {
		t.Packet.Errors = append(t.Packet.Errors, msg)
	}
	t.Packet.UpdatedAt = time.Now().UTC()
}

// SetPhase updates phase and UpdatedAt.
func (t *Task) SetPhase(phase Phase) {
	t.Packet.Phase = phase
	t.Packet.UpdatedAt = time.Now().UTC()
}

// SetProgress updates completed/total and recomputes throughput/eta.
func (t *Task) SetProgress(completed, total *int64) {
	t.Packet.Completed = completed
	t.Packet.Total = total
	t.Packet.Throughput = t.Packet.ComputeThroughput()
	t.Packet.ETA = t.Packet.ComputeETA()
	t.Packet.UpdatedAt = time.Now().UTC()
}

// SetMetrics updates actor/graph/context/ledger metrics atomically.
func (t *Task) SetMetrics(actor ActorMetrics, graph GraphMetrics, ctx ContextMetrics, ledger LedgerMetrics) {
	t.Packet.Actor = actor
	t.Packet.Graph = graph
	t.Packet.Context = ctx
	t.Packet.Ledger = ledger
	t.Packet.UpdatedAt = time.Now().UTC()
}
