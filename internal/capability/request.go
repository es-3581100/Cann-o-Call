package capability

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Request is the typed capability request. Validate before execution, reject unknown IDs fail-closed.
type Request struct {
	RequestID          string          `json:"request_id"`
	CapabilityID       string          `json:"capability_id"`
	WorkspaceID        string          `json:"workspace_id"`
	ActorID            string          `json:"actor_id,omitempty"`
	LineageID          string          `json:"lineage_id,omitempty"`
	Inputs             json.RawMessage `json:"inputs"`
	EvidenceRefs       []string        `json:"evidence_refs,omitempty"`
	RequestedOperation string          `json:"requested_operation,omitempty"`
	Timestamp          time.Time       `json:"timestamp"`
	Version            string          `json:"version,omitempty"`
}

func (r Request) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return NewError(ErrInvalidInput, "request_id required", nil)
	}
	if strings.TrimSpace(r.CapabilityID) == "" {
		return NewError(ErrInvalidInput, "capability_id required", nil)
	}
	if strings.TrimSpace(r.WorkspaceID) == "" {
		return NewError(ErrInvalidInput, "workspace_id required", nil)
	}
	if r.Timestamp.IsZero() {
		return NewError(ErrInvalidInput, "timestamp required", nil)
	}
	if r.Inputs != nil && len(r.Inputs) > 0 {
		// must be valid JSON if present
		var v any
		if err := json.Unmarshal(r.Inputs, &v); err != nil {
			return NewError(ErrInvalidInput, fmt.Sprintf("inputs malformed: %v", err), err)
		}
	}
	return nil
}

// CanonicalJSON for determinism uses sorted keys via Marshal of struct (field order fixed).
func (r Request) CanonicalJSON() ([]byte, error) {
	return json.Marshal(r)
}
