package capability

import (
	"encoding/json"
	"time"
)

// ResultStatus enumerates result lifecycle.
type ResultStatus string

const (
	ResultPending   ResultStatus = "pending"
	ResultRunning   ResultStatus = "running"
	ResultCompleted ResultStatus = "completed"
	ResultFailed    ResultStatus = "failed"
	ResultRejected  ResultStatus = "rejected"
)

// Result is typed capability result. It must not imply canonical acceptance.
type Result struct {
	RequestID          string          `json:"request_id"`
	CapabilityID       string          `json:"capability_id"`
	Status             ResultStatus    `json:"status"`
	Outputs            json.RawMessage `json:"outputs,omitempty"`
	EvidenceRefs       []string        `json:"evidence_refs,omitempty"`
	StartedAt          time.Time       `json:"started_at"`
	FinishedAt         time.Time       `json:"finished_at"`
	Elapsed            time.Duration   `json:"elapsed"`
	Warnings           []string        `json:"warnings,omitempty"`
	Errors             []string        `json:"errors,omitempty"`
	ProposedTransition json.RawMessage `json:"proposed_transition,omitempty"` // optional ProposedTransition JSON
	Metadata           map[string]any  `json:"metadata,omitempty"`
}

func (r Result) Validate() error {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		// allow zero for pending but not for terminal
	}
	return nil
}

// CanonicalJSON deterministic.
func (r Result) CanonicalJSON() ([]byte, error) {
	return json.Marshal(r)
}
