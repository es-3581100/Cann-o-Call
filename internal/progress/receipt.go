package progress

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// DurableRef is the Rust-bound durable acknowledgement evidence. Receipt must
// NOT claim durable acceptance without Rust-bound evidence.
type DurableRef struct {
	EventID     string `json:"event_id"`
	Sequence    int64  `json:"sequence"`
	EventHash   string `json:"event_hash"`
	RustAckID   string `json:"rust_ack_id,omitempty"`
	RustAckSeq  int64  `json:"rust_ack_seq,omitempty"`
	RustAckHash string `json:"rust_ack_hash,omitempty"`
}

// Validate checks Rust binding invariants.
func (d DurableRef) Validate() error {
	if strings.TrimSpace(d.EventID) == "" {
		return fmt.Errorf("event_id required")
	}
	if d.Sequence <= 0 {
		return fmt.Errorf("sequence must be >0")
	}
	if !isSHA256(d.EventHash) {
		return fmt.Errorf("event_hash must be sha256 hex")
	}
	// Rust ACK is required for durable acceptance; it binds the same identity.
	if strings.TrimSpace(d.RustAckID) == "" || d.RustAckSeq <= 0 || !isSHA256(d.RustAckHash) {
		return fmt.Errorf("rust acknowledgement required and must be valid sha256")
	}
	if d.RustAckID != d.EventID {
		return fmt.Errorf("rust ack id %q does not match event_id %q", d.RustAckID, d.EventID)
	}
	if d.RustAckSeq != d.Sequence {
		return fmt.Errorf("rust ack seq %d does not match event seq %d", d.RustAckSeq, d.Sequence)
	}
	return nil
}

// ReceiptPacket is the distinct typed final terminal result persisted as artifact.
// It is separate from existing receipts.Service Receipt (hash-chained receipts);
// it never claims durable acceptance without Rust-bound evidence.
type ReceiptPacket struct {
	TaskID                string       `json:"task_id"`
	Operation             Operation    `json:"operation"`
	StartedAt             time.Time    `json:"started_at"`
	FinishedAt            time.Time    `json:"finished_at"`
	Elapsed               string       `json:"elapsed"`
	FinalStatus           TaskStatus   `json:"final_status"`
	UnitsProcessed        int64        `json:"units_processed"`
	ActorsActivated       int          `json:"actors_activated"`
	ActorsCompleted       int          `json:"actors_completed"`
	ActorsFailed          int          `json:"actors_failed"`
	ActorsSuppressed      int          `json:"actors_suppressed"`
	AcceptedTransitionIDs []string     `json:"accepted_transition_ids,omitempty"`
	DurableRefs           []DurableRef `json:"durable_refs,omitempty"`
	CheckpointID          string       `json:"checkpoint_id,omitempty"`
	HeadEventID           string       `json:"head_event_id,omitempty"`
	HeadSequence          int64        `json:"head_sequence,omitempty"`
	HeadHash              string       `json:"head_hash,omitempty"`
	ResultID              string       `json:"result_id,omitempty"`
	ResultHash            string       `json:"result_hash,omitempty"`
	Warnings              []string     `json:"warnings,omitempty"`
	Errors                []string     `json:"errors,omitempty"`
}

// Validate checks receipt invariants, including durable binding.
func (r ReceiptPacket) Validate() error {
	if strings.TrimSpace(r.TaskID) == "" {
		return fmt.Errorf("task_id required")
	}
	if strings.TrimSpace(string(r.Operation)) == "" {
		return fmt.Errorf("operation required")
	}
	if r.StartedAt.IsZero() {
		return fmt.Errorf("started_at required")
	}
	if r.FinishedAt.IsZero() {
		return fmt.Errorf("finished_at required")
	}
	if r.FinishedAt.Before(r.StartedAt) {
		return fmt.Errorf("finished_at before started_at")
	}
	switch r.FinalStatus {
	case StatusComplete, StatusFailed, StatusRejected:
	default:
		return fmt.Errorf("final_status must be terminal complete/failed/rejected, got %q", r.FinalStatus)
	}
	if len(r.Warnings) > 10 {
		return fmt.Errorf("warnings exceeds bound 10")
	}
	if len(r.Errors) > 10 {
		return fmt.Errorf("errors exceeds bound 10")
	}
	// Durable binding: if accepted transitions claimed, durable refs required and must validate.
	if len(r.AcceptedTransitionIDs) > 0 {
		if len(r.DurableRefs) == 0 {
			return fmt.Errorf("receipt claims accepted transitions but has no durable refs; must not claim durable acceptance without Rust-bound evidence")
		}
		if len(r.AcceptedTransitionIDs) != len(r.DurableRefs) {
			return fmt.Errorf("accepted_transition_ids count %d does not match durable_refs count %d", len(r.AcceptedTransitionIDs), len(r.DurableRefs))
		}
		seen := map[string]bool{}
		for i, id := range r.AcceptedTransitionIDs {
			if strings.TrimSpace(id) == "" {
				return fmt.Errorf("accepted_transition_ids[%d] empty", i)
			}
			if seen[id] {
				return fmt.Errorf("duplicate accepted_transition_id %q", id)
			}
			seen[id] = true
		}
		for i, d := range r.DurableRefs {
			if err := d.Validate(); err != nil {
				return fmt.Errorf("durable_refs[%d]: %w", i, err)
			}
			if d.EventID != r.AcceptedTransitionIDs[i] {
				// Allow any order? Enforce correspondence by sorting check.
				// For strict binding, require sorted order match or existence.
				found := false
				for _, aid := range r.AcceptedTransitionIDs {
					if aid == d.EventID {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("durable_refs[%d] event_id %q not in accepted_transition_ids", i, d.EventID)
				}
			}
		}
	}
	// No COMPLETE while durable failed: if final is complete but durable refs invalid, already caught.
	// Also if durable refs present but final is rejected/failed, that's allowed (durable failure -> FAILED/REJECTED)
	if r.HeadHash != "" && !isSHA256(r.HeadHash) && r.HeadHash != "genesis" {
		return fmt.Errorf("head_hash must be sha256 or genesis")
	}
	if r.ResultHash != "" && !isSHA256(r.ResultHash) {
		return fmt.Errorf("result_hash must be sha256")
	}
	return nil
}

// BuildReceiptFromTask constructs a receipt from terminal task and provided
// durable evidence. It enforces that COMPLETE is not allowed while durable
// failed: caller must pass finalStatus that reflects durable outcome.
func BuildReceiptFromTask(t *Task, finalStatus TaskStatus, units int64, acceptedIDs []string, durableRefs []DurableRef, resultID, resultHash string) (ReceiptPacket, error) {
	if t == nil {
		return ReceiptPacket{}, fmt.Errorf("task is nil")
	}
	switch finalStatus {
	case StatusComplete, StatusFailed, StatusRejected:
	default:
		return ReceiptPacket{}, fmt.Errorf("final_status must be terminal")
	}
	// Durable failure must not be COMPLETE: if durable refs claim acceptance but
	// any ref invalid, final cannot be complete. Validate refs first.
	for i, d := range durableRefs {
		if err := d.Validate(); err != nil {
			if finalStatus == StatusComplete {
				return ReceiptPacket{}, fmt.Errorf("durable_refs[%d] invalid but final_status is complete: %w", i, err)
			}
		}
	}
	if len(acceptedIDs) > 0 && len(durableRefs) == 0 && finalStatus == StatusComplete {
		return ReceiptPacket{}, fmt.Errorf("cannot be complete while durable refs missing for accepted transitions")
	}
	finished := time.Now().UTC()
	if !t.Packet.UpdatedAt.IsZero() {
		finished = t.Packet.UpdatedAt
		if finished.Before(t.Packet.StartedAt) {
			finished = time.Now().UTC()
		}
	}
	// Copy and sort ids/refs for determinism.
	ids := append([]string(nil), acceptedIDs...)
	sort.Strings(ids)
	refs := append([]DurableRef(nil), durableRefs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].EventID < refs[j].EventID })

	// Derive actor counts from packet actor metrics.
	activated := t.Packet.Actor.Active + t.Packet.Actor.Completed + t.Packet.Actor.Failed
	// If queued >0 include? For receipt keep activated as completed+failed+active observed.

	rp := ReceiptPacket{
		TaskID:                t.Packet.TaskID,
		Operation:             t.Packet.Operation,
		StartedAt:             t.Packet.StartedAt,
		FinishedAt:            finished,
		Elapsed:               finished.Sub(t.Packet.StartedAt).Truncate(time.Millisecond).String(),
		FinalStatus:           finalStatus,
		UnitsProcessed:        units,
		ActorsActivated:       activated,
		ActorsCompleted:       t.Packet.Actor.Completed,
		ActorsFailed:          t.Packet.Actor.Failed,
		ActorsSuppressed:      t.Packet.Actor.Suppressed + t.Packet.Actor.Rejected,
		AcceptedTransitionIDs: ids,
		DurableRefs:           refs,
		HeadEventID:           t.Packet.Ledger.HeadEventID,
		HeadSequence:          t.Packet.Ledger.CurrentSequence,
		HeadHash:              t.Packet.Ledger.HeadHash,
		ResultID:              resultID,
		ResultHash:            resultHash,
		Warnings:              boundedCopy(t.Packet.Warnings, 10),
		Errors:                boundedCopy(t.Packet.Errors, 10),
	}
	if err := rp.Validate(); err != nil {
		return ReceiptPacket{}, err
	}
	return rp, nil
}

// Hash returns deterministic sha256 of canonical receipt (without hash field).
func (r ReceiptPacket) Hash() string {
	b, _ := json.Marshal(r)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Store persists ReceiptPackets as typed artifacts under dir/receipts/<task_id>.json.
// It does not break existing receipts.Service; this is distinct.
type Store struct {
	mu  sync.Mutex
	dir string
}

// NewStore creates store at dir (creates if needed). dir may be empty for in-memory only.
func NewStore(dir string) (*Store, error) {
	if strings.TrimSpace(dir) != "" {
		if err := os.MkdirAll(filepath.Join(dir, "receipts"), 0o755); err != nil {
			return nil, fmt.Errorf("create receipt store: %w", err)
		}
	}
	return &Store{dir: strings.TrimSpace(dir)}, nil
}

// Save persists receipt atomically. Returns error if receipt invalid or durable
// claim lacks Rust evidence.
func (s *Store) Save(r ReceiptPacket) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if s.dir == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, "receipts", r.TaskID+".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load returns receipt by taskID.
func (s *Store) Load(taskID string) (ReceiptPacket, bool) {
	if s.dir == "" {
		return ReceiptPacket{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, "receipts", taskID+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		return ReceiptPacket{}, false
	}
	var r ReceiptPacket
	if err := json.Unmarshal(b, &r); err != nil {
		return ReceiptPacket{}, false
	}
	return r, true
}

// List returns all receipts sorted by FinishedAt ascending.
func (s *Store) List() []ReceiptPacket {
	if s.dir == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.dir, "receipts"))
	if err != nil {
		return nil
	}
	var out []ReceiptPacket
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.dir, "receipts", e.Name()))
		if err != nil {
			continue
		}
		var r ReceiptPacket
		if err := json.Unmarshal(b, &r); err != nil {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FinishedAt.Equal(out[j].FinishedAt) {
			return out[i].TaskID < out[j].TaskID
		}
		return out[i].FinishedAt.Before(out[j].FinishedAt)
	})
	return out
}
