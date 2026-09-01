// Package transition owns admission and projection of semantic transitions.
// The event log (and its Rust sidecar) is deliberately only a durable opaque
// recorder; neither is consulted when making a policy decision.
package transition

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"flatten-workspace/internal/eventlog"
)

const acceptedEventType = "transition.authority.accepted"

type RejectClass string

const (
	Malformed        RejectClass = "malformed"
	Invalid          RejectClass = "invalid"
	Stale            RejectClass = "stale"
	Policy           RejectClass = "policy"
	Durable          RejectClass = "durable"
	Duplicate        RejectClass = "duplicate"
	Conflict         RejectClass = "conflict"
	ReplayCorruption RejectClass = "replay_corruption"
)

type Rejection struct {
	Class   RejectClass     `json:"class"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (r *Rejection) Error() string { return string(r.Class) + ": " + r.Message }

func reject(class RejectClass, format string, args ...any) error {
	return &Rejection{Class: class, Message: fmt.Sprintf(format, args...), Data: json.RawMessage(`{}`)}
}

func Class(err error) RejectClass {
	var r *Rejection
	if errors.As(err, &r) {
		return r.Class
	}
	return ""
}

// StateRef is a versioned semantic predecessor, never a storage sequence.
type StateRef struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

// ProposedTransition is the complete typed request. ResultData and
// AdmissionData are JSON values rather than maps so their canonical form is
// explicit and no map iteration participates in semantic hashing.
type ProposedTransition struct {
	TransitionID  string          `json:"transition_id"`
	ProposalID    string          `json:"proposal_id"`
	RequestID     string          `json:"request_id"`
	Prior         StateRef        `json:"prior"`
	Operation     string          `json:"operation"`
	Entity        string          `json:"entity"`
	Node          string          `json:"node"`
	ResultData    json.RawMessage `json:"result_data"`
	AdmissionData json.RawMessage `json:"admission_data,omitempty"`
}

type DurableBinding struct {
	EventID   string                        `json:"event_id"`
	Sequence  int64                         `json:"sequence"`
	EventHash string                        `json:"event_hash"`
	RustAck   *eventlog.RustAcknowledgement `json:"rust_ack,omitempty"`
}

type AcceptedTransition struct {
	Proposal  ProposedTransition `json:"proposal"`
	Result    StateRef           `json:"result"`
	Admission json.RawMessage    `json:"admission_data"`
	Durable   DurableBinding     `json:"durable"`
	Duplicate bool               `json:"duplicate,omitempty"`
}

type NodeState struct {
	Entity string          `json:"entity"`
	Node   string          `json:"node"`
	Data   json.RawMessage `json:"data"`
}

// Projection's Nodes are permanently ordered by (entity, node).
type Projection struct {
	ID      string      `json:"id"`
	Version uint64      `json:"version"`
	Nodes   []NodeState `json:"nodes"`
	Hash    string      `json:"hash"`
}

func Initial() Projection {
	p := Projection{ID: "semantic-state", Nodes: []NodeState{}}
	p.Hash = hashProjection(p)
	return p
}

func (p Projection) Ref() StateRef { return StateRef{ID: p.ID, Version: p.Version, Hash: p.Hash} }

type DurableWriter interface {
	Append(eventlog.Event) (eventlog.Event, error)
}

// PolicyFunc is a Go-only policy hook. A non-nil error is a policy denial.
type PolicyFunc func(ProposedTransition, Projection) error

type Authority struct {
	mu       sync.Mutex
	writer   DurableWriter
	policy   PolicyFunc
	state    Projection
	accepted map[string]acceptedRecord
}

type acceptedRecord struct {
	request  []byte
	accepted AcceptedTransition
}

func New(writer DurableWriter, policy PolicyFunc) *Authority {
	return &Authority{writer: writer, policy: policy, state: Initial(), accepted: map[string]acceptedRecord{}}
}

func (a *Authority) Projection() Projection {
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneProjection(a.state)
}

// Propose validates in Go, appends a Go-produced opaque accepted event, and
// advances the projection only after Append acknowledges success.
func (a *Authority) Propose(p ProposedTransition) (AcceptedTransition, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	request, normalized, err := canonicalProposal(p)
	if err != nil {
		return AcceptedTransition{}, err
	}
	if prior, ok := a.accepted[normalized.TransitionID]; ok {
		if bytes.Equal(prior.request, request) {
			duplicate := prior.accepted
			duplicate.Duplicate = true
			return duplicate, nil
		}
		return AcceptedTransition{}, reject(Conflict, "transition id %q was used by a different request", normalized.TransitionID)
	}
	if normalized.Prior != a.state.Ref() {
		return AcceptedTransition{}, reject(Stale, "predecessor is not current semantic state")
	}
	if a.policy != nil {
		if err := a.policy(normalized, cloneProjection(a.state)); err != nil {
			return AcceptedTransition{}, reject(Policy, "%v", err)
		}
	}

	next := apply(a.state, normalized)
	accepted := AcceptedTransition{
		Proposal:  normalized,
		Result:    next.Ref(),
		Admission: cloneRaw(normalized.AdmissionData),
	}
	payload, err := canonicalAccepted(accepted)
	if err != nil { // impossible after canonicalProposal; preserve a typed boundary.
		return AcceptedTransition{}, reject(Malformed, "encode accepted transition: %v", err)
	}
	saved, err := a.writer.Append(eventlog.Event{
		ID:          normalized.TransitionID,
		Type:        acceptedEventType,
		WorkspaceID: normalized.Entity,
		Action:      "transition.accepted",
		Status:      "accepted",
		Details:     map[string]any{"accepted_transition": json.RawMessage(payload)},
	})
	if err != nil {
		return AcceptedTransition{}, reject(Durable, "append accepted transition: %v", err)
	}
	if saved.ID != normalized.TransitionID {
		return AcceptedTransition{}, reject(Durable, "append acknowledgement bound unexpected event id %q", saved.ID)
	}
	accepted.Durable = durableBinding(saved)
	a.state = next
	a.accepted[normalized.TransitionID] = acceptedRecord{request: request, accepted: accepted}
	return accepted, nil
}

func durableBinding(saved eventlog.Event) DurableBinding {
	binding := DurableBinding{EventID: saved.ID, Sequence: saved.Seq, EventHash: saved.Hash}
	if saved.RustAck != nil {
		ack := *saved.RustAck
		binding.RustAck = &ack
	}
	return binding
}

// Restore and Import are deliberately proposal-only paths: neither bypasses
// validation, durable acknowledgement, or projection admission.
func (a *Authority) Restore(p ProposedTransition) (AcceptedTransition, error) {
	p.Operation = "restore"
	return a.Propose(p)
}

func (a *Authority) Import(p ProposedTransition) (AcceptedTransition, error) {
	p.Operation = "import"
	return a.Propose(p)
}

func canonicalProposal(p ProposedTransition) ([]byte, ProposedTransition, error) {
	if strings.TrimSpace(p.TransitionID) == "" || strings.TrimSpace(p.ProposalID) == "" || strings.TrimSpace(p.RequestID) == "" || strings.TrimSpace(p.Entity) == "" || strings.TrimSpace(p.Node) == "" || strings.TrimSpace(p.Prior.ID) == "" || strings.TrimSpace(p.Prior.Hash) == "" {
		return nil, p, reject(Malformed, "transition, proposal, request, predecessor, entity, and node identities are required")
	}
	if p.Operation != "upsert" && p.Operation != "restore" && p.Operation != "import" {
		return nil, p, reject(Invalid, "unsupported operation %q", p.Operation)
	}
	var err error
	if p.ResultData, err = canonicalJSON(p.ResultData); err != nil {
		return nil, p, reject(Malformed, "result data: %v", err)
	}
	if len(p.AdmissionData) == 0 {
		p.AdmissionData = json.RawMessage(`{}`)
	} else if p.AdmissionData, err = canonicalJSON(p.AdmissionData); err != nil {
		return nil, p, reject(Malformed, "admission data: %v", err)
	}
	b, err := canonicalJSON(mustJSON(p))
	if err != nil {
		return nil, p, reject(Malformed, "canonical proposal: %v", err)
	}
	return b, p, nil
}

func apply(current Projection, p ProposedTransition) Projection {
	next := cloneProjection(current)
	replaced := false
	for i := range next.Nodes {
		if next.Nodes[i].Entity == p.Entity && next.Nodes[i].Node == p.Node {
			next.Nodes[i].Data = cloneRaw(p.ResultData)
			replaced = true
			break
		}
	}
	if !replaced {
		next.Nodes = append(next.Nodes, NodeState{Entity: p.Entity, Node: p.Node, Data: cloneRaw(p.ResultData)})
	}
	sort.Slice(next.Nodes, func(i, j int) bool {
		if next.Nodes[i].Entity == next.Nodes[j].Entity {
			return next.Nodes[i].Node < next.Nodes[j].Node
		}
		return next.Nodes[i].Entity < next.Nodes[j].Entity
	})
	next.Version++
	next.Hash = hashProjection(next)
	return next
}

func hashProjection(p Projection) string {
	clone := cloneProjection(p)
	clone.Hash = ""
	b, err := canonicalJSON(mustJSON(clone))
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func cloneProjection(p Projection) Projection {
	p.Nodes = append([]NodeState(nil), p.Nodes...)
	for i := range p.Nodes {
		p.Nodes[i].Data = cloneRaw(p.Nodes[i].Data)
	}
	return p
}

func cloneRaw(b json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), b...) }

func canonicalAccepted(a AcceptedTransition) ([]byte, error) { return canonicalJSON(mustJSON(a)) }

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// canonicalJSON recursively sorts object keys. Semantic hashes are computed
// from this representation, never from Go map traversal or map formatting.
func canonicalJSON(raw []byte) (json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	var out bytes.Buffer
	if err := writeCanonical(&out, v); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeCanonical(out *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if x {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		b, _ := json.Marshal(x)
		out.Write(b)
	case json.Number:
		if _, err := json.Marshal(x); err != nil {
			return err
		}
		out.WriteString(x.String())
	case []any:
		out.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeCanonical(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			b, _ := json.Marshal(key)
			out.Write(b)
			out.WriteByte(':')
			if err := writeCanonical(out, x[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JSON value %T", v)
	}
	return nil
}
