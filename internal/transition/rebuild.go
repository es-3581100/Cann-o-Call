package transition

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"flatten-workspace/internal/eventlog"
)

// NewFromEventLog is the safe restart path: it rejects malformed and
// hash-chain-corrupt durable history before deriving any semantic projection.
func NewFromEventLog(log *eventlog.Service, policy PolicyFunc) (*Authority, error) {
	events, err := log.RustAcknowledgedEvents()
	if err != nil {
		return nil, reject(ReplayCorruption, "read Rust-acknowledged durable history: %v", err)
	}
	return Rebuild(events, log, policy)
}

// Rebuild derives state only from accepted events after validating the complete
// supplied history. Arbitrary history must never rebuild past corruption.
func Rebuild(events []eventlog.Event, writer DurableWriter, policy PolicyFunc) (*Authority, error) {
	if err := eventlog.ValidateHistory(events); err != nil {
		return nil, reject(ReplayCorruption, "checked durable history: %v", err)
	}
	a := New(writer, policy)
	for _, ev := range events {
		if err := a.replay(ev); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Authority) replay(ev eventlog.Event) error {
	if ev.Type != acceptedEventType {
		return nil
	}
	if ev.Status != "accepted" || ev.Action != "transition.accepted" || ev.ID == "" || len(ev.Details) != 1 {
		return reject(ReplayCorruption, "accepted event %q has an invalid durable envelope", ev.ID)
	}
	detail, ok := ev.Details["accepted_transition"]
	if !ok {
		return reject(ReplayCorruption, "accepted event %q has no transition payload", ev.ID)
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return reject(ReplayCorruption, "accepted event %q payload: %v", ev.ID, err)
	}
	var accepted AcceptedTransition
	if err := json.Unmarshal(raw, &accepted); err != nil {
		return reject(ReplayCorruption, "accepted event %q payload: %v", ev.ID, err)
	}
	request, proposal, err := canonicalProposal(accepted.Proposal)
	if err != nil {
		return reject(ReplayCorruption, "accepted event %q proposal: %v", ev.ID, err)
	}
	if ev.ID != proposal.TransitionID {
		return reject(ReplayCorruption, "accepted event id does not bind proposal transition id")
	}
	if _, exists := a.accepted[proposal.TransitionID]; exists {
		return reject(ReplayCorruption, "duplicate accepted transition id %q in durable history", proposal.TransitionID)
	}
	if proposal.Prior != a.state.Ref() {
		return reject(ReplayCorruption, "accepted event %q has stale or invalid predecessor", ev.ID)
	}
	next := apply(a.state, proposal)
	if accepted.Result != next.Ref() {
		return reject(ReplayCorruption, "accepted event %q result does not match replay", ev.ID)
	}
	if admission, err := canonicalJSON(accepted.Admission); err != nil || !bytes.Equal(admission, proposal.AdmissionData) {
		return reject(ReplayCorruption, "accepted event %q admission data does not match proposal", ev.ID)
	}
	accepted.Proposal = proposal
	accepted.Admission = cloneRaw(proposal.AdmissionData)
	accepted.Durable = durableBinding(ev)
	a.state = next
	a.accepted[proposal.TransitionID] = acceptedRecord{request: request, accepted: accepted}
	return nil
}

type Checkpoint struct {
	State           Projection           `json:"state"`
	HistorySequence int64                `json:"history_sequence"`
	HistoryHash     string               `json:"history_hash"`
	Accepted        []AcceptedTransition `json:"accepted"`
}

func (a *Authority) Checkpoint() Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := Checkpoint{State: cloneProjection(a.state), HistoryHash: "genesis"}
	ids := make([]string, 0, len(a.accepted))
	for id := range a.accepted {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		accepted := a.accepted[id].accepted
		cp.Accepted = append(cp.Accepted, accepted)
		if accepted.Durable.Sequence > cp.HistorySequence {
			cp.HistorySequence = accepted.Durable.Sequence
			cp.HistoryHash = accepted.Durable.EventHash
		}
	}
	return cp
}

// VerifyCheckpoint proves that the snapshot is the state at its durable head,
// then proves replaying its suffix yields the same result as full replay.
func VerifyCheckpoint(cp Checkpoint, history []eventlog.Event, writer DurableWriter, policy PolicyFunc) error {
	if err := eventlog.ValidateHistory(history); err != nil {
		return reject(ReplayCorruption, "checked durable history: %v", err)
	}
	if cp.State.Hash != hashProjection(cp.State) {
		return reject(ReplayCorruption, "checkpoint semantic hash mismatch")
	}
	prefix := make([]eventlog.Event, 0, len(history))
	for _, ev := range history {
		if ev.Seq <= cp.HistorySequence {
			prefix = append(prefix, ev)
		}
	}
	if cp.HistorySequence == 0 {
		if cp.HistoryHash != "genesis" {
			return reject(ReplayCorruption, "initial checkpoint has non-genesis hash")
		}
	} else if len(prefix) == 0 || prefix[len(prefix)-1].Seq != cp.HistorySequence || prefix[len(prefix)-1].Hash != cp.HistoryHash {
		return reject(ReplayCorruption, "checkpoint durable head is absent from history")
	}
	atHead, err := Rebuild(prefix, writer, policy)
	if err != nil {
		return err
	}
	if !sameProjection(atHead.state, cp.State) || !sameAccepted(atHead, cp.Accepted) {
		return reject(ReplayCorruption, "checkpoint differs from replay at durable head")
	}
	fromCheckpoint, err := authorityFromCheckpoint(cp, writer, policy)
	if err != nil {
		return err
	}
	for _, ev := range history {
		if ev.Seq > cp.HistorySequence {
			if err := fromCheckpoint.replay(ev); err != nil {
				return err
			}
		}
	}
	full, err := Rebuild(history, writer, policy)
	if err != nil {
		return err
	}
	if !sameProjection(fromCheckpoint.state, full.state) || !sameAccepted(fromCheckpoint, acceptedSlice(full)) {
		return reject(ReplayCorruption, "checkpoint suffix rebuild differs from full replay")
	}
	return nil
}

func RebuildFromCheckpoint(cp Checkpoint, history []eventlog.Event, writer DurableWriter, policy PolicyFunc) (*Authority, error) {
	if err := VerifyCheckpoint(cp, history, writer, policy); err != nil {
		return nil, err
	}
	a, err := authorityFromCheckpoint(cp, writer, policy)
	if err != nil {
		return nil, err
	}
	for _, ev := range history {
		if ev.Seq > cp.HistorySequence {
			if err := a.replay(ev); err != nil {
				return nil, err
			}
		}
	}
	return a, nil
}

func authorityFromCheckpoint(cp Checkpoint, writer DurableWriter, policy PolicyFunc) (*Authority, error) {
	a := New(writer, policy)
	a.state = cloneProjection(cp.State)
	for _, accepted := range cp.Accepted {
		request, proposal, err := canonicalProposal(accepted.Proposal)
		if err != nil {
			return nil, reject(ReplayCorruption, "checkpoint proposal: %v", err)
		}
		if _, exists := a.accepted[proposal.TransitionID]; exists {
			return nil, reject(ReplayCorruption, "checkpoint duplicate transition id %q", proposal.TransitionID)
		}
		if accepted.Durable.EventID != proposal.TransitionID {
			return nil, reject(ReplayCorruption, "checkpoint binding does not match transition id")
		}
		if err := eventlog.ValidateRustAcknowledgement(eventlog.Event{
			ID:      accepted.Durable.EventID,
			Seq:     accepted.Durable.Sequence,
			RustAck: accepted.Durable.RustAck,
		}); err != nil {
			return nil, reject(ReplayCorruption, "checkpoint binding lacks valid Rust acknowledgement: %v", err)
		}
		accepted.Proposal = proposal
		a.accepted[proposal.TransitionID] = acceptedRecord{request: request, accepted: accepted}
	}
	return a, nil
}

func acceptedSlice(a *Authority) []AcceptedTransition {
	ids := make([]string, 0, len(a.accepted))
	for id := range a.accepted {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]AcceptedTransition, 0, len(ids))
	for _, id := range ids {
		result = append(result, a.accepted[id].accepted)
	}
	return result
}

func sameAccepted(a *Authority, expected []AcceptedTransition) bool {
	got := acceptedSlice(a)
	if len(got) != len(expected) {
		return false
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Proposal.TransitionID < expected[j].Proposal.TransitionID })
	for i := range got {
		if got[i].Proposal.TransitionID != expected[i].Proposal.TransitionID || got[i].Result != expected[i].Result || !sameDurableBinding(got[i].Durable, expected[i].Durable) {
			return false
		}
	}
	return true
}

func sameDurableBinding(left, right DurableBinding) bool {
	if left.EventID != right.EventID || left.Sequence != right.Sequence || left.EventHash != right.EventHash {
		return false
	}
	if left.RustAck == nil || right.RustAck == nil {
		return left.RustAck == nil && right.RustAck == nil
	}
	return *left.RustAck == *right.RustAck
}

func sameProjection(a, b Projection) bool {
	left, _ := canonicalJSON(mustJSON(a))
	right, _ := canonicalJSON(mustJSON(b))
	return bytes.Equal(left, right)
}

func (cp Checkpoint) String() string {
	return fmt.Sprintf("checkpoint@%d/%s", cp.HistorySequence, cp.HistoryHash)
}
