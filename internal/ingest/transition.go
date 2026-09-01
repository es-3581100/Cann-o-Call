package ingest

import (
	"encoding/json"
	"fmt"

	"flatten-workspace/internal/transition"
)

// Operation constants for canonical ingest mutations.
const (
	OperationSourceIngested = "source.ingested"
	OperationGraphIngested  = "context_graph.ingested"
)

// ProposeIngest proposes a single SourcePacket via the Go admission authority.
// It preserves Go admission -> Rust durable ACK -> projection update.
// Returns AcceptedTransition on success; on Rust unavailable it fails-closed with Durable class.
func ProposeIngest(authority *transition.Authority, pkt SourcePacket) (transition.AcceptedTransition, error) {
	if authority == nil {
		return transition.AcceptedTransition{}, fmt.Errorf("authority is nil")
	}
	if err := pkt.Validate(); err != nil {
		return transition.AcceptedTransition{}, fmt.Errorf("packet invalid: %w", err)
	}
	// Build result data as canonical JSON. Do not embed large blobs beyond reference.
	resultData, err := json.Marshal(map[string]any{
		"legacy_action":  "source.ingested",
		"legacy_details": map[string]any{"source_id": pkt.Identity.SourceID, "workspace_id": pkt.Identity.WorkspaceID},
		"packet":         pkt,
	})
	if err != nil {
		return transition.AcceptedTransition{}, err
	}
	// Use deterministic transition id tied to packet + workspace to allow idempotent dedup.
	// But authority also handles duplicate transition_id suppression.
	transitionID := DeterministicSourceID(pkt.Identity.WorkspaceID, "transition", pkt.Identity.SourceID, pkt.Content.ContentHash, ExtractionVersion)
	proposalID := "proposal-" + transitionID[:16]
	requestID := "request-" + transitionID[:16]
	prior := authority.Projection().Ref()
	proposal := transition.ProposedTransition{
		TransitionID: transitionID,
		ProposalID:   proposalID,
		RequestID:    requestID,
		Prior:        prior,
		Operation:    "upsert",
		Entity:       pkt.Identity.WorkspaceID,
		Node:         pkt.Identity.SourceID,
		ResultData:   json.RawMessage(resultData),
	}
	return authority.Propose(proposal)
}

// PacketFromAccepted extracts SourcePacket from an AcceptedTransition.
func PacketFromAccepted(at transition.AcceptedTransition) (*SourcePacket, error) {
	var wrapper struct {
		Packet SourcePacket `json:"packet"`
	}
	if err := json.Unmarshal(at.Proposal.ResultData, &wrapper); err != nil {
		return nil, err
	}
	if err := wrapper.Packet.Validate(); err != nil {
		return nil, err
	}
	return &wrapper.Packet, nil
}

// PacketsFromHistory rebuilds packets from accepted history for graph rebuild.
func PacketsFromHistory(accepted []transition.AcceptedTransition) ([]SourcePacket, error) {
	var out []SourcePacket
	for _, at := range accepted {
		pkt, err := PacketFromAccepted(at)
		if err != nil {
			continue // skip non-ingest transitions (build-ledger etc) — keep determinism
		}
		out = append(out, *pkt)
	}
	return out, nil
}
