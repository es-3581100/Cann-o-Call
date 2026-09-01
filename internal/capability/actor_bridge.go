package capability

import (
	"encoding/json"
	"fmt"
	"time"

	"flatten-workspace/internal/actorstub"
)

// ActorBridge enforces that actors may REQUEST not self-authorize, and preserves lineage/budget invariants.
type ActorBridge struct {
	ctrl *actorstub.Controller
	mgr  *Manager
}

func NewActorBridge(c *actorstub.Controller, m *Manager) *ActorBridge {
	return &ActorBridge{ctrl: c, mgr: m}
}

// RequestFromActor validates actor descriptor and builds CapabilityRequest preserving lineage, not replenishing budget, not creating new lineage.
func (b *ActorBridge) RequestFromActor(actorID, capabilityID string, inputs json.RawMessage, workspaceID, requestedOp string) (Request, error) {
	if b.ctrl == nil {
		return Request{}, NewError(ErrInvalidInput, "actor controller is nil", nil)
	}
	desc, ok := b.ctrl.Descriptor(actorID)
	if !ok {
		return Request{}, NewError(ErrInvalidInput, fmt.Sprintf("actor %q not found", actorID), nil)
	}
	if desc.LineageID == "" {
		return Request{}, NewError(ErrInvalidInput, "actor has invalid lineage", nil)
	}
	rid := fmt.Sprintf("req-%s-%s", actorID, capabilityID)
	if len(rid) > 64 {
		rid = rid[:64]
	}
	req := Request{
		RequestID:          rid,
		CapabilityID:       capabilityID,
		WorkspaceID:        workspaceID,
		ActorID:            actorID,
		LineageID:          desc.LineageID,
		Inputs:             inputs,
		EvidenceRefs:       []string{"evidence://" + actorID},
		RequestedOperation: requestedOp,
		Timestamp:          time.Now().UTC(),
		Version:            "1",
	}
	if err := req.Validate(); err != nil {
		return Request{}, err
	}
	if _, err := b.mgr.Registry().GetDescriptor(capabilityID); err != nil {
		return Request{}, err
	}
	return req, nil
}

// LineagePreserved verifies that request lineage equals actor lineage.
func (b *ActorBridge) LineagePreserved(actorID string, req Request) bool {
	desc, ok := b.ctrl.Descriptor(actorID)
	if !ok {
		return false
	}
	return desc.LineageID == req.LineageID
}
