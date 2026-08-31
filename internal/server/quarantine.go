package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

type uiQuarantine struct {
	WorkspaceID      string
	Count            int
	UnsafeEntryCount int
	Items            []string
	Decisions        []workspace.QuarantineDecision
	Blobs            []*workspace.QuarantinedBlob
}

func findQuarantineItem(ws *workspace.Workspace, item string) bool {
	for _, q := range ws.Quarantined {
		if q == item {
			return true
		}
	}

	return false
}

func (s *Server) quarantineData(ws *workspace.Workspace) uiQuarantine {
	return uiQuarantine{
		WorkspaceID:      ws.ID,
		Count:            len(ws.Quarantined),
		UnsafeEntryCount: ws.Source.UnsafeEntryCount,
		Items:            ws.Quarantined,
		Decisions:        ws.QuarantineDecisions,
		Blobs:            ws.QuarantinedBlobs,
	}
}

func (s *Server) jsonQuarantine(w http.ResponseWriter, r *http.Request) {
	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, s.quarantineData(ws))
}

func (s *Server) uiQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	s.renderAny(w, "quarantine", s.quarantineData(ws))
}

func (s *Server) opQuarantineDecision(
	r *http.Request,
	ws *workspace.Workspace,
	item string,
	decision string,
	reason string,
) (*receipts.Receipt, error) {
	if item == "" {
		return nil, errors.New("quarantine item is required")
	}

	if decision != "approve" && decision != "reject" {
		return nil, errors.New("decision must be approve or reject")
	}

	if !findQuarantineItem(ws, item) {
		return nil, fmt.Errorf("quarantine item %q not found", item)
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"quarantine.decision.recorded",
		"Record quarantine approval/rejection decision",
		nil,
		map[string]any{
			"item":     item,
			"decision": decision,
			"reason":   reason,
		},
	)
	if err != nil {
		return nil, err
	}

	newDecision := workspace.QuarantineDecision{
		Item:      item,
		Decision:  decision,
		Reason:    reason,
		DecidedAt: time.Now().UTC(),
		ReceiptID: receipt.ID,
	}

	updated := []workspace.QuarantineDecision{}

	for _, existing := range ws.QuarantineDecisions {
		if existing.Item != item {
			updated = append(updated, existing)
		}
	}

	ws.QuarantineDecisions = append(updated, newDecision)

	return receipt, nil
}

func (s *Server) jsonQuarantineDecision(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	var req struct {
		Item     string `json:"item"`
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode quarantine decision: %w", err))
		return
	}

	receipt, err := s.opQuarantineDecision(r, ws, req.Item, req.Decision, req.Reason)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"receipt":    receipt,
		"quarantine": s.quarantineData(ws),
	})
}

func (s *Server) uiQuarantineDecision(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("parse form: %v", err))
		return
	}

	if err := s.requireAuthority(r); err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	item := r.FormValue("item")
	decision := r.FormValue("decision")
	reason := r.FormValue("reason")

	_, err := s.opQuarantineDecision(r, ws, item, decision, reason)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	s.renderAny(w, "quarantine", s.quarantineData(ws))
}
