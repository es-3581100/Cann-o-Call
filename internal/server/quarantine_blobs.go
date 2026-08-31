package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"flatten-workspace/internal/policy"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

func findQuarantineBlob(ws *workspace.Workspace, blobID string) *workspace.QuarantinedBlob {
	for _, blob := range ws.QuarantinedBlobs {
		if blob.ID == blobID {
			return blob
		}
	}

	return nil
}

func (s *Server) opAdmitQuarantineBlob(
	r *http.Request,
	ws *workspace.Workspace,
	blobID string,
	targetPath string,
) (*receipts.Receipt, error) {
	blob := findQuarantineBlob(ws, blobID)
	if blob == nil {
		return nil, fmt.Errorf("quarantine blob %q not found", blobID)
	}

	if blob.Status != "quarantined" {
		return nil, fmt.Errorf("quarantine blob %q already has status %q", blobID, blob.Status)
	}

	if err := policy.RejectIfSecrets(blob.Data); err != nil {
		return nil, err
	}

	if targetPath == "" {
		targetPath = blob.SafePath
	}

	if err := workspace.IsSafePath(targetPath); err != nil {
		return nil, fmt.Errorf("invalid target path: %w", err)
	}

	oldHash := fileSHA(ws, targetPath)

	f, err := workspace.UpsertFile(ws, targetPath, blob.Data)
	if err != nil {
		return nil, err
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"quarantine.blob.admitted",
		"Admit quarantined ZIP blob into workspace under explicit authority",
		[]string{targetPath},
		map[string]any{
			"blob_id":       blobID,
			"original_path": blob.OriginalPath,
			"target_path":   targetPath,
			"old_sha":       oldHash,
			"new_sha":       f.SHA256,
		},
	)
	if err != nil {
		return nil, err
	}

	blob.Status = "approved"
	blob.ReceiptID = receipt.ID
	blob.DecidedAt = time.Now().UTC().Format(time.RFC3339)
	blob.TargetPath = targetPath

	if s.Quarantine != nil {
		_ = s.Quarantine.UpdateStatus(ws.ID, blobID, blob.Status, receipt.ID, blob.DecidedAt, blob.TargetPath)
	}

	ws.QuarantineDecisions = append(ws.QuarantineDecisions, workspace.QuarantineDecision{
		Item:      blob.OriginalPath,
		Decision:  "approve",
		Reason:    fmt.Sprintf("admitted to %s", targetPath),
		DecidedAt: time.Now().UTC(),
		ReceiptID: receipt.ID,
	})

	return receipt, nil
}

func (s *Server) opRejectQuarantineBlob(
	r *http.Request,
	ws *workspace.Workspace,
	blobID string,
	reason string,
) (*receipts.Receipt, error) {
	blob := findQuarantineBlob(ws, blobID)
	if blob == nil {
		return nil, fmt.Errorf("quarantine blob %q not found", blobID)
	}

	if blob.Status != "quarantined" {
		return nil, fmt.Errorf("quarantine blob %q already has status %q", blobID, blob.Status)
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"quarantine.blob.rejected",
		"Reject quarantined ZIP blob",
		nil,
		map[string]any{
			"blob_id":       blobID,
			"original_path": blob.OriginalPath,
			"reason":        reason,
		},
	)
	if err != nil {
		return nil, err
	}

	blob.Status = "rejected"
	blob.ReceiptID = receipt.ID
	blob.DecidedAt = time.Now().UTC().Format(time.RFC3339)

	if s.Quarantine != nil {
		_ = s.Quarantine.UpdateStatus(ws.ID, blobID, blob.Status, receipt.ID, blob.DecidedAt, "")
	}

	ws.QuarantineDecisions = append(ws.QuarantineDecisions, workspace.QuarantineDecision{
		Item:      blob.OriginalPath,
		Decision:  "reject",
		Reason:    reason,
		DecidedAt: time.Now().UTC(),
		ReceiptID: receipt.ID,
	})

	return receipt, nil
}

func (s *Server) jsonAdmitQuarantineBlob(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	blobID := r.PathValue("blobID")

	var req struct {
		TargetPath string `json:"target_path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode admit request: %w", err))
		return
	}

	receipt, err := s.opAdmitQuarantineBlob(r, ws, blobID, req.TargetPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"receipt":    receipt,
		"quarantine": s.quarantineData(ws),
	})
}

func (s *Server) jsonRejectQuarantineBlob(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	blobID := r.PathValue("blobID")

	var req struct {
		Reason string `json:"reason"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode reject request: %w", err))
		return
	}

	receipt, err := s.opRejectQuarantineBlob(r, ws, blobID, req.Reason)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"receipt":    receipt,
		"quarantine": s.quarantineData(ws),
	})
}

func (s *Server) uiAdmitQuarantineBlob(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("parse form: %v", err))
		return
	}

	if err := s.requireAuthority(r); err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	id := r.PathValue("id")
	blobID := r.PathValue("blobID")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	targetPath := r.FormValue("target_path")

	receipt, err := s.opAdmitQuarantineBlob(r, ws, blobID, targetPath)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Admitted quarantined blob %s. Receipt: %s",
		blobID,
		receipt.ID,
	)

	s.uiRenderResult(w, r, msg)
}

func (s *Server) uiRejectQuarantineBlob(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("parse form: %v", err))
		return
	}

	if err := s.requireAuthority(r); err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	id := r.PathValue("id")
	blobID := r.PathValue("blobID")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	reason := r.FormValue("reason")

	receipt, err := s.opRejectQuarantineBlob(r, ws, blobID, reason)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Rejected quarantined blob %s. Receipt: %s",
		blobID,
		receipt.ID,
	)

	s.uiRenderResult(w, r, msg)
}
