package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"flatten-workspace/internal/projectroot"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

type bindRequest struct {
	ProjectRoot string `json:"project_root"`
	Confirm     bool   `json:"confirm"`
	VerifyFiles bool   `json:"verify_files"`
	Force       bool   `json:"force"`
}

func (s *Server) opBindWorkspace(
	r *http.Request,
	ws *workspace.Workspace,
	req bindRequest,
) (*receipts.Receipt, error) {
	if !req.Confirm {
		return nil, errors.New("binding requires confirm:true")
	}

	root := strings.TrimSpace(req.ProjectRoot)
	if root == "" {
		return nil, errors.New("project_root is required")
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve project root: %w", err)
	}

	verification := projectroot.Verify(absRoot, ws, req.VerifyFiles)

	status := "bound"

	if !verification.Verified {
		if !req.Force {
			return nil, fmt.Errorf(
				"project root verification failed: %s",
				strings.Join(verification.Notes, "; "),
			)
		}

		status = "bound_forced"
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"workspace.binding.recorded",
		"Bind workspace to project root",
		nil,
		map[string]any{
			"root":         absRoot,
			"verify_files": req.VerifyFiles,
			"force":        req.Force,
			"verification": verification,
		},
	)
	if err != nil {
		return nil, err
	}

	ws.Binding = &workspace.WorkspaceBinding{
		ProjectRoot:  absRoot,
		BoundAt:      time.Now().UTC(),
		Status:       status,
		Verification: verification,
		ReceiptID:    receipt.ID,
	}

	return receipt, nil
}

func (s *Server) jsonBindWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	var req bindRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode bind request: %w", err))
		return
	}

	receipt, err := s.opBindWorkspace(r, ws, req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"binding": ws.Binding,
		"receipt": receipt,
	})
}

func (s *Server) uiBindWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("parse form: %v", err))
		return
	}

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	if err := s.requireAuthority(r); err != nil {
		data := s.workspaceUIData(ws, err.Error())
		s.renderPartial(w, "workspace", data)
		return
	}

	req := bindRequest{
		ProjectRoot: r.FormValue("project_root"),
		Confirm:     r.FormValue("confirm") == "true",
		VerifyFiles: r.FormValue("verify_files") == "true",
		Force:       r.FormValue("force") == "true",
	}

	receipt, err := s.opBindWorkspace(r, ws, req)
	if err != nil {
		data := s.workspaceUIData(ws, err.Error())
		s.renderPartial(w, "workspace", data)
		return
	}

	msg := fmt.Sprintf(
		"Bound workspace to %s. Status: %s. Receipt: %s",
		ws.Binding.ProjectRoot,
		ws.Binding.Status,
		receipt.ID,
	)

	data := s.workspaceUIData(ws, msg)
	s.renderPartial(w, "workspace", data)
}
