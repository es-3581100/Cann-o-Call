package server

import (
	"fmt"
	"io"
	"net/http"

	"flatten-workspace/internal/snapshot"
)

func (s *Server) jsonVerifySnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(256 << 20); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("parse multipart form: %w", err))
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("multipart file field 'file' required: %w", err))
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("read uploaded snapshot: %w", err))
		return
	}

	loaded, err := snapshot.ReadZip(data)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, fmt.Errorf("read snapshot bundle: %w", err))
		return
	}

	workspaceID := r.URL.Query().Get("workspace_id")

	result, err := snapshot.VerifyLoaded(loaded, workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) uiVerifySnapshot(w http.ResponseWriter, r *http.Request) {
	file, _, err := r.FormFile("file")
	if err != nil {
		s.uiRenderError(w, r, "multipart file field 'file' is required")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("read uploaded snapshot: %v", err))
		return
	}

	loaded, err := snapshot.ReadZip(data)
	if err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("read snapshot bundle: %v", err))
		return
	}

	result, err := snapshot.VerifyLoaded(loaded, "")
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Snapshot verification\n\nProjection OK: %v\nReplay OK: %v\nFiles: %v\nEvents: %v\nReceipts: %v\n\nCurrent projection hash: %v\nSnapshot projection hash: %v\nCurrent ledger hash: %v\nReplayed ledger hash: %v",
		result["projection_ok"],
		result["replay_ok"],
		result["file_count"],
		result["event_count"],
		result["receipt_count"],
		result["current_projection_hash"],
		result["snapshot_projection_hash"],
		result["current_ledger_hash"],
		result["replayed_ledger_hash"],
	)

	s.uiRenderResult(w, r, msg)
}
