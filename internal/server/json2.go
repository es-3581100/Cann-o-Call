package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"flatten-workspace/internal/workspace"
)

func (s *Server) getWorkspaceForAPI(w http.ResponseWriter, r *http.Request) (*workspace.Workspace, bool) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return nil, false
	}

	return ws, true
}

func (s *Server) jsonCreateWorkspaceFromZip(w http.ResponseWriter, r *http.Request) {
	var data []byte
	var name string

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		if err := r.ParseMultipartForm(256 << 20); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("parse multipart form: %w", err))
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("multipart file field 'file' required: %w", err))
			return
		}
		defer file.Close()

		name = header.Filename

		data, err = io.ReadAll(file)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("read uploaded file: %w", err))
			return
		}
	} else {
		var err error

		data, err = io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
			return
		}

		name = r.URL.Query().Get("name")
		if name == "" {
			name = "upload.zip"
		}
	}

	ws, receipt, err := s.opImportZip(r, data, name)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
	})
}

func (s *Server) jsonMaterializeWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	var req struct {
		Root    string `json:"root"`
		Confirm bool   `json:"confirm"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode materialization request: %w", err))
		return
	}

	root, written, receipt, err := s.opMaterialize(r, ws, req.Root, req.Confirm)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"root":    root,
		"written": written,
	})
}

func (s *Server) jsonUpdateBuildLedgerState(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	f, receipt, err := s.opUpdateState(r, ws, body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
	})
}

func (s *Server) jsonAppendBuildLedgerEvent(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	f, receipt, err := s.opAppendEvent(r, ws, body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
	})
}

func (s *Server) jsonCreateBuildLedgerRun(w http.ResponseWriter, r *http.Request) {
	s.jsonCreateBuildLedgerDocument(w, r, "runs", "run", "build_ledger.run.created")
}

func (s *Server) jsonCreateBuildLedgerReceipt(w http.ResponseWriter, r *http.Request) {
	s.jsonCreateBuildLedgerDocument(w, r, "receipts", "receipt", "build_ledger.receipt.created")
}

func (s *Server) jsonCreateBuildLedgerVerification(w http.ResponseWriter, r *http.Request) {
	s.jsonCreateBuildLedgerDocument(w, r, "verification", "verification", "build_ledger.verification.created")
}

func (s *Server) jsonCreateBuildLedgerDocument(
	w http.ResponseWriter,
	r *http.Request,
	dir string,
	idPrefix string,
	action string,
) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	docID, f, receipt, err := s.opCreateDocument(r, ws, body, dir, idPrefix, action)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
		"doc_id":  docID,
	})
}
