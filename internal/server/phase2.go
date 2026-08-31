package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/ids"
	"flatten-workspace/internal/materialize"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"

	"gopkg.in/yaml.v3"
)

func (s *Server) requireAuthority(r *http.Request) error {
	token := r.Header.Get("X-Authority-Token")

	if token == "" {
		token = r.FormValue("authority_token")
	}

	return s.Authority.Check(token)
}

func (s *Server) recordTransition(
	r *http.Request,
	ws *workspace.Workspace,
	action string,
	objective string,
	files []string,
	details map[string]any,
) (*receipts.Receipt, error) {
	actor := r.Header.Get("X-Actor")
	if actor == "" {
		actor = "api"
	}

	receiptID := ids.New("receipt")
	eventID := ids.New("event")

	if details == nil {
		details = map[string]any{}
	}

	details["authority_source"] = "explicit_authority_token"

	ev := eventlog.Event{
		ID:          eventID,
		Type:        "transition",
		WorkspaceID: ws.ID,
		Actor:       actor,
		Action:      action,
		Status:      "accepted",
		Details:     details,
		ReceiptID:   receiptID,
	}

	savedEvent, err := s.Events.Append(ev)
	if err != nil {
		return nil, err
	}

	rec := receipts.Receipt{
		ID:              receiptID,
		WorkspaceID:     ws.ID,
		Action:          action,
		Objective:       objective,
		Status:          "delivered",
		AuthoritySource: "explicit_authority_token",
		EventID:         savedEvent.ID,
		FilesChanged:    files,
		Details:         details,
	}

	savedReceipt, err := s.Receipts.Save(rec)
	if err != nil {
		return nil, err
	}

	s.maybeActivateActor(ws.ID, action, details)

	return &savedReceipt, nil
}

func (s *Server) createWorkspaceFromZip(w http.ResponseWriter, r *http.Request) {
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

	ws, err := workspace.FromZipBytes(data, name)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}

	s.store.Add(ws)

	receipt, err := s.recordTransition(
		r,
		ws,
		"workspace.imported_zip",
		"Import real ZIP and convert it into flatten-workspace/v1",
		nil,
		map[string]any{
			"archive_name":       name,
			"file_count":         ws.FileCount,
			"directory_count":    ws.DirectoryCount,
			"unsafe_entry_count": ws.Source.UnsafeEntryCount,
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record zip import transition: %w", err))
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
	})
}

func (s *Server) getEnvelope(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}

	env, err := ws.ToEnvelope()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	format := r.URL.Query().Get("format")
	if format == "yaml" {
		b, err := yaml.Marshal(env)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(b)
		return
	}

	writeJSON(w, http.StatusOK, env)
}

func (s *Server) materializeWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
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

	if !req.Confirm {
		writeError(w, http.StatusBadRequest, errors.New("materialization requires confirm:true"))
		return
	}

	base := filepath.Join(s.DataDir, "materialized")

	root := req.Root
	if root == "" {
		root = filepath.Join(base, ws.ID)
	} else if !filepath.IsAbs(root) {
		root = filepath.Join(base, root)
	}

	if !s.AllowAbsoluteRoot {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		absBase, err := filepath.Abs(base)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if !strings.HasPrefix(absRoot, absBase+string(os.PathSeparator)) {
			writeError(
				w,
				http.StatusForbidden,
				errors.New("materialization root must remain under the controlled data directory unless ALLOW_ABSOLUTE_ROOT=true"),
			)
			return
		}
	}

	written, err := materialize.WriteWorkspace(ws, root, s.AllowAbsoluteRoot)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"workspace.materialized",
		"Materialize workspace to disk under explicit authority",
		written,
		map[string]any{
			"root":       root,
			"file_count": len(written),
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record materialization transition: %w", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"root":    root,
		"written": written,
	})
}

func fileSHA(ws *workspace.Workspace, p string) string {
	if f, ok := ws.Files[p]; ok {
		return f.SHA256
	}
	return ""
}

func (s *Server) updateBuildLedgerState(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var parsed any
	if err := yaml.Unmarshal(body, &parsed); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body must be valid YAML: %w", err))
		return
	}

	p := "build-ledger/current/state.yaml"
	oldHash := fileSHA(ws, p)

	f, err := workspace.UpsertFile(ws, p, body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"build_ledger.state.updated",
		"Update build-ledger current state projection",
		[]string{p},
		map[string]any{
			"path":     p,
			"old_sha":  oldHash,
			"new_sha":  f.SHA256,
			"new_size": f.Size,
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record state update transition: %w", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
	})
}

func (s *Server) appendBuildLedgerEvent(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var eventObj map[string]any
	if err := json.Unmarshal(body, &eventObj); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body must be a JSON object: %w", err))
		return
	}

	if eventObj["id"] == nil {
		eventObj["id"] = ids.New("event")
	}

	if eventObj["type"] == nil {
		eventObj["type"] = "event"
	}

	if eventObj["revision"] == nil {
		eventObj["revision"] = 1
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if eventObj["created_at"] == nil {
		eventObj["created_at"] = now
	}

	eventObj["updated_at"] = now

	if eventObj["status"] == nil {
		eventObj["status"] = "recorded"
	}

	line, err := json.Marshal(eventObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	p := "build-ledger/events/ledger.jsonl"
	oldHash := fileSHA(ws, p)

	f, err := workspace.AppendToFile(ws, p, line)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		"build_ledger.event.appended",
		"Append event to build-ledger durable JSONL history",
		[]string{p},
		map[string]any{
			"path":    p,
			"old_sha": oldHash,
			"new_sha": f.SHA256,
			"event":   eventObj["id"],
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record event append transition: %w", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
	})
}

func (s *Server) createBuildLedgerRun(w http.ResponseWriter, r *http.Request) {
	s.createBuildLedgerDocument(w, r, "runs", "run", "build_ledger.run.created")
}

func (s *Server) createBuildLedgerReceipt(w http.ResponseWriter, r *http.Request) {
	s.createBuildLedgerDocument(w, r, "receipts", "receipt", "build_ledger.receipt.created")
}

func (s *Server) createBuildLedgerVerification(w http.ResponseWriter, r *http.Request) {
	s.createBuildLedgerDocument(w, r, "verification", "verification", "build_ledger.verification.created")
}

func (s *Server) createBuildLedgerDocument(
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

	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body must be valid YAML: %w", err))
		return
	}

	docID, _ := doc["id"].(string)
	if docID == "" {
		docID = ids.New(idPrefix)
		doc["id"] = docID
	}

	if doc["type"] == nil {
		doc["type"] = idPrefix
	}

	if doc["revision"] == nil {
		doc["revision"] = 1
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if doc["created_at"] == nil {
		doc["created_at"] = now
	}

	doc["updated_at"] = now

	out, err := yaml.Marshal(doc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	p := fmt.Sprintf("build-ledger/%s/%s.yaml", dir, docID)
	oldHash := fileSHA(ws, p)

	f, err := workspace.UpsertFile(ws, p, out)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	receipt, err := s.recordTransition(
		r,
		ws,
		action,
		"Create build-ledger document under explicit authority",
		[]string{p},
		map[string]any{
			"path":    p,
			"doc_id":  docID,
			"old_sha": oldHash,
			"new_sha": f.SHA256,
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record build-ledger document transition: %w", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
		"file":    f,
	})
}
