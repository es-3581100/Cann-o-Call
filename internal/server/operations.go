package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"flatten-workspace/internal/buildledger"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/ids"
	"flatten-workspace/internal/materialize"
	"flatten-workspace/internal/policy"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"

	"gopkg.in/yaml.v3"
)

func (s *Server) opImportZip(
	r *http.Request,
	data []byte,
	name string,
) (*workspace.Workspace, *receipts.Receipt, error) {
	ws, err := workspace.FromZipBytes(data, name)
	if err != nil {
		return nil, nil, err
	}

	s.syncWorkspaceQuarantine(ws)

	buildLedgerFiles := map[string]string{}
	fileList := []string{}

	for p, f := range ws.Files {
		if strings.HasPrefix(p, "build-ledger/") {
			if detections := policy.ScanBytes(f.Data); len(detections) > 0 {
				return nil, nil, fmt.Errorf(
					"build-ledger file %q rejected: secret-bearing data detected",
					p,
				)
			}

			buildLedgerFiles[p] = base64.StdEncoding.EncodeToString(f.Data)
			fileList = append(fileList, p)
		}
	}

	sort.Strings(fileList)

	s.store.Add(ws)

	importReceipt, err := s.recordTransition(
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
		return nil, nil, err
	}

	if len(buildLedgerFiles) > 0 {
		_, err := s.recordTransition(
			r,
			ws,
			"build_ledger.baseline.recorded",
			"Record baseline build-ledger state for replay",
			fileList,
			map[string]any{
				"file_count": len(buildLedgerFiles),
				"files":      buildLedgerFiles,
			},
		)
		if err != nil {
			return nil, nil, err
		}
	}

	return ws, importReceipt, nil
}

func (s *Server) opMaterialize(
	r *http.Request,
	ws *workspace.Workspace,
	root string,
	confirm bool,
) (string, []string, *receipts.Receipt, error) {
	if !confirm {
		return "", nil, nil, errors.New("materialization requires confirm:true")
	}

	base := filepath.Join(s.DataDir, "materialized")

	if root == "" {
		root = filepath.Join(base, ws.ID)
	} else if !filepath.IsAbs(root) {
		root = filepath.Join(base, root)
	}

	if !s.AllowAbsoluteRoot {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			return "", nil, nil, err
		}

		absBase, err := filepath.Abs(base)
		if err != nil {
			return "", nil, nil, err
		}

		if !strings.HasPrefix(absRoot, absBase+string(os.PathSeparator)) {
			return "", nil, nil, errors.New(
				"materialization root must remain under the controlled data directory unless ALLOW_ABSOLUTE_ROOT=true",
			)
		}
	}

	written, err := materialize.WriteWorkspace(ws, root, s.AllowAbsoluteRoot)
	if err != nil {
		return "", nil, nil, err
	}

	rec, err := s.recordTransition(
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
		return "", nil, nil, err
	}

	return root, written, rec, nil
}

func (s *Server) opUpdateState(
	r *http.Request,
	ws *workspace.Workspace,
	body []byte,
) (*workspace.File, *receipts.Receipt, error) {
	if len(body) == 0 {
		return nil, nil, errors.New("empty state document")
	}

	if err := policy.RejectIfSecrets(body); err != nil {
		return nil, nil, err
	}

	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return nil, nil, fmt.Errorf("body must be valid YAML: %w", err)
	}

	if err := buildledger.ValidateState(doc); err != nil {
		return nil, nil, err
	}

	p := "build-ledger/current/state.yaml"
	oldHash := fileSHA(ws, p)
	oldContentBase64 := ""
	if oldFile, ok := ws.Files[p]; ok {
		oldContentBase64 = base64.StdEncoding.EncodeToString(oldFile.Data)
	}

	f, err := workspace.UpsertFile(ws, p, body)
	if err != nil {
		return nil, nil, err
	}

	rec, err := s.recordTransition(
		r,
		ws,
		"build_ledger.state.updated",
		"Update build-ledger current state projection",
		[]string{p},
		map[string]any{
			"path":               p,
			"old_sha":            oldHash,
			"new_sha":            f.SHA256,
			"new_size":           f.Size,
			"content_base64":     base64.StdEncoding.EncodeToString(f.Data),
			"old_content_base64": oldContentBase64,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return f, rec, nil
}

func (s *Server) opAppendEvent(
	r *http.Request,
	ws *workspace.Workspace,
	body []byte,
) (*workspace.File, *receipts.Receipt, error) {
	if len(body) == 0 {
		return nil, nil, errors.New("empty event body")
	}

	if err := policy.RejectIfSecrets(body); err != nil {
		return nil, nil, err
	}

	var eventObj map[string]any
	if err := json.Unmarshal(body, &eventObj); err != nil {
		return nil, nil, fmt.Errorf("body must be a JSON object: %w", err)
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

	if eventObj["event"] == nil {
		eventObj["event"] = "recorded_event"
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if eventObj["created_at"] == nil {
		eventObj["created_at"] = now
	}

	eventObj["updated_at"] = now

	if eventObj["status"] == nil {
		eventObj["status"] = "recorded"
	}

	if err := buildledger.ValidateEvent(eventObj); err != nil {
		return nil, nil, err
	}

	line, err := json.Marshal(eventObj)
	if err != nil {
		return nil, nil, err
	}

	if err := policy.RejectIfSecrets(line); err != nil {
		return nil, nil, err
	}

	p := "build-ledger/events/ledger.jsonl"
	oldHash := fileSHA(ws, p)
	oldContentBase64 := ""
	if oldFile, ok := ws.Files[p]; ok {
		oldContentBase64 = base64.StdEncoding.EncodeToString(oldFile.Data)
	}

	f, err := workspace.AppendToFile(ws, p, line)
	if err != nil {
		return nil, nil, err
	}

	rec, err := s.recordTransition(
		r,
		ws,
		"build_ledger.event.appended",
		"Append event to build-ledger durable JSONL history",
		[]string{p},
		map[string]any{
			"path":               p,
			"old_sha":            oldHash,
			"new_sha":            f.SHA256,
			"event":              eventObj["id"],
			"content_base64":     base64.StdEncoding.EncodeToString(f.Data),
			"old_content_base64": oldContentBase64,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return f, rec, nil
}

func (s *Server) opCreateDocument(
	r *http.Request,
	ws *workspace.Workspace,
	body []byte,
	dir string,
	idPrefix string,
	action string,
) (string, *workspace.File, *receipts.Receipt, error) {
	if len(body) == 0 {
		return "", nil, nil, errors.New("empty document body")
	}

	if err := policy.RejectIfSecrets(body); err != nil {
		return "", nil, nil, err
	}

	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return "", nil, nil, fmt.Errorf("body must be valid YAML: %w", err)
	}

	docID, _ := doc["id"].(string)
	if docID == "" {
		docID = ids.New(idPrefix)
		doc["id"] = docID
	}

	if strings.ContainsAny(docID, "/\\\x00") || docID == "." || docID == ".." {
		return "", nil, nil, fmt.Errorf("invalid document id %q", docID)
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

	if err := buildledger.ValidateDocument(idPrefix, doc); err != nil {
		return "", nil, nil, err
	}

	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", nil, nil, err
	}

	if err := policy.RejectIfSecrets(out); err != nil {
		return "", nil, nil, err
	}

	p := fmt.Sprintf("build-ledger/%s/%s.yaml", dir, docID)
	oldHash := fileSHA(ws, p)
	oldContentBase64 := ""
	if oldFile, ok := ws.Files[p]; ok {
		oldContentBase64 = base64.StdEncoding.EncodeToString(oldFile.Data)
	}

	f, err := workspace.UpsertFile(ws, p, out)
	if err != nil {
		return "", nil, nil, err
	}

	rec, err := s.recordTransition(
		r,
		ws,
		action,
		"Create build-ledger document under explicit authority",
		[]string{p},
		map[string]any{
			"path":               p,
			"doc_id":             docID,
			"old_sha":            oldHash,
			"new_sha":            f.SHA256,
			"content_base64":     base64.StdEncoding.EncodeToString(f.Data),
			"old_content_base64": oldContentBase64,
		},
	)
	if err != nil {
		return "", nil, nil, err
	}

	return docID, f, rec, nil
}

func (s *Server) recordGlobalTransition(
	r *http.Request,
	workspaceID string,
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
		WorkspaceID: workspaceID,
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
		WorkspaceID:     workspaceID,
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

	return &savedReceipt, nil
}

func (s *Server) createCheckpoint(w http.ResponseWriter, r *http.Request) {
	if err := s.requireAuthority(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}

	sidecarURL := os.Getenv("RUST_LEDGER_URL")

	var checkpoint map[string]any

	if sidecarURL != "" {
		resp, err := http.Post(sidecarURL+"/checkpoints", "application/json", strings.NewReader("{}"))
		if err != nil {
			writeError(w, http.StatusBadGateway, fmt.Errorf("sidecar checkpoint request failed: %w", err))
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			writeError(w, http.StatusBadGateway, fmt.Errorf("read sidecar checkpoint response: %w", err))
			return
		}

		if resp.StatusCode >= 300 {
			writeError(w, http.StatusBadGateway, fmt.Errorf("sidecar checkpoint failed: %s", string(body)))
			return
		}

		if err := json.Unmarshal(body, &checkpoint); err != nil {
			checkpoint = map[string]any{
				"raw": string(body),
			}
		}
	} else {
		cp, err := s.Events.Checkpoint(filepath.Join(s.DataDir, "checkpoints"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		checkpoint = cp
	}

	receipt, err := s.recordGlobalTransition(
		r,
		"",
		"ledger.checkpoint.created",
		"Create ledger checkpoint",
		nil,
		map[string]any{
			"checkpoint": checkpoint,
			"sidecar":    sidecarURL != "",
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record checkpoint transition: %w", err))
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"receipt":    receipt,
		"checkpoint": checkpoint,
	})
}
