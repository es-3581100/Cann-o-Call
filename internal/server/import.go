package server

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"flatten-workspace/internal/policy"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

func (s *Server) scanBuildLedgerSecrets(ws *workspace.Workspace) error {
	for p, f := range ws.Files {
		if !strings.HasPrefix(p, "build-ledger/") {
			continue
		}

		if detections := policy.ScanBytes(f.Data); len(detections) > 0 {
			return fmt.Errorf(
				"build-ledger file %q rejected: secret-bearing data detected",
				p,
			)
		}
	}

	return nil
}

func (s *Server) recordBuildLedgerBaseline(
	r *http.Request,
	ws *workspace.Workspace,
) error {
	buildLedgerFiles := map[string]string{}
	fileList := []string{}

	for p, f := range ws.Files {
		if !strings.HasPrefix(p, "build-ledger/") {
			continue
		}

		buildLedgerFiles[p] = base64.StdEncoding.EncodeToString(f.Data)
		fileList = append(fileList, p)
	}

	if len(buildLedgerFiles) == 0 {
		return nil
	}

	sort.Strings(fileList)

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

	return err
}

func (s *Server) opImportEnvelope(
	r *http.Request,
	data []byte,
	name string,
) (*workspace.Workspace, *receipts.Receipt, error) {
	ws, err := workspace.Parse(data)
	if err != nil {
		return nil, nil, err
	}

	if name != "" && ws.Source.Name == "" {
		ws.Source.Name = name
	}

	if err := s.scanBuildLedgerSecrets(ws); err != nil {
		return nil, nil, err
	}

	importReceipt, err := s.recordTransition(
		r,
		ws,
		"workspace.imported_envelope",
		"Import flatten-workspace/v1 envelope",
		nil,
		map[string]any{
			"source_name":      ws.Source.Name,
			"file_count":       ws.FileCount,
			"directory_count":  ws.DirectoryCount,
			"manifest_checked": ws.ManifestChecked,
			"issue_count":      len(ws.Issues),
		},
	)
	if err != nil {
		return nil, nil, err
	}

	if err := s.recordBuildLedgerBaseline(r, ws); err != nil {
		return nil, nil, err
	}
	// A workspace becomes visible only after every durable import transition,
	// including its build-ledger baseline, has been admitted.
	s.store.Add(ws)

	return ws, importReceipt, nil
}

func (s *Server) jsonCreateWorkspaceFromEnvelope(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("read request body: %w", err))
		return
	}

	if len(data) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("empty envelope body"))
		return
	}

	name := r.URL.Query().Get("name")

	ws, receipt, err := s.opImportEnvelope(r, data, name)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"summary": ws.Summary(),
		"receipt": receipt,
	})
}

func (s *Server) uiCreateFromEnvelope(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("parse form: %v", err))
		return
	}

	body := []byte(r.FormValue("envelope"))
	if len(body) == 0 {
		s.uiRenderError(w, r, "envelope field is required")
		return
	}

	name := r.FormValue("name")

	ws, receipt, err := s.opImportEnvelope(r, body, name)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	result := fmt.Sprintf("Imported envelope. Receipt: %s", receipt.ID)
	ui := s.workspaceUIData(ws, result)

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "workspace", ui)
	} else {
		s.renderPage(w, ui)
	}
}
