package server

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"

	"flatten-workspace/internal/studio"
	"flatten-workspace/internal/workspace"
)

type UINode struct {
	Name        string
	Path        string
	Type        string
	Size        int64
	SHA256      string
	Language    string
	WorkspaceID string
	Children    []UINode
}

type uiFile struct {
	Path     string
	Content  string
	Encoding string
	SHA256   string
	Language string
	Verified bool
}

type uiData struct {
	Theme      StudioTheme
	Studio     *studio.ViewModel
	Workspaces []workspace.Summary
	Workspace  *workspace.Summary
	Binding    *workspace.WorkspaceBinding
	Tree       []UINode
	Result     string
	Error      string
	File       *uiFile
	ReceiptID  string
}

// StudioTheme is presentation-only. Keeping the palette typed and supplied by
// the server makes the embedded page's token values auditable in one place.
type StudioTheme struct {
	Void       string `json:"void"`
	Surface    string `json:"surface"`
	Raised     string `json:"raised"`
	Inset      string `json:"inset"`
	Text       string `json:"text"`
	Muted      string `json:"muted"`
	Line       string `json:"line"`
	Mint       string `json:"mint"`
	Cyan       string `json:"cyan"`
	Periwinkle string `json:"periwinkle"`
	Yellow     string `json:"yellow"`
	Red        string `json:"red"`
}

var studioTheme = StudioTheme{
	Void:       "#0B0F1A",
	Surface:    "#121827",
	Raised:     "#1D2538",
	Inset:      "#080C14",
	Text:       "#E6F7FF",
	Muted:      "#A6B5C7",
	Line:       "#40506A",
	Mint:       "#00FFD1",
	Cyan:       "#33C7FF",
	Periwinkle: "#7A88FF",
	Yellow:     "#FFD84D",
	Red:        "#FF5A5A",
}

func (s *Server) renderAny(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := s.Templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) renderPage(w http.ResponseWriter, data uiData) {
	data.Theme = studioTheme
	if data.Studio == nil {
		snapshot := s.studioSnapshot()
		data.Studio = &snapshot
	}
	s.renderAny(w, "page", data)
}

func (s *Server) renderPartial(w http.ResponseWriter, name string, data uiData) {
	s.renderAny(w, name, data)
}

func (s *Server) uiRenderError(w http.ResponseWriter, r *http.Request, msg string) {
	data := uiData{
		Error: msg,
	}

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "result", data)
	} else {
		s.renderPage(w, data)
	}
}

func (s *Server) uiRenderResult(w http.ResponseWriter, r *http.Request, msg string) {
	data := uiData{
		Result: msg,
	}

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "result", data)
	} else {
		s.renderPage(w, data)
	}
}

func (s *Server) uiRenderReceiptResult(w http.ResponseWriter, r *http.Request, msg string, receiptID string) {
	data := uiData{
		Result:    msg,
		ReceiptID: receiptID,
	}

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "result", data)
	} else {
		s.renderPage(w, data)
	}
}

func toUINodes(nodes []workspace.TreeNode, workspaceID string) []UINode {
	out := make([]UINode, 0, len(nodes))

	for _, n := range nodes {
		out = append(out, UINode{
			Name:        n.Name,
			Path:        n.Path,
			Type:        n.Type,
			Size:        n.Size,
			SHA256:      n.SHA256,
			Language:    n.Language,
			WorkspaceID: workspaceID,
			Children:    toUINodes(n.Children, workspaceID),
		})
	}

	return out
}

func (s *Server) workspaceUIData(ws *workspace.Workspace, result string) uiData {
	summary := ws.Summary()

	return uiData{
		Workspaces: s.store.Summaries(),
		Workspace:  &summary,
		Binding:    ws.Binding,
		Tree:       toUINodes(ws.TreeNodes(), ws.ID),
		Result:     result,
	}
}

func (s *Server) uiHome(w http.ResponseWriter, r *http.Request) {
	data := uiData{
		Workspaces: s.store.Summaries(),
	}

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "workspace", data)
	} else {
		s.renderPage(w, data)
	}
}

func (s *Server) uiWorkspace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	data := s.workspaceUIData(ws, "")

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "workspace", data)
	} else {
		s.renderPage(w, data)
	}
}

func (s *Server) uiTree(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	nodes := toUINodes(ws.TreeNodes(), ws.ID)
	s.renderAny(w, "tree", nodes)
}

func (s *Server) uiFile(w http.ResponseWriter, r *http.Request) {
	wsID := r.URL.Query().Get("workspace")
	p := r.URL.Query().Get("path")

	if wsID == "" || p == "" {
		s.uiRenderError(w, r, "workspace and path query parameters are required")
		return
	}

	if err := workspace.IsSafePath(p); err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("invalid path: %v", err))
		return
	}

	ws, ok := s.store.Get(wsID)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", wsID))
		return
	}

	f, ok := ws.Files[p]
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("file %q not found", p))
		return
	}

	content := ""
	encoding := "utf-8"

	if utf8.Valid(f.Data) {
		content = string(f.Data)
	} else {
		content = base64.StdEncoding.EncodeToString(f.Data)
		encoding = "base64"
	}

	file := &uiFile{
		Path:     f.Path,
		Content:  content,
		Encoding: encoding,
		SHA256:   f.SHA256,
		Language: f.Language,
		Verified: f.Verified,
	}

	s.renderAny(w, "file", file)
}

func (s *Server) uiCreateFromZip(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		s.uiRenderError(w, r, "multipart file field 'file' is required")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		s.uiRenderError(w, r, fmt.Sprintf("read uploaded file: %v", err))
		return
	}

	ws, receipt, err := s.opImportZip(r, data, header.Filename)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	result := fmt.Sprintf("Imported ZIP. Receipt: %s", receipt.ID)
	ui := s.workspaceUIData(ws, result)

	if r.Header.Get("HX-Request") == "true" {
		s.renderPartial(w, "workspace", ui)
	} else {
		s.renderPage(w, ui)
	}
}

func (s *Server) uiMaterialize(w http.ResponseWriter, r *http.Request) {
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

	root := r.FormValue("root")
	confirm := r.FormValue("confirm") == "true"

	finalRoot, written, receipt, err := s.opMaterialize(r, ws, root, confirm)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Materialized %d files to %s. Receipt: %s",
		len(written),
		finalRoot,
		receipt.ID,
	)

	s.uiRenderResult(w, r, msg)
}

func (s *Server) uiBuildLedgerEvent(w http.ResponseWriter, r *http.Request) {
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

	body := []byte(r.FormValue("event_json"))

	f, receipt, err := s.opAppendEvent(r, ws, body)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Appended build-ledger event. New ledger SHA-256: %s. Receipt: %s",
		f.SHA256,
		receipt.ID,
	)

	s.uiRenderReceiptResult(w, r, msg, receipt.ID)
}

func (s *Server) uiBuildLedgerState(w http.ResponseWriter, r *http.Request) {
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

	body := []byte(r.FormValue("state_yaml"))

	f, receipt, err := s.opUpdateState(r, ws, body)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Updated build-ledger state. New SHA-256: %s. Receipt: %s",
		f.SHA256,
		receipt.ID,
	)

	s.uiRenderReceiptResult(w, r, msg, receipt.ID)
}

func (s *Server) uiBuildLedgerRun(w http.ResponseWriter, r *http.Request) {
	s.uiBuildLedgerDocument(w, r, "runs", "run", "build_ledger.run.created")
}

func (s *Server) uiBuildLedgerReceipt(w http.ResponseWriter, r *http.Request) {
	s.uiBuildLedgerDocument(w, r, "receipts", "receipt", "build_ledger.receipt.created")
}

func (s *Server) uiBuildLedgerVerification(w http.ResponseWriter, r *http.Request) {
	s.uiBuildLedgerDocument(w, r, "verification", "verification", "build_ledger.verification.created")
}

func (s *Server) uiBuildLedgerDocument(
	w http.ResponseWriter,
	r *http.Request,
	dir string,
	idPrefix string,
	action string,
) {
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

	body := []byte(r.FormValue("document_yaml"))

	docID, f, receipt, err := s.opCreateDocument(r, ws, body, dir, idPrefix, action)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Created build-ledger document %s. New SHA-256: %s. Receipt: %s",
		docID,
		f.SHA256,
		receipt.ID,
	)

	s.uiRenderReceiptResult(w, r, msg, receipt.ID)
}
