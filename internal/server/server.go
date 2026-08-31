package server

import (
	"archive/zip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/authority"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/quarantine"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
	"flatten-workspace/web"
)

type Store struct {
	mu         sync.RWMutex
	workspaces map[string]*workspace.Workspace
}

func NewStore() *Store {
	return &Store{workspaces: map[string]*workspace.Workspace{}}
}

func (s *Store) Add(ws *workspace.Workspace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workspaces[ws.ID] = ws
}

func (s *Store) Get(id string) (*workspace.Workspace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ws, ok := s.workspaces[id]
	return ws, ok
}

func (s *Store) Summaries() []workspace.Summary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]workspace.Summary, 0, len(s.workspaces))
	for _, ws := range s.workspaces {
		out = append(out, ws.Summary())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

type Server struct {
	store             *Store
	handler           http.Handler
	Events            *eventlog.Service
	Receipts          *receipts.Service
	Authority         *authority.Service
	DataDir           string
	AllowAbsoluteRoot bool
	Templates         *template.Template
	Quarantine        *quarantine.Store
	Actors            *actorstub.Controller
}

func New() *Server {
	dataDir := envOr("DATA_DIR", "data")

	events, err := eventlog.New(filepath.Join(dataDir, "events"), os.Getenv("RUST_LEDGER_URL"))
	if err != nil {
		panic(err)
	}

	receiptStore, err := receipts.New(filepath.Join(dataDir, "receipts"))
	if err != nil {
		panic(err)
	}

	quarantineStore, err := quarantine.New(filepath.Join(dataDir, "quarantine"))
	if err != nil {
		panic(err)
	}

	actorMax := 16
	if v := os.Getenv("ACTOR_MAX_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			actorMax = n
		}
	}
	actorTTL := 5 * time.Minute
	if v := os.Getenv("ACTOR_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			actorTTL = d
		}
	}

	s := &Server{
		store:             NewStore(),
		Events:            events,
		Receipts:          receiptStore,
		Authority:         authority.New(os.Getenv("AUTHORITY_TOKEN")),
		DataDir:           dataDir,
		AllowAbsoluteRoot: os.Getenv("ALLOW_ABSOLUTE_ROOT") == "true",
		Templates:         template.Must(template.ParseFS(web.FS, "templates/*.html")),
		Quarantine:        quarantineStore,
		Actors:            actorstub.New(actorMax, actorTTL),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Current JSON surface.
	mux.HandleFunc("POST /api/workspaces", s.jsonCreateWorkspaceFromEnvelope)
	mux.HandleFunc("POST /api/workspaces/from-zip", s.jsonCreateWorkspaceFromZip)
	mux.HandleFunc("GET /api/workspaces", s.listWorkspaces)
	mux.HandleFunc("GET /api/workspaces/{id}", s.getWorkspace)
	mux.HandleFunc("GET /api/workspaces/{id}/tree", s.getTree)
	mux.HandleFunc("GET /api/workspaces/{id}/file", s.getFile)
	mux.HandleFunc("GET /api/workspaces/{id}/zip", s.getZip)
	mux.HandleFunc("GET /api/workspaces/{id}/envelope", s.getEnvelope)
	mux.HandleFunc("GET /api/workspaces/{id}/snapshot", s.snapshotWorkspace)
	mux.HandleFunc("GET /api/workspaces/{id}/replay/verify", s.jsonVerifyReplay)
	mux.HandleFunc("GET /api/workspaces/{id}/verification-report", s.verificationReport)
	mux.HandleFunc("GET /api/workspaces/{id}/quarantine", s.jsonQuarantine)
	mux.HandleFunc("GET /api/workspaces/{id}/actors", s.jsonWorkspaceActors)
	mux.HandleFunc("GET /api/receipts", s.jsonListReceipts)
	mux.HandleFunc("GET /api/receipts/verify", s.jsonVerifyReceipts)
	mux.HandleFunc("GET /api/receipts/{receiptID}/diff", s.jsonReceiptDiff)
	mux.HandleFunc("GET /api/ledger/verify", s.jsonVerifyLedger)
	mux.HandleFunc("POST /api/checkpoints", s.createCheckpoint)
	mux.HandleFunc("POST /api/snapshots/verify", s.jsonVerifySnapshot)

	mux.HandleFunc("POST /api/workspaces/{id}/materialize", s.jsonMaterializeWorkspace)
	mux.HandleFunc("POST /api/workspaces/{id}/bind", s.jsonBindWorkspace)
	mux.HandleFunc("POST /api/workspaces/{id}/build-ledger/current/state", s.jsonUpdateBuildLedgerState)
	mux.HandleFunc("POST /api/workspaces/{id}/build-ledger/events", s.jsonAppendBuildLedgerEvent)
	mux.HandleFunc("POST /api/workspaces/{id}/build-ledger/runs", s.jsonCreateBuildLedgerRun)
	mux.HandleFunc("POST /api/workspaces/{id}/build-ledger/receipts", s.jsonCreateBuildLedgerReceipt)
	mux.HandleFunc("POST /api/workspaces/{id}/build-ledger/verification", s.jsonCreateBuildLedgerVerification)
	mux.HandleFunc("POST /api/workspaces/{id}/quarantine/decisions", s.jsonQuarantineDecision)
	mux.HandleFunc("POST /api/workspaces/{id}/quarantine/blobs/{blobID}/admit", s.jsonAdmitQuarantineBlob)
	mux.HandleFunc("POST /api/workspaces/{id}/quarantine/blobs/{blobID}/reject", s.jsonRejectQuarantineBlob)

	// HTMX/server-rendered control surface.
	mux.HandleFunc("GET /ui", s.uiHome)
	mux.HandleFunc("GET /ui/workspaces/{id}", s.uiWorkspace)
	mux.HandleFunc("GET /ui/workspaces/{id}/tree", s.uiTree)
	mux.HandleFunc("GET /ui/workspaces/{id}/quarantine", s.uiQuarantine)
	mux.HandleFunc("GET /ui/workspaces/{id}/actors", s.uiWorkspaceActors)
	mux.HandleFunc("GET /ui/workspaces/{id}/replay/verify", s.uiVerifyReplay)
	mux.HandleFunc("GET /ui/ledger/verify", s.uiVerifyLedger)
	mux.HandleFunc("GET /ui/receipts/verify", s.uiVerifyReceipts)
	mux.HandleFunc("GET /ui/receipts/{receiptID}/diff", s.uiReceiptDiff)
	mux.HandleFunc("GET /ui/file", s.uiFile)
	mux.HandleFunc("POST /ui/workspaces/from-envelope", s.uiCreateFromEnvelope)
	mux.HandleFunc("POST /ui/workspaces/from-zip", s.uiCreateFromZip)
	mux.HandleFunc("POST /ui/workspaces/{id}/materialize", s.uiMaterialize)
	mux.HandleFunc("POST /ui/workspaces/{id}/bind", s.uiBindWorkspace)
	mux.HandleFunc("POST /ui/workspaces/{id}/build-ledger/event", s.uiBuildLedgerEvent)
	mux.HandleFunc("POST /ui/workspaces/{id}/build-ledger/state", s.uiBuildLedgerState)
	mux.HandleFunc("POST /ui/workspaces/{id}/build-ledger/run", s.uiBuildLedgerRun)
	mux.HandleFunc("POST /ui/workspaces/{id}/build-ledger/receipt", s.uiBuildLedgerReceipt)
	mux.HandleFunc("POST /ui/workspaces/{id}/build-ledger/verification", s.uiBuildLedgerVerification)
	mux.HandleFunc("POST /ui/workspaces/{id}/quarantine/decisions", s.uiQuarantineDecision)
	mux.HandleFunc("POST /ui/workspaces/{id}/quarantine/blobs/{blobID}/admit", s.uiAdmitQuarantineBlob)
	mux.HandleFunc("POST /ui/workspaces/{id}/quarantine/blobs/{blobID}/reject", s.uiRejectQuarantineBlob)
	mux.HandleFunc("POST /ui/snapshots/verify", s.uiVerifySnapshot)
	mux.Handle("GET /", http.RedirectHandler("/ui", http.StatusFound))

	s.handler = mux
	return s
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (s *Server) Handler() http.Handler { return s.handler }

// Legacy envelope handler retained as a donor/reference implementation.
func (s *Server) createWorkspace(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 256<<20)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("read request body: %w", err))
		return
	}
	if len(data) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("empty body"))
		return
	}
	ws, err := workspace.Parse(data)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	s.store.Add(ws)
	writeJSON(w, http.StatusCreated, ws.Summary())
}

func (s *Server) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Summaries())
}

func (s *Server) getWorkspace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, ws.Summary())
}

func (s *Server) getTree(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, ws.TreeNodes())
}

func (s *Server) getFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p := r.URL.Query().Get("path")
	if p == "" {
		writeError(w, http.StatusBadRequest, errors.New("path query parameter is required"))
		return
	}
	if err := workspace.IsSafePath(p); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid path: %w", err))
		return
	}
	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}
	f, ok := ws.Files[p]
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("file %q not found", p))
		return
	}
	contentEncoding := "utf-8"
	content := ""
	if utf8.Valid(f.Data) {
		content = string(f.Data)
	} else {
		content = base64.StdEncoding.EncodeToString(f.Data)
		contentEncoding = "base64"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"path": f.Path, "size": f.Size, "sha256": f.SHA256,
		"declared_sha256": f.DeclaredSHA256, "encoding": f.Encoding,
		"kind": f.Kind, "language": f.Language, "media_type": f.MediaType,
		"verified": f.Verified, "content": content, "content_encoding": contentEncoding,
	})
}

func (s *Server) getZip(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ws, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("workspace %q not found", id))
		return
	}
	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.zip"`, ws.ID))
	zw := zip.NewWriter(w)
	for _, p := range paths {
		fw, err := zw.Create(p)
		if err != nil {
			return
		}
		if _, err := fw.Write(ws.Files[p].Data); err != nil {
			return
		}
	}
	_ = zw.Close()
}

func (s *Server) jsonListReceipts(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Receipts.List())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
