package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/orchestrator"
	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/workspace"
)

// ensureOrchestrator returns deterministic orchestrator+store for workspace.
func (s *Server) ensureOrchestrator(workspaceID string) *orchestrator.Orchestrator {
	s.muOrch.Lock()
	defer s.muOrch.Unlock()
	if o, ok := s.orchestrators[workspaceID]; ok {
		return o
	}
	gs := graph.NewStore(workspaceID)
	s.graphStores[workspaceID] = gs
	cfg := scoring.Config{}.WithDefaults()
	o := orchestrator.New(workspaceID, gs, s.Authority, s.Actors, cfg)
	s.orchestrators[workspaceID] = o
	return o
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	workspaceID := r.URL.Query().Get("workspace")
	if workspaceID == "" {
		workspaceID = r.FormValue("workspace")
	}
	if workspaceID == "" {
		workspaceID = "default"
	}
	// Create task
	task, err := s.beginTask(progress.OperationIngest, workspaceID, r.URL.Query().Get("path"), progress.PhaseRequestAccepted)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	taskID := task.Packet.TaskID

	// Determine source: multipart zip or JSON body
	var ws *workspace.Workspace
	var packets []ingest.SourcePacket
	var ingestErr error

	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err == nil {
			file, header, ferr := r.FormFile("file")
			if ferr == nil {
				b, _ := io.ReadAll(file)
				file.Close()
				packets, ws, ingestErr = ingest.IngestZipBytes(header.Filename, b, workspaceID)
			} else {
				ingestErr = fmt.Errorf("multipart file required: %w", ferr)
			}
		} else {
			ingestErr = err
		}
	} else {
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			ingestErr = fmt.Errorf("empty ingest body")
		} else {
			// Try envelope first, then raw file
			packets, ws, ingestErr = ingest.ParseEnvelopeBytes(body)
			if ingestErr != nil {
				// Try single file ingest via query param path
				srcRef := r.URL.Query().Get("path")
				if srcRef == "" {
					srcRef = "ingest.bin"
				}
				var pkt *ingest.SourcePacket
				pkt, ingestErr = ingest.IngestBytes(workspaceID, srcRef, "", body, srcRef)
				if ingestErr == nil {
					packets = []ingest.SourcePacket{*pkt}
					// Build minimal workspace for storage? Not needed.
					ws, _ = workspace.Parse(body)
				}
			}
		}
	}

	if ingestErr != nil {
		s.failTask(taskID, ingestErr.Error())
		writeError(w, http.StatusBadRequest, ingestErr)
		return
	}

	// Apply through orchestrator authority path if ws available
	orch := s.ensureOrchestrator(workspaceID)
	var applied int
	for _, pkt := range packets {
		// Ensure workspaceID matches orchestrator
		if pkt.Identity.WorkspaceID != workspaceID {
			continue
		}
		_, err := orch.Ingest(pkt)
		if err != nil {
			// Record warning but continue deterministically
			_ = s.Tasks.AddWarning(taskID, fmt.Sprintf("ingest packet %s: %v", pkt.Identity.SourceID, err))
			continue
		}
		applied++
	}
	// If ws derived from zip, also store workspace in server store
	if ws != nil {
		if ws.ID == "" {
			ws.ID = workspaceID
		}
		s.store.Add(ws)
	}
	_ = s.Tasks.Update(taskID, func(p *progress.ProgressPacket) {
		total := int64(len(packets))
		completed := int64(applied)
		p.Completed = &completed
		p.Total = &total
		p.Phase = progress.PhaseDurableAppend
	})
	s.completeTask(taskID, progress.StatusComplete, "")

	writeJSON(w, http.StatusOK, map[string]any{
		"task_id":      taskID,
		"workspace_id": workspaceID,
		"ingested":     applied,
		"total":        len(packets),
		"summary":      wsSummary(ws),
	})
}

func wsSummary(ws *workspace.Workspace) any {
	if ws == nil {
		return nil
	}
	return ws.Summary()
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query       string `json:"query"`
		WorkspaceID string `json:"workspace_id"`
		RequestID   string `json:"request_id"`
	}
	body, _ := io.ReadAll(r.Body)
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decode query: %w", err))
			return
		}
	}
	// Fallback to query params
	if req.Query == "" {
		req.Query = r.URL.Query().Get("query")
	}
	if req.WorkspaceID == "" {
		req.WorkspaceID = r.URL.Query().Get("workspace")
	}
	if req.RequestID == "" {
		req.RequestID = r.URL.Query().Get("request_id")
	}
	if req.Query == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("query required"))
		return
	}
	if req.WorkspaceID == "" {
		req.WorkspaceID = "default"
	}
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("req-%d", len(req.Query))
	}
	task, err := s.beginTask(progress.OperationQuery, req.WorkspaceID, req.Query, progress.PhaseRequestAccepted)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	taskID := task.Packet.TaskID

	orch := s.ensureOrchestrator(req.WorkspaceID)
	result, qerr := orch.Query(req.RequestID, req.Query, "")
	if qerr != nil {
		s.failTask(taskID, qerr.Error())
		writeError(w, http.StatusInternalServerError, qerr)
		return
	}
	// Update task metrics with result counts
	_ = s.Tasks.Update(taskID, func(p *progress.ProgressPacket) {
		total := int64(result.CandidatesConsidered)
		completed := int64(len(result.Selected))
		p.Completed = &completed
		p.Total = &total
		p.Phase = progress.PhaseResultsProduced
	})
	s.completeTask(taskID, progress.StatusComplete, "")

	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": taskID,
		"result":  result,
	})
}

func (s *Server) handleTaskReceipt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.ProgressReceipts == nil {
		writeError(w, http.StatusNotFound, fmt.Errorf("receipt store not initialized"))
		return
	}
	rp, ok := s.ProgressReceipts.Load(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("receipt for task %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, rp)
}

// Wire existing ingest paths to tasks: patch opImport methods
func (s *Server) wrapImportTask(op progress.Operation, workspaceID, key string, fn func() (*workspace.Workspace, error)) (*workspace.Workspace, error) {
	task, err := s.beginTask(op, workspaceID, key, progress.PhaseRequestAccepted)
	if err != nil {
		// fallback directly
		return fn()
	}
	ws, ferr := fn()
	if ferr != nil {
		s.failTask(task.Packet.TaskID, ferr.Error())
		return nil, ferr
	}
	// Success: complete
	_ = s.Tasks.Update(task.Packet.TaskID, func(p *progress.ProgressPacket) {
		c := int64(1)
		t := int64(1)
		p.Completed = &c
		p.Total = &t
		p.Phase = progress.PhaseTerminal
	})
	s.completeTask(task.Packet.TaskID, progress.StatusComplete, "")
	return ws, nil
}

// snapshot and checkpoint task wiring helpers called from existing handlers.
func (s *Server) withCheckpointTask(fn func() (map[string]any, error)) (map[string]any, error) {
	task, err := s.beginTask(progress.OperationCheckpoint, "global", "", progress.PhaseRequestAccepted)
	if err != nil {
		return fn()
	}
	res, ferr := fn()
	if ferr != nil {
		s.failTask(task.Packet.TaskID, ferr.Error())
		return nil, ferr
	}
	s.completeTask(task.Packet.TaskID, progress.StatusComplete, "")
	return res, nil
}

func ensureDataDir() string {
	if v := os.Getenv("DATA_DIR"); v != "" {
		return v
	}
	return "data"
}

func localPathJoin(elem ...string) string {
	return filepath.Join(elem...)
}
