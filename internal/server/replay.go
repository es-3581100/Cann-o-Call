package server

import (
	"fmt"
	"net/http"

	"flatten-workspace/internal/progress"
	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/replay"
	"flatten-workspace/internal/transition"
	"flatten-workspace/internal/workspace"
)

func (s *Server) opVerifyReplay(ws *workspace.Workspace) (map[string]any, error) {
	// Replay of authority decisions is sourced from the Rust recorder, not the
	// local Go mirror. With no configured recorder this is intentionally empty.
	events, err := s.Events.RustAcknowledgedEvents()
	if err != nil {
		return nil, err
	}
	// Validate the typed authority stream before interpreting legacy build-ledger
	// details. Legacy CHUNK-02 events remain valid inputs to the latter replay.
	if _, err := transition.Rebuild(events, s.Events, nil); err != nil {
		return nil, fmt.Errorf("rebuild typed authority stream: %w", err)
	}

	current := projection.BuildLedgerFromWorkspace(ws)

	replayed, err := replay.BuildLedgerProjectionFromEvents(events, ws.ID)
	if err != nil {
		return nil, err
	}

	equivalent := projection.Equivalent(current, replayed)

	return map[string]any{
		"equivalent":          equivalent,
		"current_hash":        current.Hash,
		"replayed_hash":       replayed.Hash,
		"current_file_count":  current.FileCount,
		"replayed_file_count": replayed.FileCount,
		"current_dir_count":   current.DirectoryCount,
		"replayed_dir_count":  replayed.DirectoryCount,
	}, nil
}

func (s *Server) jsonVerifyReplay(w http.ResponseWriter, r *http.Request) {
	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}
	task, _ := s.beginTask(progress.OperationReplay, ws.ID, ws.ID, progress.PhaseRequestAccepted)
	var taskID string
	if task != nil {
		taskID = task.Packet.TaskID
	}
	result, err := s.opVerifyReplay(ws)
	if err != nil {
		if taskID != "" {
			s.failTask(taskID, err.Error())
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if taskID != "" {
		s.completeTask(taskID, progress.StatusComplete, "")
		result["task_id"] = taskID
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) uiVerifyReplay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, fmt.Sprintf("workspace %q not found", id))
		return
	}

	result, err := s.opVerifyReplay(ws)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Replay equivalent: %v\ncurrent hash: %v\nreplayed hash: %v\ncurrent files: %v\nreplayed files: %v",
		result["equivalent"],
		result["current_hash"],
		result["replayed_hash"],
		result["current_file_count"],
		result["replayed_file_count"],
	)

	s.uiRenderResult(w, r, msg)
}
