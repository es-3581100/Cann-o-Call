package server

import (
	"fmt"
	"net/http"

	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/replay"
	"flatten-workspace/internal/workspace"
)

func (s *Server) opVerifyReplay(ws *workspace.Workspace) (map[string]any, error) {
	events, err := s.Events.List()
	if err != nil {
		return nil, err
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

	result, err := s.opVerifyReplay(ws)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
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
