package server

import (
	"net/http"
	"strings"
)

func (s *Server) maybeActivateActor(workspaceID, action string, details map[string]any) {
	if s.Actors == nil {
		return
	}

	if !strings.HasPrefix(action, "build_ledger.") && action != "quarantine.blob.admitted" {
		return
	}

	path := ""

	if details != nil {
		if v, ok := details["path"].(string); ok {
			path = v
		}

		if path == "" {
			if v, ok := details["target_path"].(string); ok {
				path = v
			}
		}
	}

	_ = s.Actors.Activate(workspaceID, action, path)
}

func (s *Server) jsonWorkspaceActors(w http.ResponseWriter, r *http.Request) {
	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	if s.Actors == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}

	writeJSON(w, http.StatusOK, s.Actors.List(ws.ID))
}

func (s *Server) uiWorkspaceActors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ws, ok := s.store.Get(id)
	if !ok {
		s.uiRenderError(w, r, "workspace not found")
		return
	}

	if s.Actors == nil {
		s.renderAny(w, "actors", []any{})
		return
	}

	s.renderAny(w, "actors", s.Actors.List(ws.ID))
}
