package server

import (
	"fmt"
	"net/http"

	"flatten-workspace/internal/ids"
	"flatten-workspace/internal/snapshot"
)

func (s *Server) snapshotWorkspace(w http.ResponseWriter, r *http.Request) {
	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	snapshotID := ids.New("snapshot")

	_, err := s.recordGlobalTransition(
		r,
		ws.ID,
		"workspace.snapshot.exported",
		"Export workspace snapshot bundle",
		nil,
		map[string]any{
			"snapshot_id": snapshotID,
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record snapshot transition: %w", err))
		return
	}

	events, err := s.Events.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("read events: %w", err))
		return
	}

	allReceipts := s.Receipts.List()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s.zip"`, snapshotID),
	)

	if err := snapshot.WriteZip(ws, events, allReceipts, snapshotID, w); err != nil {
		return
	}
}
