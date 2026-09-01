package server

import (
	"flatten-workspace/internal/quarantine"
	"flatten-workspace/internal/workspace"
)

func (s *Server) syncWorkspaceQuarantine(ws *workspace.Workspace) {
	if s.Quarantine == nil {
		return
	}

	currentIDs := map[string]bool{}

	for _, blob := range ws.QuarantinedBlobs {
		currentIDs[blob.ID] = true

		item := &quarantine.Item{
			ID:           blob.ID,
			WorkspaceID:  ws.ID,
			OriginalPath: blob.OriginalPath,
			SafePath:     blob.SafePath,
			TargetPath:   blob.TargetPath,
			Reason:       blob.Reason,
			Status:       blob.Status,
			SHA256:       blob.SHA256,
			Size:         blob.Size,
			DecidedAt:    blob.DecidedAt,
			ReceiptID:    blob.ReceiptID,
			Data:         blob.Data,
		}

		_ = s.Quarantine.Add(item)
	}

	persisted := s.Quarantine.ListWorkspace(ws.ID)

	blobs := []*workspace.QuarantinedBlob{}

	for _, item := range persisted {
		if !currentIDs[item.ID] {
			continue
		}

		blob := &workspace.QuarantinedBlob{
			ID:           item.ID,
			OriginalPath: item.OriginalPath,
			SafePath:     item.SafePath,
			TargetPath:   item.TargetPath,
			Reason:       item.Reason,
			Status:       item.Status,
			SHA256:       item.SHA256,
			Size:         item.Size,
			DecidedAt:    item.DecidedAt,
			ReceiptID:    item.ReceiptID,
			Data:         item.Data,
		}

		blobs = append(blobs, blob)

	}

	ws.QuarantinedBlobs = blobs
}
