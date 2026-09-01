package snapshot

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/replay"
	"flatten-workspace/internal/workspace"
)

type Loaded struct {
	Meta       map[string]any
	Projection *projection.Projection
	Events     []eventlog.Event
	Receipts   []receipts.Receipt
	Files      map[string][]byte
	Unsafe     []string
}

func ReadZip(data []byte) (*Loaded, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	loaded := &Loaded{
		Files:  map[string][]byte{},
		Unsafe: []string{},
	}

	for _, zf := range zr.File {
		name := path.Clean(zf.Name)

		if strings.HasSuffix(name, "/") {
			continue
		}

		if strings.HasPrefix(name, "/") || strings.Contains(name, "..") {
			loaded.Unsafe = append(loaded.Unsafe, zf.Name)
			continue
		}

		rc, err := zf.Open()
		if err != nil {
			loaded.Unsafe = append(loaded.Unsafe, zf.Name)
			continue
		}

		content, err := io.ReadAll(rc)
		rc.Close()

		if err != nil {
			loaded.Unsafe = append(loaded.Unsafe, zf.Name)
			continue
		}

		switch {
		case name == "snapshot/meta.json":
			var meta map[string]any
			if err := json.Unmarshal(content, &meta); err == nil {
				loaded.Meta = meta
			}

		case name == "snapshot/projection.json":
			var proj projection.Projection
			if err := json.Unmarshal(content, &proj); err == nil {
				loaded.Projection = &proj
			}

		case name == "snapshot/events.jsonl":
			lines := strings.Split(string(content), "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				var ev eventlog.Event
				if err := json.Unmarshal([]byte(line), &ev); err == nil {
					loaded.Events = append(loaded.Events, ev)
				}
			}

		case strings.HasPrefix(name, "receipts/"):
			var r receipts.Receipt
			if err := json.Unmarshal(content, &r); err == nil {
				loaded.Receipts = append(loaded.Receipts, r)
			}

		case strings.HasPrefix(name, "workspace/"):
			rel := strings.TrimPrefix(name, "workspace/")
			if rel == "" {
				continue
			}

			if err := workspace.IsSafePath(rel); err != nil {
				loaded.Unsafe = append(loaded.Unsafe, name)
				continue
			}

			loaded.Files[rel] = content

		default:
			// Ignore unknown snapshot members.
		}
	}

	return loaded, nil
}

func WorkspaceIDFromLoaded(loaded *Loaded) string {
	if loaded.Meta != nil {
		if id, ok := loaded.Meta["workspace_id"].(string); ok && id != "" {
			return id
		}
	}

	for _, ev := range loaded.Events {
		if ev.WorkspaceID != "" {
			return ev.WorkspaceID
		}
	}

	return ""
}

func VerifyLoaded(loaded *Loaded, workspaceID string) (map[string]any, error) {
	if workspaceID == "" {
		workspaceID = WorkspaceIDFromLoaded(loaded)
	}

	current := projection.BuildFromFiles(loaded.Files)

	snapshotHash := ""
	projectionOK := false

	if loaded.Projection != nil {
		snapshotHash = loaded.Projection.Hash
		projectionOK = loaded.Projection.Hash == current.Hash
	}

	ledgerFiles := map[string][]byte{}

	for p, data := range loaded.Files {
		if strings.HasPrefix(p, "build-ledger/") {
			ledgerFiles[p] = data
		}
	}

	currentLedger := projection.BuildFromFiles(ledgerFiles)

	for _, event := range loaded.Events {
		if event.Type != "transition.authority.accepted" {
			continue
		}
		// Snapshot archives are portable input, not a live Rust acknowledgement
		// source. A syntactically valid copied ACK cannot establish authority.
		return nil, fmt.Errorf("untrusted snapshot archive contains typed accepted event %q; live Rust authority is required", event.ID)
	}

	replayed, replayErr := replay.BuildLedgerProjectionFromEvents(loaded.Events, workspaceID)

	replayOK := replayErr == nil && projection.Equivalent(currentLedger, replayed)

	result := map[string]any{
		"workspace_id":             workspaceID,
		"file_count":               len(loaded.Files),
		"event_count":              len(loaded.Events),
		"receipt_count":            len(loaded.Receipts),
		"unsafe_snapshot_entries":  loaded.Unsafe,
		"projection_ok":            projectionOK,
		"current_projection_hash":  current.Hash,
		"snapshot_projection_hash": snapshotHash,
		"replay_ok":                replayOK,
		"current_ledger_hash":      currentLedger.Hash,
		"replayed_ledger_hash":     replayed.Hash,
	}

	if replayErr != nil {
		result["replay_error"] = replayErr.Error()
	}

	return result, nil
}
