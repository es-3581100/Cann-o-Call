package snapshot

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"time"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/projection"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

type Meta struct {
	SnapshotID   string                `json:"snapshot_id"`
	CreatedAt    time.Time             `json:"created_at"`
	WorkspaceID  string                `json:"workspace_id"`
	Source       workspace.Source      `json:"source"`
	Summary      workspace.Summary     `json:"summary"`
	Projection   projection.Projection `json:"projection"`
	EventCount   int                   `json:"event_count"`
	ReceiptCount int                   `json:"receipt_count"`
}

func WriteZip(
	ws *workspace.Workspace,
	events []eventlog.Event,
	allReceipts []receipts.Receipt,
	snapshotID string,
	w io.Writer,
) error {
	filteredEvents := []eventlog.Event{}
	for _, ev := range events {
		if ev.WorkspaceID == ws.ID {
			filteredEvents = append(filteredEvents, ev)
		}
	}

	filteredReceipts := []receipts.Receipt{}
	for _, r := range allReceipts {
		if r.WorkspaceID == ws.ID {
			filteredReceipts = append(filteredReceipts, r)
		}
	}

	proj := projection.BuildFromWorkspace(ws)

	meta := Meta{
		SnapshotID:   snapshotID,
		CreatedAt:    time.Now().UTC(),
		WorkspaceID:  ws.ID,
		Source:       ws.Source,
		Summary:      ws.Summary(),
		Projection:   proj,
		EventCount:   len(filteredEvents),
		ReceiptCount: len(filteredReceipts),
	}

	zw := zip.NewWriter(w)

	if err := addJSON(zw, "snapshot/meta.json", meta); err != nil {
		return err
	}

	if err := addJSON(zw, "snapshot/projection.json", proj); err != nil {
		return err
	}

	if err := addFile(zw, "snapshot/events.jsonl", eventsJSONL(filteredEvents)); err != nil {
		return err
	}

	for _, r := range filteredReceipts {
		name := fmt.Sprintf("receipts/%s.json", path.Base(r.ID))
		if err := addJSON(zw, name, r); err != nil {
			return err
		}
	}

	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		f, ok := ws.Files[p]
		if !ok {
			continue
		}

		name := path.Join("workspace", p)

		if err := addFile(zw, name, f.Data); err != nil {
			return err
		}
	}

	return zw.Close()
}

func addFile(zw *zip.Writer, name string, data []byte) error {
	fw, err := zw.Create(name)
	if err != nil {
		return err
	}

	if _, err := fw.Write(data); err != nil {
		return err
	}

	return nil
}

func addJSON(zw *zip.Writer, name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return addFile(zw, name, b)
}

func eventsJSONL(events []eventlog.Event) []byte {
	var buf bytes.Buffer

	for _, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}

		buf.Write(b)
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}
