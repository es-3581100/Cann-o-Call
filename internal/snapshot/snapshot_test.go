package snapshot

import (
	"archive/zip"
	"bytes"
	"testing"

	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/receipts"
	"flatten-workspace/internal/workspace"
)

func TestWriteZip(t *testing.T) {
	ws := &workspace.Workspace{
		ID: "ws-test",
		Source: workspace.Source{
			Name: "test-workspace",
		},
		Files: map[string]*workspace.File{
			"a.txt": {
				Path: "a.txt",
				Data: []byte("hello"),
			},
		},
	}

	events := []eventlog.Event{
		{
			ID:          "event-test",
			WorkspaceID: "ws-test",
			Action:      "test.action",
			Status:      "accepted",
		},
	}

	allReceipts := []receipts.Receipt{
		{
			ID:          "receipt-test",
			WorkspaceID: "ws-test",
			Action:      "test.action",
			Status:      "delivered",
		},
	}

	buf := new(bytes.Buffer)

	if err := WriteZip(ws, events, allReceipts, "snapshot-test", buf); err != nil {
		t.Fatalf("WriteZip failed: %v", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("open snapshot zip: %v", err)
	}

	names := map[string]bool{}

	for _, f := range zr.File {
		names[f.Name] = true
	}

	expected := []string{
		"snapshot/meta.json",
		"snapshot/projection.json",
		"snapshot/events.jsonl",
		"receipts/receipt-test.json",
		"workspace/a.txt",
	}

	for _, name := range expected {
		if !names[name] {
			t.Fatalf("expected snapshot to contain %q", name)
		}
	}
}
