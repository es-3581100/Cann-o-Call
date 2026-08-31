package quarantine

import (
	"testing"
	"time"
)

func TestQuarantineStorePersistence(t *testing.T) {
	dir := t.TempDir()

	s1, err := New(dir)
	if err != nil {
		t.Fatalf("create first quarantine store: %v", err)
	}

	err = s1.Add(&Item{
		ID:           "quarantine-test",
		WorkspaceID:  "ws-test",
		OriginalPath: "../evil.txt",
		SafePath:     "quarantine/evil.txt",
		Status:       "quarantined",
		SHA256:       "test",
		Size:         4,
		Data:         []byte("evil"),
	})
	if err != nil {
		t.Fatalf("add quarantine item: %v", err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("create second quarantine store: %v", err)
	}

	items := s2.ListWorkspace("ws-test")
	if len(items) != 1 {
		t.Fatalf("expected 1 persisted quarantine item, got %d", len(items))
	}

	now := time.Now().UTC().Format(time.RFC3339)

	err = s2.UpdateStatus(
		"ws-test",
		"quarantine-test",
		"approved",
		"receipt-test",
		now,
		"quarantine/admitted-evil.txt",
	)
	if err != nil {
		t.Fatalf("update quarantine status: %v", err)
	}

	s3, err := New(dir)
	if err != nil {
		t.Fatalf("create third quarantine store: %v", err)
	}

	item, ok := s3.Get("ws-test", "quarantine-test")
	if !ok {
		t.Fatal("expected quarantine item after reload")
	}

	if item.Status != "approved" {
		t.Fatalf("expected approved status, got %s", item.Status)
	}

	if item.TargetPath != "quarantine/admitted-evil.txt" {
		t.Fatalf("expected target path to persist, got %s", item.TargetPath)
	}
}
