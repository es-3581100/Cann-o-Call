package receipts

import (
	"testing"
	"time"
)

func TestReceiptsReloadFromDisk(t *testing.T) {
	dir := t.TempDir()

	s1, err := New(dir)
	if err != nil {
		t.Fatalf("create first receipt service: %v", err)
	}

	original := Receipt{
		ID:              "receipt-test-0001",
		WorkspaceID:     "ws-test",
		Action:          "test.action",
		Objective:       "Test receipt durability",
		Status:          "delivered",
		AuthoritySource: "test",
		EventID:         "event-test-0001",
		CreatedAt:       time.Now().UTC(),
		Details: map[string]any{
			"path": "build-ledger/current/state.yaml",
		},
	}

	if _, err := s1.Save(original); err != nil {
		t.Fatalf("save receipt: %v", err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("create second receipt service: %v", err)
	}

	loaded, ok := s2.Get("receipt-test-0001")
	if !ok {
		t.Fatal("expected receipt to be reloaded from disk")
	}

	if loaded.Action != original.Action {
		t.Fatalf("expected action %q, got %q", original.Action, loaded.Action)
	}

	if loaded.Hash == "" {
		t.Fatal("expected reloaded receipt to have a hash")
	}
}
