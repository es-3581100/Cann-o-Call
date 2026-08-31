package actorstub

import (
	"testing"
	"time"
)

func TestActorActivationBounds(t *testing.T) {
	c := New(1, time.Minute)

	first := c.Activate("ws-1", "build_ledger.state.updated", "build-ledger/current/state.yaml")
	if first == nil {
		t.Fatal("expected first activation")
	}

	if first.Status != "active" {
		t.Fatalf("expected first activation active, got %s", first.Status)
	}

	duplicate := c.Activate("ws-1", "build_ledger.state.updated", "build-ledger/current/state.yaml")
	if duplicate != nil {
		t.Fatal("expected duplicate activation to be deduplicated")
	}

	second := c.Activate("ws-1", "build_ledger.event.appended", "build-ledger/events/ledger.jsonl")
	if second == nil {
		t.Fatal("expected second activation record")
	}

	if second.Status != "rejected_budget" {
		t.Fatalf("expected second activation rejected by budget, got %s", second.Status)
	}
}
