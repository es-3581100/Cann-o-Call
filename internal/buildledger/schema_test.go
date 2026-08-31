package buildledger

import "testing"

func TestValidateStateValid(t *testing.T) {
	doc := map[string]any{
		"id":       "state-msl-0001",
		"type":     "project",
		"revision": 1,
		"status":   "in_progress",
	}

	if err := ValidateState(doc); err != nil {
		t.Fatalf("expected valid state, got: %v", err)
	}
}

func TestValidateStateMissingStatus(t *testing.T) {
	doc := map[string]any{
		"id":       "state-msl-0001",
		"type":     "project",
		"revision": 1,
	}

	if err := ValidateState(doc); err == nil {
		t.Fatal("expected missing status to fail")
	}
}

func TestValidateRunValid(t *testing.T) {
	doc := map[string]any{
		"id":        "run-0001",
		"type":      "run",
		"status":    "in_progress",
		"objective": "Implement replay verification",
	}

	if err := ValidateRun(doc); err != nil {
		t.Fatalf("expected valid run, got: %v", err)
	}
}

func TestValidateEventValid(t *testing.T) {
	doc := map[string]any{
		"id":     "event-0001",
		"type":   "event",
		"status": "recorded",
		"event":  "phase3_started",
	}

	if err := ValidateEvent(doc); err != nil {
		t.Fatalf("expected valid event, got: %v", err)
	}
}
