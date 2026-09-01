package eventlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

func (s *Service) List() ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.path, os.O_RDONLY, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return []Event{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)

	events := []Event{}

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}

		events = append(events, ev)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

// CheckedEvents reads the complete Go ledger and rejects malformed or broken
// records. List remains intentionally best-effort for legacy display paths;
// state reconstruction must use this method instead.
func (s *Service) CheckedEvents() ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.path, os.O_RDONLY, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return []Event{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	events := []Event{}
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("malformed event log line %d: %w", lineNumber, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := ValidateHistory(events); err != nil {
		return nil, err
	}
	return events, nil
}

// ValidateHistory checks the append-chain invariants for already decoded
// events. It is useful to consumers that receive a durable history snapshot.
func ValidateHistory(events []Event) error {
	prev := "genesis"
	var expectedSeq int64 = 1
	for _, ev := range events {
		if ev.Seq != expectedSeq {
			return fmt.Errorf("event %q: expected seq %d, got %d", ev.ID, expectedSeq, ev.Seq)
		}
		if ev.PrevHash != prev {
			return fmt.Errorf("event %q: prev_hash mismatch", ev.ID)
		}
		computed, err := hashEvent(ev)
		if err != nil {
			return fmt.Errorf("event %q: compute hash: %w", ev.ID, err)
		}
		if computed != ev.Hash {
			return fmt.Errorf("event %q: hash mismatch", ev.ID)
		}
		prev = ev.Hash
		expectedSeq++
	}
	return nil
}
