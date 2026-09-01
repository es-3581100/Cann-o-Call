package eventlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// RustAcknowledgedEvents reads the canonical Rust recorder when configured and
// reconstructs its Go evidence with the Rust acknowledgement that binds each
// record. Without a configured recorder there is no semantic authority, even
// if a Go mirror contains ACK-shaped fields.
func (s *Service) RustAcknowledgedEvents() ([]Event, error) {
	// Keep the sidecar fetch and local sequence rebase in the same critical
	// section as Append. Otherwise an append can advance the local head between
	// the fetch and rebase and be overwritten with a stale Rust head.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sidecarURL == "" {
		return []Event{}, nil
	}

	resp, err := s.client.Get(s.sidecarURL + "/events")
	if err != nil {
		return nil, fmt.Errorf("%w: read Rust event history: %v", ErrDurableRecorderUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: Rust event history returned status %d", ErrDurableRecorderUnavailable, resp.StatusCode)
	}

	var response struct {
		Events []struct {
			Seq      int64           `json:"seq"`
			PrevHash string          `json:"prev_hash"`
			Hash     string          `json:"hash"`
			Evidence json.RawMessage `json:"evidence"`
		} `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("%w: decode Rust event history: %v", ErrDurableRecorderUnavailable, err)
	}

	previousRustHash := "genesis"
	events := make([]Event, 0, len(response.Events))
	for _, record := range response.Events {
		if record.Seq != int64(len(events)+1) || record.PrevHash != previousRustHash || !isSHA256(record.Hash) {
			return nil, fmt.Errorf("Rust event history is not a valid acknowledged chain at seq %d", record.Seq)
		}
		var ev Event
		if err := json.Unmarshal(record.Evidence, &ev); err != nil {
			return nil, fmt.Errorf("decode Rust evidence seq %d: %w", record.Seq, err)
		}
		ev.RustAck = &RustAcknowledgement{ID: ev.ID, Sequence: record.Seq, Hash: record.Hash}
		if ev.Seq != record.Seq {
			return nil, fmt.Errorf("Rust evidence event %q seq %d does not match Rust seq %d", ev.ID, ev.Seq, record.Seq)
		}
		if err := ValidateRustAcknowledgement(ev); err != nil {
			return nil, fmt.Errorf("Rust evidence event %q acknowledgement: %w", ev.ID, err)
		}
		events = append(events, ev)
		previousRustHash = record.Hash
	}
	if err := ValidateHistory(events); err != nil {
		return nil, err
	}
	// The mirror may be absent or stale after a restart; Rust is the sequence
	// authority for the next append.
	s.seq = int64(len(events))
	s.lastHash = "genesis"
	if len(events) > 0 {
		s.lastHash = events[len(events)-1].Hash
	}
	return events, nil
}

// CheckedEvents reads the complete Go ledger and rejects malformed or broken
// records. List remains intentionally best-effort for legacy display paths;
// callers reading an unconfigured mirror must use this method instead.
func (s *Service) CheckedEvents() ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkedEventsLocked()
}

func (s *Service) checkedEventsLocked() ([]Event, error) {

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
		if requiresRustAcknowledgement(ev) {
			if err := ValidateRustAcknowledgement(ev); err != nil {
				return fmt.Errorf("event %q: rust acknowledgement: %w", ev.ID, err)
			}
		}
		prev = ev.Hash
		expectedSeq++
	}
	return nil
}
