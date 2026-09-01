package eventlog

import (
	"fmt"
)

func (s *Service) Verify() (map[string]any, error) {
	events, err := s.List()
	if err != nil {
		return nil, err
	}

	prev := "genesis"
	var expectedSeq int64 = 1
	checked := 0

	for _, ev := range events {
		if ev.Seq != expectedSeq {
			return map[string]any{
				"ok":             false,
				"events_checked": checked,
				"failed_seq":     ev.Seq,
				"reason":         fmt.Sprintf("expected seq %d, got %d", expectedSeq, ev.Seq),
				"last_hash":      prev,
			}, nil
		}

		if ev.PrevHash != prev {
			return map[string]any{
				"ok":             false,
				"events_checked": checked,
				"failed_seq":     ev.Seq,
				"reason":         "prev_hash mismatch",
				"last_hash":      prev,
			}, nil
		}

		computed, err := hashEvent(ev)
		if err != nil {
			return map[string]any{
				"ok":             false,
				"events_checked": checked,
				"failed_seq":     ev.Seq,
				"reason":         fmt.Sprintf("hash computation failed: %v", err),
				"last_hash":      prev,
			}, nil
		}

		if computed != ev.Hash {
			return map[string]any{
				"ok":             false,
				"events_checked": checked,
				"failed_seq":     ev.Seq,
				"reason":         "hash mismatch",
				"last_hash":      prev,
			}, nil
		}
		if requiresRustAcknowledgement(ev) {
			if err := ValidateRustAcknowledgement(ev); err != nil {
				return map[string]any{
					"ok":             false,
					"events_checked": checked,
					"failed_seq":     ev.Seq,
					"reason":         fmt.Sprintf("rust acknowledgement: %v", err),
					"last_hash":      prev,
				}, nil
			}
		}

		prev = ev.Hash
		expectedSeq++
		checked++
	}

	return map[string]any{
		"ok":             true,
		"events_checked": checked,
		"last_hash":      prev,
	}, nil
}
