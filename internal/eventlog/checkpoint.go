package eventlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (s *Service) Checkpoint(dir string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}

	checkpoint := map[string]any{
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"seq":        s.seq,
		"last_hash":  s.lastHash,
		"source":     "rust_acknowledged_mirror",
	}

	name := fmt.Sprintf(
		"checkpoint-%06d-%s.json",
		s.seq,
		shortHash(s.lastHash),
	)

	b, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return nil, err
	}

	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		return nil, err
	}

	checkpoint["path"] = p

	return checkpoint, nil
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
