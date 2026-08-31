package receipts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Receipt struct {
	ID              string         `json:"id"`
	Seq             int64          `json:"seq,omitempty"`
	CreatedAt       time.Time      `json:"created_at"`
	WorkspaceID     string         `json:"workspace_id,omitempty"`
	Action          string         `json:"action"`
	Objective       string         `json:"objective"`
	Status          string         `json:"status"`
	AuthoritySource string         `json:"authority_source"`
	EventID         string         `json:"event_id"`
	FilesChanged    []string       `json:"files_changed,omitempty"`
	Details         map[string]any `json:"details,omitempty"`
	Notes           []string       `json:"notes,omitempty"`
	PrevHash        string         `json:"prev_hash,omitempty"`
	Hash            string         `json:"hash"`
}

type Service struct {
	mu       sync.Mutex
	dir      string
	receipts map[string]Receipt
	lastHash string
	seq      int64
}

func New(dir string) (*Service, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create receipts directory: %w", err)
	}

	s := &Service{
		dir:      dir,
		receipts: map[string]Receipt{},
		lastHash: "genesis",
		seq:      0,
	}

	if err := s.loadFromDisk(); err != nil {
		return nil, fmt.Errorf("load receipts from disk: %w", err)
	}

	s.rebuildChainState()

	return s, nil
}

func (s *Service) loadFromDisk() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		p := filepath.Join(s.dir, entry.Name())

		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}

		var r Receipt
		if err := json.Unmarshal(b, &r); err != nil {
			continue
		}

		if r.ID == "" {
			continue
		}

		if r.Hash == "" {
			if h, err := hashReceipt(r); err == nil {
				r.Hash = h
			}
		}

		s.receipts[r.ID] = r
	}

	return nil
}

func (s *Service) rebuildChainState() {
	chained := []Receipt{}

	for _, r := range s.receipts {
		if r.Seq > 0 {
			chained = append(chained, r)
		}
	}

	sort.Slice(chained, func(i, j int) bool {
		if chained[i].Seq != chained[j].Seq {
			return chained[i].Seq < chained[j].Seq
		}
		return chained[i].CreatedAt.Before(chained[j].CreatedAt)
	})

	for _, r := range chained {
		if r.Seq > s.seq {
			s.seq = r.Seq
			s.lastHash = r.Hash
		}
	}
}

func (s *Service) Save(r Receipt) (Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.ID == "" {
		return r, fmt.Errorf("receipt id is required")
	}

	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now().UTC()
	}

	if r.Status == "" {
		r.Status = "delivered"
	}

	if r.Seq == 0 {
		s.seq++
		r.Seq = s.seq
		r.PrevHash = s.lastHash
	}

	hash, err := hashReceipt(r)
	if err != nil {
		return r, err
	}
	r.Hash = hash

	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return r, err
	}

	p := filepath.Join(s.dir, r.ID+".json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		return r, err
	}

	s.receipts[r.ID] = r

	if r.Seq > s.seq {
		s.seq = r.Seq
	}

	if r.Seq > 0 {
		s.lastHash = r.Hash
	}

	return r, nil
}

func (s *Service) Get(id string) (Receipt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.receipts[id]
	return r, ok
}

func (s *Service) List() []Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Receipt, 0, len(s.receipts))
	for _, r := range s.receipts {
		out = append(out, r)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return out
}

func (s *Service) Verify() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	chained := []Receipt{}
	legacy := []Receipt{}

	for _, r := range s.receipts {
		if r.Seq > 0 {
			chained = append(chained, r)
		} else {
			legacy = append(legacy, r)
		}
	}

	sort.Slice(chained, func(i, j int) bool {
		if chained[i].Seq != chained[j].Seq {
			return chained[i].Seq < chained[j].Seq
		}
		return chained[i].CreatedAt.Before(chained[j].CreatedAt)
	})

	prev := "genesis"
	var expectedSeq int64 = 1
	chainedChecked := 0

	for _, r := range chained {
		if r.Seq != expectedSeq {
			return map[string]any{
				"ok":              false,
				"reason":          fmt.Sprintf("expected receipt seq %d, got %d", expectedSeq, r.Seq),
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  len(legacy),
				"last_hash":       prev,
			}
		}

		if r.PrevHash != prev {
			return map[string]any{
				"ok":              false,
				"reason":          "prev_hash mismatch",
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  len(legacy),
				"last_hash":       prev,
			}
		}

		computed, err := hashReceipt(r)
		if err != nil {
			return map[string]any{
				"ok":              false,
				"reason":          fmt.Sprintf("hash computation failed: %v", err),
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  len(legacy),
				"last_hash":       prev,
			}
		}

		if computed != r.Hash {
			return map[string]any{
				"ok":              false,
				"reason":          "receipt hash mismatch",
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  len(legacy),
				"last_hash":       prev,
			}
		}

		prev = r.Hash
		expectedSeq++
		chainedChecked++
	}

	legacyChecked := 0

	for _, r := range legacy {
		computed, err := hashReceipt(r)
		if err != nil {
			return map[string]any{
				"ok":              false,
				"reason":          fmt.Sprintf("legacy hash computation failed: %v", err),
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  legacyChecked,
				"last_hash":       prev,
			}
		}

		if computed != r.Hash {
			return map[string]any{
				"ok":              false,
				"reason":          "legacy receipt hash mismatch",
				"failed_receipt":  r.ID,
				"chained_checked": chainedChecked,
				"legacy_checked":  legacyChecked,
				"last_hash":       prev,
			}
		}

		legacyChecked++
	}

	return map[string]any{
		"ok":              true,
		"chained_checked": chainedChecked,
		"legacy_checked":  legacyChecked,
		"last_hash":       prev,
	}
}

func hashReceipt(r Receipt) (string, error) {
	clone := r
	clone.Hash = ""

	b, err := json.Marshal(clone)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
