package quarantine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Item struct {
	ID           string `json:"id"`
	WorkspaceID  string `json:"workspace_id"`
	OriginalPath string `json:"original_path"`
	SafePath     string `json:"safe_path"`
	TargetPath   string `json:"target_path,omitempty"`
	Reason       string `json:"reason"`
	Status       string `json:"status"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
	DecidedAt    string `json:"decided_at,omitempty"`
	ReceiptID    string `json:"receipt_id,omitempty"`
	Data         []byte `json:"data"`
}

type Store struct {
	mu    sync.Mutex
	dir   string
	items map[string]*Item
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create quarantine directory: %w", err)
	}

	s := &Store{
		dir:   dir,
		items: map[string]*Item{},
	}

	if err := s.load(); err != nil {
		return nil, fmt.Errorf("load quarantine store: %w", err)
	}

	return s, nil
}

func key(workspaceID, id string) string {
	return workspaceID + "__" + id
}

func (s *Store) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		p := filepath.Join(s.dir, entry.Name())

		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}

		var item Item
		if err := json.Unmarshal(b, &item); err != nil {
			continue
		}

		if item.ID == "" || item.WorkspaceID == "" {
			continue
		}

		s.items[key(item.WorkspaceID, item.ID)] = &item
	}

	return nil
}

func (s *Store) persist(item *Item) error {
	b, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}

	name := fmt.Sprintf("%s__%s.json", item.WorkspaceID, item.ID)
	p := filepath.Join(s.dir, name)

	return os.WriteFile(p, b, 0o644)
}

func (s *Store) Add(item *Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item.ID == "" || item.WorkspaceID == "" {
		return fmt.Errorf("quarantine item requires id and workspace_id")
	}

	k := key(item.WorkspaceID, item.ID)

	if _, ok := s.items[k]; ok {
		return nil
	}

	if err := s.persist(item); err != nil {
		return err
	}

	s.items[k] = item

	return nil
}

func (s *Store) UpdateStatus(
	workspaceID string,
	id string,
	status string,
	receiptID string,
	decidedAt string,
	targetPath string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(workspaceID, id)

	item, ok := s.items[k]
	if !ok {
		return fmt.Errorf("quarantine item %q not found", id)
	}

	item.Status = status
	item.ReceiptID = receiptID
	item.DecidedAt = decidedAt

	if targetPath != "" {
		item.TargetPath = targetPath
	}

	return s.persist(item)
}

func (s *Store) Get(workspaceID, id string) (*Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key(workspaceID, id)]
	return item, ok
}

func (s *Store) ListWorkspace(workspaceID string) []*Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := []*Item{}

	for _, item := range s.items {
		if item.WorkspaceID == workspaceID {
			out = append(out, item)
		}
	}

	return out
}
