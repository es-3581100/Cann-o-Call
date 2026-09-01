package eventlog

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	ID          string         `json:"id"`
	Seq         int64          `json:"seq"`
	CreatedAt   time.Time      `json:"created_at"`
	Type        string         `json:"type"`
	WorkspaceID string         `json:"workspace_id,omitempty"`
	Actor       string         `json:"actor,omitempty"`
	Action      string         `json:"action"`
	Status      string         `json:"status"`
	Details     map[string]any `json:"details,omitempty"`
	ReceiptID   string         `json:"receipt_id,omitempty"`
	Hash        string         `json:"hash"`
	PrevHash    string         `json:"prev_hash"`
	// RustAck is evidence that the optional Rust sidecar durably accepted this
	// already-Go-approved event. It is deliberately excluded from the Go event
	// hash because it is received only after the Go event has been hashed.
	RustAck *RustAcknowledgement `json:"rust_ack,omitempty"`
}

// RustAcknowledgement is the exact acknowledgement returned by the Rust
// sidecar after it persists a Go event.
type RustAcknowledgement struct {
	ID       string `json:"id"`
	Sequence int64  `json:"seq"`
	Hash     string `json:"hash"`
}

type Service struct {
	mu         sync.Mutex
	path       string
	lastHash   string
	seq        int64
	sidecarURL string
	client     *http.Client
}

func New(dir, sidecarURL string) (*Service, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create event log directory: %w", err)
	}

	s := &Service{
		path:       filepath.Join(dir, "ledger.jsonl"),
		lastHash:   "genesis",
		seq:        0,
		sidecarURL: sidecarURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	if err := s.load(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) load() error {
	f, err := os.OpenFile(s.path, os.O_RDONLY, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open event log: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			// In a stricter system, corrupted lines should quarantine.
			continue
		}

		if ev.Seq > s.seq {
			s.seq = ev.Seq
		}

		if ev.Hash != "" {
			s.lastHash = ev.Hash
		}
	}

	return scanner.Err()
}

func (s *Service) Append(ev Event) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ev.ID == "" {
		return ev, errors.New("event id is required")
	}

	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}

	if ev.Type == "" {
		ev.Type = "transition"
	}

	if ev.Status == "" {
		ev.Status = "accepted"
	}

	s.seq++
	ev.Seq = s.seq
	ev.PrevHash = s.lastHash

	hash, err := hashEvent(ev)
	if err != nil {
		s.seq--
		return ev, err
	}
	ev.Hash = hash

	if s.sidecarURL != "" {
		ack, err := s.forward(ev)
		if err != nil {
			s.seq--
			return ev, fmt.Errorf("rust sidecar rejected event: %w", err)
		}
		ev.RustAck = &ack
	}

	line, err := json.Marshal(ev)
	if err != nil {
		s.seq--
		return ev, err
	}

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		s.seq--
		return ev, err
	}
	defer f.Close()

	if _, err := f.Write(append(line, '\n')); err != nil {
		s.seq--
		return ev, err
	}

	s.lastHash = ev.Hash

	return ev, nil
}

func (s *Service) forward(ev Event) (RustAcknowledgement, error) {
	body, err := json.Marshal(ev)
	if err != nil {
		return RustAcknowledgement{}, err
	}

	resp, err := s.client.Post(
		s.sidecarURL+"/events",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return RustAcknowledgement{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return RustAcknowledgement{}, fmt.Errorf("sidecar returned status %d", resp.StatusCode)
	}

	var ack struct {
		Status string `json:"status"`
		ID     string `json:"id"`
		Seq    int64  `json:"seq"`
		Hash   string `json:"hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		return RustAcknowledgement{}, fmt.Errorf("decode sidecar acknowledgement: %w", err)
	}
	if ack.Status != "recorded" {
		return RustAcknowledgement{}, fmt.Errorf("sidecar acknowledgement has unexpected status %q", ack.Status)
	}
	if ack.ID == "" || ack.ID != ev.ID {
		return RustAcknowledgement{}, fmt.Errorf("sidecar acknowledgement id %q does not match event id %q", ack.ID, ev.ID)
	}
	if ack.Seq <= 0 || ack.Seq != ev.Seq {
		return RustAcknowledgement{}, fmt.Errorf("sidecar acknowledgement seq %d does not match event seq %d", ack.Seq, ev.Seq)
	}
	if !isSHA256(ack.Hash) {
		return RustAcknowledgement{}, errors.New("sidecar acknowledgement has invalid hash")
	}
	return RustAcknowledgement{ID: ack.ID, Sequence: ack.Seq, Hash: ack.Hash}, nil
}

func hashEvent(ev Event) (string, error) {
	clone := ev
	clone.Hash = ""
	clone.RustAck = nil

	b, err := json.Marshal(clone)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func isSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
