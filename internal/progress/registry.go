package progress

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MaxRegistrySize bounds registry growth. Oldest completed/failed/rejected
// beyond this limit are evicted. Active tasks are never evicted to avoid loss.
const MaxRegistrySize = 128

// MaxWarnings and MaxErrors bound per-task slices.
const (
	MaxWarnings = 10
	MaxErrors   = 10
)

// Registry is the bounded, race-safe task registry. It is the sole authority
// for live task state; durable accepted state is separate and derived from
// Rust-acknowledged history. Transient in-flight progress does NOT survive
// restart; after restart canonical state derives from durable history.
type Registry struct {
	mu      sync.Mutex
	tasks   map[string]*Task
	maxSize int
}

// NewRegistry creates a bounded registry. maxSize <=0 defaults to MaxRegistrySize.
func NewRegistry(maxSize int) *Registry {
	if maxSize <= 0 {
		maxSize = MaxRegistrySize
	}
	return &Registry{
		tasks:   make(map[string]*Task),
		maxSize: maxSize,
	}
}

// RandomTaskID generates a uuid-v4 style random task id (hex, not deterministic).
func RandomTaskID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}

// Create creates a new task with given taskID. If taskID exists and is
// idempotent (same operation), it returns existing task. If taskID exists with
// different operation, it returns conflict error. Empty taskID generates random.
func (r *Registry) Create(taskID string, op Operation, phase Phase) (*Task, error) {
	if strings.TrimSpace(string(op)) == "" {
		return nil, NewTaskError(ErrInvalidInput, "operation required", nil)
	}
	if strings.TrimSpace(taskID) == "" {
		taskID = RandomTaskID()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.tasks[taskID]; ok {
		if existing.Packet.Operation != op {
			return nil, NewTaskError(ErrInvalidInput, fmt.Sprintf("task id %q already used by operation %q", taskID, existing.Packet.Operation), nil)
		}
		cp := *existing
		return &cp, nil
	}
	now := time.Now().UTC()
	t := &Task{
		Packet: ProgressPacket{
			TaskID:    taskID,
			Operation: op,
			Phase:     phase,
			Status:    StatusPending,
			StartedAt: now,
			UpdatedAt: now,
			Warnings:  []string{},
			Errors:    []string{},
		},
		createdAt: now,
	}
	r.tasks[taskID] = t
	r.evictIfNeededLocked()
	cp := *t
	return &cp, nil
}

// CreateDeterministic creates task with deterministic id derived from operation+workspace+key.
func (r *Registry) CreateDeterministic(op Operation, workspaceID, key string, phase Phase) (*Task, error) {
	id := DeterministicTaskID(op, workspaceID, key)
	return r.Create(id, op, phase)
}

// Get returns copy of task and existence.
func (r *Registry) Get(taskID string) (*Task, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return nil, false
	}
	cp := *t
	// copy slices
	cp.Packet.Warnings = append([]string(nil), t.Packet.Warnings...)
	cp.Packet.Errors = append([]string(nil), t.Packet.Errors...)
	return &cp, true
}

// List returns copies sorted by StartedAt ascending.
func (r *Registry) List() []*Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		cp := *t
		cp.Packet.Warnings = append([]string(nil), t.Packet.Warnings...)
		cp.Packet.Errors = append([]string(nil), t.Packet.Errors...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Packet.StartedAt.Equal(out[j].Packet.StartedAt) {
			return out[i].Packet.TaskID < out[j].Packet.TaskID
		}
		return out[i].Packet.StartedAt.Before(out[j].Packet.StartedAt)
	})
	return out
}

// Update atomically applies fn to task packet.
func (r *Registry) Update(taskID string, fn func(*ProgressPacket)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("task %q not found", taskID), nil)
	}
	if fn != nil {
		fn(&t.Packet)
		// enforce bounds
		if len(t.Packet.Warnings) > MaxWarnings {
			t.Packet.Warnings = boundedCopy(t.Packet.Warnings, MaxWarnings)
		}
		if len(t.Packet.Errors) > MaxErrors {
			t.Packet.Errors = boundedCopy(t.Packet.Errors, MaxErrors)
		}
		if t.Packet.UpdatedAt.IsZero() {
			t.Packet.UpdatedAt = time.Now().UTC()
		}
	}
	return nil
}

// Transition validates and applies status change.
func (r *Registry) Transition(taskID string, to TaskStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("task %q not found", taskID), nil)
	}
	if err := ValidateTransition(t.Packet.Status, to); err != nil {
		return err
	}
	t.Packet.Status = to
	t.Packet.UpdatedAt = time.Now().UTC()
	if to == StatusRunning && t.Packet.Phase == PhasePending {
		t.Packet.Phase = PhaseRunning
	}
	if IsTerminal(to) {
		t.Packet.Phase = PhaseTerminal
	}
	return nil
}

// SetPhase updates phase.
func (r *Registry) SetPhase(taskID string, phase Phase) error {
	return r.Update(taskID, func(p *ProgressPacket) {
		p.Phase = phase
		p.UpdatedAt = time.Now().UTC()
	})
}

// AddWarning appends bounded warning.
func (r *Registry) AddWarning(taskID, msg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("task %q not found", taskID), nil)
	}
	t.AddWarning(msg)
	if len(t.Packet.Warnings) > MaxWarnings {
		t.Packet.Warnings = boundedCopy(t.Packet.Warnings, MaxWarnings)
	}
	return nil
}

// AddError appends bounded error.
func (r *Registry) AddError(taskID, msg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("task %q not found", taskID), nil)
	}
	t.AddError(msg)
	if len(t.Packet.Errors) > MaxErrors {
		t.Packet.Errors = boundedCopy(t.Packet.Errors, MaxErrors)
	}
	return nil
}

// SetProgress updates completed/total and recomputes throughput/eta.
func (r *Registry) SetProgress(taskID string, completed, total *int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return NewTaskError(ErrInvalidInput, fmt.Sprintf("task %q not found", taskID), nil)
	}
	t.SetProgress(completed, total)
	return nil
}

// Delete removes task (explicit cleanup).
func (r *Registry) Delete(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tasks, taskID)
}

// Count returns current registry size.
func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.tasks)
}

// MarkAbandoned marks all pending/running as failed with abandon reason.
// Call on restart to reflect transient tasks are unknown/interrupted.
func (r *Registry) MarkAbandoned() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	for _, t := range r.tasks {
		if t.Packet.Status == StatusPending || t.Packet.Status == StatusRunning {
			t.Packet.Status = StatusFailed
			t.Packet.Phase = PhaseTerminal
			t.Packet.UpdatedAt = now
			if len(t.Packet.Errors) < MaxErrors {
				t.Packet.Errors = append(t.Packet.Errors, "abandoned: transient in-flight progress does not survive restart; canonical state derives from durable history")
			}
		}
	}
}

// evictIfNeededLocked removes oldest terminal tasks beyond maxSize.
// Must be called with lock held. Never evicts pending/running.
func (r *Registry) evictIfNeededLocked() {
	if len(r.tasks) <= r.maxSize {
		return
	}
	// Collect terminal tasks sorted by UpdatedAt asc (oldest first).
	type entry struct {
		id string
		at time.Time
	}
	var terminals []entry
	for id, t := range r.tasks {
		if IsTerminal(t.Packet.Status) {
			terminals = append(terminals, entry{id: id, at: t.Packet.UpdatedAt})
		}
	}
	sort.Slice(terminals, func(i, j int) bool {
		if terminals[i].at.Equal(terminals[j].at) {
			return terminals[i].id < terminals[j].id
		}
		return terminals[i].at.Before(terminals[j].at)
	})
	need := len(r.tasks) - r.maxSize
	evicted := 0
	for _, e := range terminals {
		if evicted >= need {
			break
		}
		delete(r.tasks, e.id)
		evicted++
	}
}
