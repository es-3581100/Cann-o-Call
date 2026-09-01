package capability

import (
	"fmt"
	"sort"
	"sync"
)

// Registry is bounded deterministic registry for executors.
type Registry struct {
	mu      sync.RWMutex
	maxSize int
	entries map[string]Executor // capability_id -> executor
}

func NewRegistry(maxSize int) *Registry {
	if maxSize <= 0 {
		maxSize = 64
	}
	return &Registry{
		maxSize: maxSize,
		entries: make(map[string]Executor),
	}
}

func (r *Registry) MaxSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.maxSize
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Register adds executor. Duplicate ID rejected, bounded check, descriptor validation.
func (r *Registry) Register(exec Executor) error {
	if exec == nil {
		return NewError(ErrInvalidInput, "executor is nil", nil)
	}
	desc := exec.Describe()
	if err := desc.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[desc.ID]; exists {
		return NewError(ErrInvalidInput, fmt.Sprintf("duplicate capability_id %q rejected", desc.ID), nil)
	}
	if len(r.entries) >= r.maxSize {
		return NewError(ErrResourceLimit, fmt.Sprintf("registry bound %d reached", r.maxSize), nil)
	}
	r.entries[desc.ID] = exec
	return nil
}

// Get retrieves executor by ID. Unknown returns ErrUnknownCapability fail-closed.
func (r *Registry) Get(id string) (Executor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	exec, ok := r.entries[id]
	if !ok {
		return nil, NewError(ErrUnknownCapability, fmt.Sprintf("unknown capability %q", id), nil)
	}
	// Disabled rejected at call time (policy above executors also checks)
	if !exec.Describe().Enabled {
		return nil, NewError(ErrCapabilityDisabled, fmt.Sprintf("capability %q disabled", id), nil)
	}
	return exec, nil
}

// GetDescriptor returns descriptor copy without enabled check (for inspection).
func (r *Registry) GetDescriptor(id string) (Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	exec, ok := r.entries[id]
	if !ok {
		return Descriptor{}, NewError(ErrUnknownCapability, fmt.Sprintf("unknown capability %q", id), nil)
	}
	return exec.Describe(), nil
}

// List returns descriptors deterministically sorted by capability_id.
func (r *Registry) List() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.entries))
	for _, exec := range r.entries {
		out = append(out, exec.Describe())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListIDs returns sorted IDs deterministically.
func (r *Registry) ListIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.entries))
	for id := range r.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// MustRegister panics on error (for init registration).
func (r *Registry) MustRegister(exec Executor) {
	if err := r.Register(exec); err != nil {
		panic(err)
	}
}
