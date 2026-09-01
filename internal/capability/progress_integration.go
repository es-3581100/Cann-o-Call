package capability

import (
	"context"
	"sync"
	"time"

	"flatten-workspace/internal/progress"
)

// TaskRecord ties capability execution to progress observability.
type TaskRecord struct {
	TaskID       string
	CapabilityID string
	RequestID    string
	ActorID      string
	Status       string // pending/running/completed/failed/rejected
	StartedAt    time.Time
	FinishedAt   time.Time
	Elapsed      time.Duration
}

type ProgressIntegration struct {
	mu       sync.Mutex
	records  map[string]*TaskRecord
	registry *progress.Registry
}

func NewProgressIntegration(reg *progress.Registry) *ProgressIntegration {
	if reg == nil {
		reg = progress.NewRegistry(128)
	}
	return &ProgressIntegration{records: map[string]*TaskRecord{}, registry: reg}
}

func (p *ProgressIntegration) Registry() *progress.Registry { return p.registry }

func (p *ProgressIntegration) Begin(req Request) *TaskRecord {
	rec := &TaskRecord{
		TaskID: req.RequestID, CapabilityID: req.CapabilityID, RequestID: req.RequestID,
		ActorID: req.ActorID, Status: "pending", StartedAt: time.Now().UTC(),
	}
	p.mu.Lock()
	p.records[req.RequestID] = rec
	p.mu.Unlock()
	// Also create progress.Task for observability
	_, _ = p.registry.Create(req.RequestID, progress.Operation("capability"), progress.PhaseRequestAccepted)
	_ = p.registry.Transition(req.RequestID, progress.StatusRunning)
	return rec
}

func (p *ProgressIntegration) Complete(req Request, res Result, err error) *TaskRecord {
	p.mu.Lock()
	rec, ok := p.records[req.RequestID]
	if !ok {
		rec = &TaskRecord{TaskID: req.RequestID, CapabilityID: req.CapabilityID, RequestID: req.RequestID, ActorID: req.ActorID, StartedAt: time.Now().UTC()}
		p.records[req.RequestID] = rec
	}
	p.mu.Unlock()
	rec.FinishedAt = time.Now().UTC()
	rec.Elapsed = rec.FinishedAt.Sub(rec.StartedAt)
	switch res.Status {
	case ResultCompleted:
		rec.Status = "completed"
		_ = p.registry.Transition(req.RequestID, progress.StatusComplete)
	case ResultFailed:
		rec.Status = "failed"
		_ = p.registry.Transition(req.RequestID, progress.StatusFailed)
	case ResultRejected:
		rec.Status = "rejected"
		_ = p.registry.Transition(req.RequestID, progress.StatusRejected)
	default:
		if err != nil {
			rec.Status = "failed"
			_ = p.registry.Transition(req.RequestID, progress.StatusFailed)
		} else {
			rec.Status = "completed"
			_ = p.registry.Transition(req.RequestID, progress.StatusComplete)
		}
	}
	// Ensure terminal phase
	_ = p.registry.SetPhase(req.RequestID, progress.PhaseTerminal)
	return rec
}

func (p *ProgressIntegration) Get(requestID string) (*TaskRecord, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.records[requestID]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (p *ProgressIntegration) List() []*TaskRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*TaskRecord, 0, len(p.records))
	for _, r := range p.records {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

// OnRestart marks pending/running as abandoned (transient does not survive).
func (p *ProgressIntegration) OnRestart() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	for _, r := range p.records {
		if r.Status == "pending" || r.Status == "running" {
			r.Status = "failed"
			r.FinishedAt = now
			r.Elapsed = r.FinishedAt.Sub(r.StartedAt)
		}
	}
	p.registry.MarkAbandoned()
}

// ExecuteWithProgress wraps Manager Execute with progress tracking.
func (p *ProgressIntegration) ExecuteWithProgress(ctx context.Context, mgr *Manager, req Request) (Result, error) {
	p.Begin(req)
	res, err := mgr.Execute(ctx, req)
	p.Complete(req, res, err)
	return res, err
}
