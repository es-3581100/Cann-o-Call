package capability

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"flatten-workspace/internal/transition"
)

// Manager orchestrates admission above executors and progress/authority integration.
// It does NOT decide canonical mutation; it only executes work and, for mutating path, caller must propose via Authority.
type Manager struct {
	registry  *Registry
	authority *transition.Authority
	// optional workspace store for filesystem scoping? use in-memory map for tests via WorkspaceGetter
	workspaceRootFor func(workspaceID string) string // returns root dir for workspace scope; if nil, no FS capabilities allowed
}

func NewManager(reg *Registry, auth *transition.Authority) *Manager {
	return &Manager{registry: reg, authority: auth}
}

func (m *Manager) Registry() *Registry              { return m.registry }
func (m *Manager) Authority() *transition.Authority { return m.authority }

// SetWorkspaceRootGetter allows filesystem executors to resolve workspace root.
func (m *Manager) SetWorkspaceRootGetter(fn func(string) string) {
	m.workspaceRootFor = fn
}

// ValidateRequest performs fail-closed validation before execution.
func (m *Manager) ValidateRequest(req Request) error {
	if err := req.Validate(); err != nil {
		return err
	}
	desc, err := m.registry.GetDescriptor(req.CapabilityID)
	if err != nil {
		return err // unknown -> fail-closed
	}
	// Disabled check
	if !desc.Enabled {
		return NewError(ErrCapabilityDisabled, fmt.Sprintf("capability %q disabled", req.CapabilityID), nil)
	}
	// Input size bound
	if req.Inputs != nil && len(req.Inputs) > desc.ResourceBounds.MaxInputBytes {
		return NewError(ErrResourceLimit, fmt.Sprintf("input %d exceeds bound %d", len(req.Inputs), desc.ResourceBounds.MaxInputBytes), nil)
	}
	return nil
}

// Execute performs bounded execution. It respects context timeout, input/output bounds, but does NOT auto-propose transition.
func (m *Manager) Execute(ctx context.Context, req Request) (Result, error) {
	start := time.Now().UTC()
	// Validate first
	if err := m.ValidateRequest(req); err != nil {
		return Result{RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultRejected, StartedAt: start, FinishedAt: time.Now().UTC()}, err
	}
	exec, err := m.registry.Get(req.CapabilityID)
	if err != nil {
		return Result{RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultRejected, StartedAt: start, FinishedAt: time.Now().UTC()}, err
	}
	desc := exec.Describe()
	// Determine timeout: descriptor timeout or ctx deadline
	timeout := desc.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Channel for bounded execution
	type execOut struct {
		res Result
		err error
	}
	ch := make(chan execOut, 1)
	go func() {
		res, e := exec.Execute(cctx, req)
		ch <- execOut{res, e}
	}()

	select {
	case <-cctx.Done():
		// Distinguish timeout vs cancellation
		if cctx.Err() == context.DeadlineExceeded {
			fin := time.Now().UTC()
			return Result{
				RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultFailed,
				StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
				Errors: []string{ErrTimeout + ": descriptor timeout exceeded"},
			}, NewError(ErrTimeout, "execution timeout", cctx.Err())
		}
		fin := time.Now().UTC()
		return Result{
			RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultFailed,
			StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
			Errors: []string{"cancelled"},
		}, NewError(ErrExecutionFailed, "execution cancelled", cctx.Err())
	case out := <-ch:
		// Check output bounds
		if out.res.Outputs != nil && len(out.res.Outputs) > desc.ResourceBounds.MaxOutputBytes {
			fin := time.Now().UTC()
			return Result{
				RequestID: req.RequestID, CapabilityID: req.CapabilityID, Status: ResultFailed,
				StartedAt: start, FinishedAt: fin, Elapsed: fin.Sub(start),
				Errors: []string{ErrResourceLimit + ": output exceeds bound"},
			}, NewError(ErrResourceLimit, fmt.Sprintf("output %d exceeds bound %d", len(out.res.Outputs), desc.ResourceBounds.MaxOutputBytes), nil)
		}
		// Ensure result has timing
		if out.res.StartedAt.IsZero() {
			out.res.StartedAt = start
		}
		if out.res.FinishedAt.IsZero() {
			out.res.FinishedAt = time.Now().UTC()
		}
		if out.res.Elapsed == 0 {
			out.res.Elapsed = out.res.FinishedAt.Sub(out.res.StartedAt)
		}
		// Normalize error taxonomy: if exec returned error but no status, mark failed
		if out.err != nil {
			if out.res.Status == "" {
				out.res.Status = ResultFailed
			}
		} else {
			if out.res.Status == "" {
				out.res.Status = ResultCompleted
			}
		}
		// Security: executor output cannot alter tier, register capabilities, increase budget, etc.
		// We ignore any such claims in output; tier remains descriptor's tier.
		return out.res, out.err
	}
}

// BuildProposedTransition converts a mutating Result into a ProposedTransition for Go admission.
// Caller must provide operation/entity/node mapping; result itself does not imply acceptance.
func (m *Manager) BuildProposedTransition(req Request, res Result, operation, entity, node string, prior transition.StateRef, resultData json.RawMessage) transition.ProposedTransition {
	if len(resultData) == 0 {
		resultData = res.Outputs
	}
	if len(resultData) == 0 {
		resultData = json.RawMessage(`{}`)
	}
	// Deterministic IDs based on request
	transitionID := fmt.Sprintf("cap-%s-%s", req.CapabilityID, req.RequestID)
	if len(transitionID) > 64 {
		transitionID = transitionID[:64]
	}
	return transition.ProposedTransition{
		TransitionID:  transitionID,
		ProposalID:    "proposal-" + req.RequestID,
		RequestID:     req.RequestID,
		Prior:         prior,
		Operation:     operation,
		Entity:        entity,
		Node:          node,
		ResultData:    resultData,
		AdmissionData: json.RawMessage(`{}`),
	}
}

// ExecuteMutating is helper that executes then proposes via Authority if result completed.
// Returns AcceptedTransition on success; on Rust unavailable fail-closed projection unchanged.
func (m *Manager) ExecuteMutating(ctx context.Context, req Request, operation, entity, node string, resultDataOverride json.RawMessage) (Result, transition.AcceptedTransition, error) {
	res, err := m.Execute(ctx, req)
	if err != nil {
		return res, transition.AcceptedTransition{}, err
	}
	if res.Status != ResultCompleted {
		return res, transition.AcceptedTransition{}, NewError(ErrExecutionFailed, fmt.Sprintf("execution not completed: %s", res.Status), nil)
	}
	// Only T2/T3 mutating capabilities may route to canonical mutation; T1 must not.
	desc, _ := m.registry.GetDescriptor(req.CapabilityID)
	if desc.Tier == TierRead {
		return res, transition.AcceptedTransition{}, NewError(ErrCapabilityDenied, "T1_READ cannot propose canonical mutation", nil)
	}
	if !desc.Mutating {
		return res, transition.AcceptedTransition{}, NewError(ErrCapabilityDenied, "non-mutating capability cannot propose mutation", nil)
	}
	if m.authority == nil {
		return res, transition.AcceptedTransition{}, NewError(ErrDurableRecorderUnavailable, "authority is nil", nil)
	}
	prior := m.authority.Projection().Ref()
	// Validate resultData is valid JSON
	rd := resultDataOverride
	if len(rd) == 0 {
		rd = res.Outputs
	}
	if len(rd) == 0 {
		rd = json.RawMessage(`{}`)
	}
	var v any
	if err := json.Unmarshal(rd, &v); err != nil {
		return res, transition.AcceptedTransition{}, NewError(ErrInvalidInput, fmt.Sprintf("result_data malformed: %v", err), err)
	}
	if strings.TrimSpace(operation) == "" {
		operation = "upsert"
	}
	if strings.TrimSpace(entity) == "" {
		entity = req.WorkspaceID
	}
	if strings.TrimSpace(node) == "" {
		node = req.CapabilityID
	}
	prop := m.BuildProposedTransition(req, res, operation, entity, node, prior, rd)
	accepted, err := m.authority.Propose(prop)
	if err != nil {
		// Map transition errors to capability taxonomy where appropriate
		msg := err.Error()
		code := ErrAdmissionRejected
		if strings.Contains(msg, "durable_recorder_unavailable") {
			code = ErrDurableRecorderUnavailable
		} else if strings.Contains(msg, "durable") {
			code = ErrDurableAppendFailed
		} else if strings.Contains(msg, "invalid") || strings.Contains(msg, "malformed") {
			code = ErrInvalidInput
		}
		return res, transition.AcceptedTransition{}, NewError(code, msg, err)
	}
	return res, accepted, nil
}
