package server

import (
	"fmt"
	"net/http"
	"time"

	"flatten-workspace/internal/observability"
	"flatten-workspace/internal/progress"
)

// Task registry handlers.

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	if s.Tasks == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	list := s.Tasks.List()
	out := make([]progress.ProgressPacket, 0, len(list))
	for _, t := range list {
		out = append(out, t.Packet)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("task id required"))
		return
	}
	if s.Tasks == nil {
		writeError(w, http.StatusNotFound, fmt.Errorf("task %q not found", id))
		return
	}
	t, ok := s.Tasks.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("task %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, t.Packet)
}

// beginTask creates task, transitions to running, updates metrics.
func (s *Server) beginTask(op progress.Operation, workspaceID, key string, phase progress.Phase) (*progress.Task, error) {
	if s.Tasks == nil {
		return nil, fmt.Errorf("task registry not initialized")
	}
	var t *progress.Task
	var err error
	if workspaceID != "" && key != "" {
		t, err = s.Tasks.CreateDeterministic(op, workspaceID, key, phase)
	} else {
		t, err = s.Tasks.Create("", op, phase)
	}
	if err != nil {
		return nil, err
	}
	// Transition pending -> running
	_ = s.Tasks.Transition(t.Packet.TaskID, progress.StatusRunning)
	// Set phase running if pending
	_ = s.Tasks.SetPhase(t.Packet.TaskID, progress.PhaseRunning)
	// Update metrics read-only projection
	s.updateTaskMetrics(t.Packet.TaskID)
	// Re-fetch
	nt, _ := s.Tasks.Get(t.Packet.TaskID)
	return nt, nil
}

func (s *Server) updateTaskMetrics(taskID string) {
	if s.Tasks == nil {
		return
	}
	actorM := observability.ActorMetricsFromController(s.Actors)
	ledgerM := observability.LedgerMetricsFromService(s.Events, s.DataDir)
	// graph and context aggregated
	s.muOrch.RLock()
	graphM := observability.GraphMetricsAggregate(s.graphStores)
	ctxM := observability.ContextMetricsAggregate(s.orchestrators)
	s.muOrch.RUnlock()
	_ = s.Tasks.Update(taskID, func(p *progress.ProgressPacket) {
		p.Actor = actorM
		p.Ledger = ledgerM
		p.Graph = graphM
		p.Context = ctxM
		p.UpdatedAt = time.Now().UTC()
		if p.Throughput == nil {
			p.Throughput = p.ComputeThroughput()
		}
		p.ETA = p.ComputeETA()
	})
}

func (s *Server) completeTask(taskID string, final progress.TaskStatus, addError string) {
	if s.Tasks == nil {
		return
	}
	s.updateTaskMetrics(taskID)
	if addError != "" {
		_ = s.Tasks.AddError(taskID, addError)
	}
	_ = s.Tasks.Transition(taskID, final)
	// Persist receipt if possible
	t, ok := s.Tasks.Get(taskID)
	if !ok {
		return
	}
	if s.ProgressReceipts != nil {
		// Build receipt from terminal task; no durable refs required for empty claim
		receipt, err := progress.BuildReceiptFromTask(t, final, 0, nil, nil, "", "")
		if err == nil {
			_ = s.ProgressReceipts.Save(receipt)
		} else {
			// Fallback minimal receipt without validation
			rp := progress.ReceiptPacket{
				TaskID:      t.Packet.TaskID,
				Operation:   t.Packet.Operation,
				StartedAt:   t.Packet.StartedAt,
				FinishedAt:  time.Now().UTC(),
				Elapsed:     time.Since(t.Packet.StartedAt).Truncate(time.Millisecond).String(),
				FinalStatus: final,
				Warnings:    t.Packet.Warnings,
				Errors:      t.Packet.Errors,
			}
			if addError != "" {
				rp.Errors = append(rp.Errors, addError)
			}
			_ = s.ProgressReceipts.Save(rp)
		}
	}
}

func (s *Server) failTask(taskID string, errMsg string) {
	s.completeTask(taskID, progress.StatusFailed, errMsg)
}
