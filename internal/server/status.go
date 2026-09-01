package server

import (
	"net/http"

	"flatten-workspace/internal/observability"
)

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	actorM := observability.ActorMetricsFromController(s.Actors)
	ledgerM := observability.LedgerMetricsFromService(s.Events, s.DataDir)
	s.muOrch.RLock()
	graphM := observability.GraphMetricsAggregate(s.graphStores)
	ctxM := observability.ContextMetricsAggregate(s.orchestrators)
	orchCount := len(s.orchestrators)
	s.muOrch.RUnlock()
	// workspaces count
	wsCount := len(s.store.Summaries())
	status := observability.CollectSystemStatus(ledgerM, graphM, actorM, ctxM, wsCount)
	// enrich with orchestrator count for debugging
	resp := map[string]any{
		"ledger":        status.Ledger,
		"graph":         status.Graph,
		"actor":         status.Actor,
		"context":       status.Context,
		"replay_status": status.Replay,
		"checkpoint":    status.Checkpoint,
		"workspaces":    status.Workspaces,
		"event_count":   status.EventCount,
		"head_event_id": status.HeadEventID,
		"head_hash":     status.HeadHash,
		"orchestrators": orchCount,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleLedgerStatus(w http.ResponseWriter, r *http.Request) {
	m := observability.LedgerMetricsFromService(s.Events, s.DataDir)
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) handleOrchestratorStatus(w http.ResponseWriter, r *http.Request) {
	s.muOrch.RLock()
	graphM := observability.GraphMetricsAggregate(s.graphStores)
	ctxM := observability.ContextMetricsAggregate(s.orchestrators)
	// per-workspace details
	details := map[string]any{}
	for wsID, orch := range s.orchestrators {
		info := orch.LastQueryInfo()
		details[wsID] = info
	}
	s.muOrch.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"graph":         graphM,
		"context":       ctxM,
		"per_workspace": details,
	})
}

func (s *Server) handleQueryStatus(w http.ResponseWriter, r *http.Request) {
	s.handleOrchestratorStatus(w, r)
}
