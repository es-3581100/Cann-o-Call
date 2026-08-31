package server

import (
	"fmt"
	"net/http"
	"time"

	"flatten-workspace/internal/ids"
	"flatten-workspace/internal/projection"

	"gopkg.in/yaml.v3"
)

func (s *Server) verificationReport(w http.ResponseWriter, r *http.Request) {
	ws, ok := s.getWorkspaceForAPI(w, r)
	if !ok {
		return
	}

	reportID := ids.New("verification-report")

	receipt, err := s.recordGlobalTransition(
		r,
		ws.ID,
		"verification.report.generated",
		"Generate exportable verification report",
		nil,
		map[string]any{
			"report_id": reportID,
		},
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("record report transition: %w", err))
		return
	}

	replayResult, replayErr := s.opVerifyReplay(ws)

	localLedger, ledgerErr := s.Events.Verify()

	var sidecar map[string]any
	if v := s.sidecarVerify(); v != nil {
		sidecar = v
	}

	receiptResult := s.Receipts.Verify()

	report := map[string]any{
		"report_id":  reportID,
		"created_at": time.Now().UTC(),
		"workspace":  ws.Summary(),
		"binding":    ws.Binding,
		"quarantine": s.quarantineData(ws),
		"projection": projection.BuildFromWorkspace(ws),
		"replay":     replayResult,
		"ledger": map[string]any{
			"local":   localLedger,
			"sidecar": sidecar,
		},
		"receipts":           receiptResult,
		"issues":             ws.Issues,
		"generation_receipt": receipt,
	}

	if replayErr != nil {
		report["replay_error"] = replayErr.Error()
	}

	if ledgerErr != nil {
		report["ledger_error"] = ledgerErr.Error()
	}

	format := r.URL.Query().Get("format")
	if format != "yaml" {
		format = "json"
	}

	if r.URL.Query().Get("download") == "true" {
		w.Header().Set(
			"Content-Disposition",
			fmt.Sprintf(`attachment; filename="%s.%s"`, reportID, format),
		)
	}

	if format == "yaml" {
		b, err := yaml.Marshal(report)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(b)
		return
	}

	writeJSON(w, http.StatusOK, report)
}
