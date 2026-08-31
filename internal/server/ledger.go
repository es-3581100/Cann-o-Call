package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func (s *Server) sidecarVerify() map[string]any {
	sidecarURL := os.Getenv("RUST_LEDGER_URL")
	if sidecarURL == "" {
		return nil
	}

	resp, err := http.Get(sidecarURL + "/events/verify")
	if err != nil {
		return map[string]any{
			"ok":    false,
			"error": err.Error(),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]any{
			"ok":    false,
			"error": fmt.Sprintf("read sidecar response: %v", err),
		}
	}

	if resp.StatusCode >= 300 {
		return map[string]any{
			"ok":    false,
			"error": fmt.Sprintf("sidecar status %d", resp.StatusCode),
			"body":  string(body),
		}
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return map[string]any{
			"ok":  false,
			"raw": string(body),
		}
	}

	return out
}

func (s *Server) jsonVerifyLedger(w http.ResponseWriter, r *http.Request) {
	local, err := s.Events.Verify()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	sidecar := s.sidecarVerify()

	writeJSON(w, http.StatusOK, map[string]any{
		"local":              local,
		"sidecar":            sidecar,
		"sidecar_configured": sidecar != nil,
	})
}

func (s *Server) uiVerifyLedger(w http.ResponseWriter, r *http.Request) {
	local, err := s.Events.Verify()
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	msg := fmt.Sprintf(
		"Local hash chain ok: %v\nEvents checked: %v\nLast hash: %v",
		local["ok"],
		local["events_checked"],
		local["last_hash"],
	)

	sidecar := s.sidecarVerify()
	if sidecar != nil {
		msg += fmt.Sprintf(
			"\n\nSidecar hash chain ok: %v\nSidecar events checked: %v",
			sidecar["ok"],
			sidecar["events_checked"],
		)
	}

	s.uiRenderResult(w, r, msg)
}
