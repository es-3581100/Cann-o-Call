package server

import (
	"fmt"
	"net/http"
)

func (s *Server) jsonVerifyReceipts(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Receipts.Verify())
}

func (s *Server) uiVerifyReceipts(w http.ResponseWriter, r *http.Request) {
	result := s.Receipts.Verify()

	msg := fmt.Sprintf(
		"Receipt chain ok: %v\nChained receipts checked: %v\nLegacy receipts checked: %v\nLast receipt hash: %v",
		result["ok"],
		result["chained_checked"],
		result["legacy_checked"],
		result["last_hash"],
	)

	s.uiRenderResult(w, r, msg)
}
