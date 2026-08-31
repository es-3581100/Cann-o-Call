package server

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf8"

	"flatten-workspace/internal/diff"
)

type uiDiff struct {
	ReceiptID string
	Path      string
	Diff      string
}

func decodeB64OrEmpty(s string) ([]byte, error) {
	if s == "" {
		return []byte{}, nil
	}

	return base64.StdEncoding.DecodeString(s)
}

func (s *Server) receiptDiffData(receiptID string) (*uiDiff, error) {
	receipt, ok := s.Receipts.Get(receiptID)
	if !ok {
		return nil, fmt.Errorf("receipt %q not found", receiptID)
	}

	if receipt.Details == nil {
		return nil, errors.New("receipt has no details")
	}

	path, _ := receipt.Details["path"].(string)
	oldB64, _ := receipt.Details["old_content_base64"].(string)
	newB64, _ := receipt.Details["content_base64"].(string)

	if newB64 == "" {
		return nil, errors.New("receipt does not contain diffable content")
	}

	oldData, err := decodeB64OrEmpty(oldB64)
	if err != nil {
		return nil, fmt.Errorf("decode old content: %w", err)
	}

	newData, err := decodeB64OrEmpty(newB64)
	if err != nil {
		return nil, fmt.Errorf("decode new content: %w", err)
	}

	if !utf8.Valid(oldData) || !utf8.Valid(newData) {
		return nil, errors.New("diff currently supports UTF-8 text content only")
	}

	diffText := diff.Unified(string(oldData), string(newData), path)

	return &uiDiff{
		ReceiptID: receiptID,
		Path:      path,
		Diff:      diffText,
	}, nil
}

func (s *Server) jsonReceiptDiff(w http.ResponseWriter, r *http.Request) {
	receiptID := r.PathValue("receiptID")

	data, err := s.receiptDiffData(receiptID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"receipt_id": data.ReceiptID,
		"path":       data.Path,
		"diff":       data.Diff,
	})
}

func (s *Server) uiReceiptDiff(w http.ResponseWriter, r *http.Request) {
	receiptID := r.PathValue("receiptID")

	data, err := s.receiptDiffData(receiptID)
	if err != nil {
		s.uiRenderError(w, r, err.Error())
		return
	}

	s.renderAny(w, "diff", data)
}
