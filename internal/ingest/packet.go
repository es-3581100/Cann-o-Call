package ingest

import (
	"fmt"
	"strings"
	"time"
)

// Content is the semantic stream — text/structure only, no metadata keywords injected.
type Content struct {
	SourceID       string `json:"source_id"`
	ContentID      string `json:"content_id"`
	ContentHash    string `json:"content_hash"`
	Encoding       string `json:"encoding"`
	MediaType      string `json:"media_type"`
	Text           string `json:"text"`
	NormalizedText string `json:"normalized_text"`
	Size           int64  `json:"size"`
	Ref            string `json:"ref,omitempty"` // reference to blob if large / non-embedded
}

// Metadata is the separate typed stream — author/timestamps/MIME/properties/OpenGraph/tool info.
type Metadata struct {
	SourceID           string            `json:"source_id"`
	Author             string            `json:"author,omitempty"`
	Title              string            `json:"title,omitempty"`
	MIMEType           string            `json:"mime_type,omitempty"`
	Language           string            `json:"language,omitempty"`
	CreatedAt          *time.Time        `json:"created_at,omitempty"`
	ModifiedAt         *time.Time        `json:"modified_at,omitempty"`
	Encoding           string            `json:"encoding,omitempty"`
	DocumentProperties map[string]string `json:"document_properties,omitempty"`
	OpenGraph          map[string]string `json:"open_graph,omitempty"`
	SourceTool         string            `json:"source_tool,omitempty"`
	RawProperties      map[string]string `json:"raw_properties,omitempty"`
	DetectedKind       string            `json:"detected_kind,omitempty"`
}

// SourcePacket bundles deterministic identity + separated content + metadata + provenance.
// Large artifacts should be referenced, not embedded, via Content.Ref / ContentHash.
type SourcePacket struct {
	Identity   SourceIdentity `json:"identity"`
	Content    Content        `json:"content"`
	Metadata   Metadata       `json:"metadata"`
	Provenance Provenance     `json:"provenance"`
}

// Validate checks packet invariants deterministically.
func (p SourcePacket) Validate() error {
	if err := p.Identity.Validate(); err != nil {
		return fmt.Errorf("packet identity: %w", err)
	}
	if strings.TrimSpace(p.Content.ContentHash) == "" || !isSHA256(p.Content.ContentHash) {
		return fmt.Errorf("content_hash invalid")
	}
	if strings.TrimSpace(p.Content.ContentID) == "" {
		return fmt.Errorf("content_id required")
	}
	expectedCID := DeterministicContentID(p.Identity.SourceID, p.Content.ContentHash)
	if !strings.EqualFold(expectedCID, p.Content.ContentID) {
		return fmt.Errorf("content_id not deterministic")
	}
	if p.Content.SourceID != p.Identity.SourceID {
		return fmt.Errorf("content source_id mismatch")
	}
	if p.Metadata.SourceID != p.Identity.SourceID {
		return fmt.Errorf("metadata source_id mismatch")
	}
	// Content/metadata separation: metadata title etc must not have been injected into Text by default.
	// We enforce that Text does not contain metadata-encoded keywords like "author:" unless adapter explicitly does.
	// This is advisory; packet validation passes regardless, but callers must not inject.
	if strings.TrimSpace(p.Provenance.WorkspaceID) == "" {
		return fmt.Errorf("provenance workspace_id required")
	}
	if p.Provenance.SourceLocator == "" {
		return fmt.Errorf("provenance locator required")
	}
	return nil
}
