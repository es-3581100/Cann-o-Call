package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ExtractionVersion is the canonical extraction identity version for CHUNK-05.
const ExtractionVersion = "v1"

// ExtractionIdentity is the extraction/version identity applied to all source packets.
const ExtractionIdentity = "ingest-v1"

// SourceType constants — at minimum cover what existing workspace already supports.
const (
	SourceTypeWorkspaceFile = "workspace_file"
	SourceTypeText          = "text"
	SourceTypeMarkdown      = "markdown"
	SourceTypeJSON          = "json"
	SourceTypeYAML          = "yaml"
	SourceTypeHTML          = "html"
	SourceTypeGeneric       = "generic"
	SourceTypeZipArchive    = "zip_archive"
)

// Span optionally locates a sub-range inside a source (byte offsets where available).
type Span struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// SourceIdentity is deterministic provenance for every admitted source.
type SourceIdentity struct {
	SourceID          string `json:"source_id"`
	SourceType        string `json:"source_type"`
	SourceRef         string `json:"source_ref"`
	SourceSHA256      string `json:"source_sha256"`
	WorkspaceID       string `json:"workspace_id"`
	ExtractionVersion string `json:"extraction_version"`
	ExtractionID      string `json:"extraction_id"`
	Locator           string `json:"locator"`
	Span              *Span  `json:"span,omitempty"`
}

// Provenance retains workspace + extraction + locator + event linkage.
type Provenance struct {
	WorkspaceID       string    `json:"workspace_id"`
	ExtractionID      string    `json:"extraction_id"`
	ExtractionVersion string    `json:"extraction_version"`
	SourceLocator     string    `json:"source_locator"`
	CreatedAt         time.Time `json:"created_at"`
	EventID           string    `json:"event_id,omitempty"`
}

// ComputeSHA256 returns lowercase hex SHA-256 of data.
func ComputeSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DeterministicSourceID derives source_id from stable inputs. Uses hex sha256 of canonical join.
// Never uses mutable pathname alone — includes bytes hash and workspace identity.
func DeterministicSourceID(workspaceID, sourceType, sourceRef, sourceSHA256, extractionVersion string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	sourceType = strings.TrimSpace(sourceType)
	sourceRef = strings.TrimSpace(sourceRef)
	sourceSHA256 = strings.ToLower(strings.TrimSpace(sourceSHA256))
	extractionVersion = strings.TrimSpace(extractionVersion)
	if extractionVersion == "" {
		extractionVersion = ExtractionVersion
	}
	payload := strings.Join([]string{workspaceID, sourceType, sourceRef, sourceSHA256, extractionVersion}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// DeterministicContentID derives content identity from sourceID + contentHash.
func DeterministicContentID(sourceID, contentHash string) string {
	payload := strings.Join([]string{strings.ToLower(sourceID), strings.ToLower(contentHash)}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// NewSourceIdentity builds a deterministic SourceIdentity. Locator defaults to SourceRef when empty.
func NewSourceIdentity(workspaceID, sourceType, sourceRef, sourceSHA256, locator string, span *Span) SourceIdentity {
	if locator == "" {
		locator = sourceRef
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	if sourceType == "" {
		sourceType = SourceTypeGeneric
	}
	sid := DeterministicSourceID(workspaceID, sourceType, sourceRef, sourceSHA256, ExtractionVersion)
	return SourceIdentity{
		SourceID:          sid,
		SourceType:        sourceType,
		SourceRef:         sourceRef,
		SourceSHA256:      strings.ToLower(sourceSHA256),
		WorkspaceID:       workspaceID,
		ExtractionVersion: ExtractionVersion,
		ExtractionID:      ExtractionIdentity,
		Locator:           locator,
		Span:              span,
	}
}

// Validate checks required fields are present and sha256-like.
func (s SourceIdentity) Validate() error {
	if strings.TrimSpace(s.SourceID) == "" {
		return fmt.Errorf("source_id required")
	}
	if strings.TrimSpace(s.SourceType) == "" {
		return fmt.Errorf("source_type required")
	}
	if strings.TrimSpace(s.SourceRef) == "" {
		return fmt.Errorf("source_ref required")
	}
	if !isSHA256(s.SourceSHA256) {
		return fmt.Errorf("source_sha256 invalid")
	}
	if strings.TrimSpace(s.WorkspaceID) == "" {
		return fmt.Errorf("workspace identity required")
	}
	if strings.TrimSpace(s.ExtractionVersion) == "" {
		return fmt.Errorf("extraction version required")
	}
	if strings.TrimSpace(s.Locator) == "" {
		return fmt.Errorf("locator required")
	}
	expected := DeterministicSourceID(s.WorkspaceID, s.SourceType, s.SourceRef, s.SourceSHA256, s.ExtractionVersion)
	if !strings.EqualFold(expected, s.SourceID) {
		return fmt.Errorf("source_id not deterministic for given inputs")
	}
	return nil
}

func isSHA256(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
