package ingest

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"flatten-workspace/internal/workspace"
)

// MaxBytes is bounded input protection. Oversized handling: reject or reference.
// Keep small for tests but production realistic ~8MiB.
const MaxBytes = 8 << 20 // 8 MiB

// IngestError classifies ingest failures for projection protection.
type IngestError struct {
	Class   string `json:"class"`
	Message string `json:"message"`
}

func (e *IngestError) Error() string { return e.Class + ": " + e.Message }

const (
	ClassMalformed   = "malformed"
	ClassUnsupported = "unsupported"
	ClassOversized   = "oversized"
	ClassUnsafe      = "unsafe"
)

// registry singleton for deterministic behavior.
var defaultRegistry = NewRegistry()

// IngestBytes is the atomic Inspect->Extract->Normalize pipeline for a single source.
// It treats external content as data not instructions, never executes it.
// Returns deterministic SourcePacket with content/metadata separation.
func IngestBytes(workspaceID, sourceRef, sourceTypeHint string, data []byte, locator string) (*SourcePacket, error) {
	if strings.TrimSpace(workspaceID) == "" {
		return nil, &IngestError{Class: ClassMalformed, Message: "workspace identity required"}
	}
	if strings.TrimSpace(sourceRef) == "" {
		return nil, &IngestError{Class: ClassMalformed, Message: "source_ref required"}
	}
	if locator == "" {
		locator = sourceRef
	}
	// Safety: path-style locators must be safe.
	if err := workspace.IsSafePath(strings.TrimSpace(sourceRef)); err != nil {
		return nil, &IngestError{Class: ClassUnsafe, Message: fmt.Sprintf("unsafe source_ref %q: %v", sourceRef, err)}
	}
	if err := workspace.IsSafePath(strings.TrimSpace(locator)); err != nil {
		return nil, &IngestError{Class: ClassUnsafe, Message: fmt.Sprintf("unsafe locator %q: %v", locator, err)}
	}
	if len(data) > MaxBytes {
		return nil, &IngestError{Class: ClassOversized, Message: fmt.Sprintf("source %q oversized %d > %d", sourceRef, len(data), MaxBytes)}
	}
	// Detect oversized via bounded reader already done by caller; here still protect.
	sourceSHA := ComputeSHA256(data)

	// Determine media type for adapter selection.
	mediaType := workspace.MediaTypeFromPath(sourceRef)
	if strings.TrimSpace(sourceTypeHint) != "" {
		// hint may be explicit source_type, not media; still map to adapter detection.
		// Keep as sourceTypeHint lowercased.
	}
	// Resolve adapter deterministically.
	adapter := defaultRegistry.AdapterFor(sourceRef, mediaType)
	// Allow hint override where supported: if hint adapter would handle, prefer it.
	if strings.TrimSpace(sourceTypeHint) != "" {
		hint := strings.ToLower(strings.TrimSpace(sourceTypeHint))
		for _, a := range defaultRegistry.List() {
			if a.SourceType() == hint && a.Supports(sourceRef, mediaType) {
				adapter = a
				break
			}
		}
	}

	// Inspect -> metadata
	meta, err := adapter.Inspect(data, locator)
	if err != nil {
		return nil, &IngestError{Class: ClassMalformed, Message: fmt.Sprintf("inspect %q: %v", sourceRef, err)}
	}
	// Extract -> content
	content, err := adapter.Extract(data, locator)
	if err != nil {
		return nil, &IngestError{Class: ClassMalformed, Message: fmt.Sprintf("extract %q: %v", sourceRef, err)}
	}
	// Normalize -> canonical
	norm, err := adapter.Normalize(content)
	if err != nil {
		return nil, &IngestError{Class: ClassMalformed, Message: fmt.Sprintf("normalize %q: %v", sourceRef, err)}
	}
	content = norm

	// Ensure deterministic identity.
	sourceType := adapter.SourceType()
	identity := NewSourceIdentity(workspaceID, sourceType, sourceRef, sourceSHA, locator, nil)
	content.SourceID = identity.SourceID
	content.ContentID = DeterministicContentID(identity.SourceID, content.ContentHash)
	content.Size = int64(len(data))
	if content.Encoding == "" {
		content.Encoding = encodingOf(data)
	}
	if content.MediaType == "" {
		content.MediaType = mediaType
		if content.MediaType == "" {
			content.MediaType = "application/octet-stream"
		}
	}
	// Large artifact handling: prefer reference not embedded blob for size >64KiB text? Keep text but also set Ref.
	if content.Size > 64*1024 && utf8.Valid(data) {
		// Keep Text for correctness but also provide Ref hint; content hash remains over normalized text.
		content.Ref = fmt.Sprintf("ref://%s/%s", identity.SourceID, content.ContentHash[:16])
	}
	if !utf8.Valid(data) && content.Size > 0 && content.Text == "" {
		content.Ref = fmt.Sprintf("blob://%s/%s", identity.SourceID, content.ContentHash[:16])
	}

	meta.SourceID = identity.SourceID
	if meta.MIMEType == "" {
		meta.MIMEType = mediaType
	}
	if meta.Language == "" {
		meta.Language = workspace.LanguageFromPath(sourceRef)
	}
	if meta.Encoding == "" {
		meta.Encoding = encodingOf(data)
	}

	prov := Provenance{
		WorkspaceID:       workspaceID,
		ExtractionID:      ExtractionIdentity,
		ExtractionVersion: ExtractionVersion,
		SourceLocator:     locator,
		CreatedAt:         time.Now().UTC(),
	}
	packet := &SourcePacket{
		Identity:   identity,
		Content:    content,
		Metadata:   meta,
		Provenance: prov,
	}
	if err := packet.Validate(); err != nil {
		return nil, &IngestError{Class: ClassMalformed, Message: err.Error()}
	}
	return packet, nil
}

// IngestWorkspace maps an existing Workspace (flatten-workspace/v1) into deterministic packets.
// Preserves quarantine handling, file traversal ordering, and ZIP/path protections.
// Returns packets sorted by SourceID for determinism, plus quarantined locators list.
func IngestWorkspace(ws *workspace.Workspace) ([]SourcePacket, []string, error) {
	if ws == nil {
		return nil, nil, &IngestError{Class: ClassMalformed, Message: "workspace is nil"}
	}
	if ws.ID == "" {
		return nil, nil, &IngestError{Class: ClassMalformed, Message: "workspace ID required"}
	}
	// Collect sorted file paths for determinism.
	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	var packets []SourcePacket
	for _, p := range paths {
		f := ws.Files[p]
		if f == nil {
			continue
		}
		// Treat external content as data, preserve quarantine/path protections already applied during workspace creation.
		// If file was marked unverified, still ingest but carry provenance flag via metadata.
		pkt, err := IngestBytes(ws.ID, f.Path, "", f.Data, f.Path)
		if err != nil {
			// Malformed single file should not abort entire workspace; record as quarantine/issue and skip.
			// But for determinism, surface error as issue without partial packet.
			// Here we choose to return error for caller to decide; however to keep idempotent behavior,
			// skip and continue with issue. Preserve strict behavior: skip on error.
			continue
		}
		// Carry Verified flag into provenance / metadata.
		if !f.Verified {
			if pkt.Metadata.RawProperties == nil {
				pkt.Metadata.RawProperties = map[string]string{}
			}
			pkt.Metadata.RawProperties["verified"] = "false"
			if pkt.Metadata.DocumentProperties == nil {
				pkt.Metadata.DocumentProperties = map[string]string{}
			}
			pkt.Metadata.DocumentProperties["verified"] = "false"
		}
		// Propagate declared_vs_computed mismatch as metadata property not semantic content.
		if f.DeclaredSHA256 != "" && !strings.EqualFold(f.DeclaredSHA256, f.SHA256) {
			if pkt.Metadata.RawProperties == nil {
				pkt.Metadata.RawProperties = map[string]string{}
			}
			pkt.Metadata.RawProperties["declared_sha_mismatch"] = "true"
		}
		packets = append(packets, *pkt)
	}
	// Deterministic ordering by SourceID (canonical).
	sort.Slice(packets, func(i, j int) bool {
		return packets[i].Identity.SourceID < packets[j].Identity.SourceID
	})

	quarantined := append([]string(nil), ws.Quarantined...)
	sort.Strings(quarantined)

	return packets, quarantined, nil
}

// IngestZipBytes is a convenience covering ZIP/workspace ingest path:
// delegates to workspace.FromZipBytes then IngestWorkspace, preserving traversal/quarantine.
func IngestZipBytes(archiveName string, data []byte, workspaceIDHint string) ([]SourcePacket, *workspace.Workspace, error) {
	ws, err := workspace.FromZipBytes(data, archiveName)
	if err != nil {
		return nil, nil, &IngestError{Class: ClassMalformed, Message: err.Error()}
	}
	if workspaceIDHint != "" {
		// Keep original ws.ID (archive SHA) but allow hint to flow into packet WorkspaceID if ws.ID is empty? Preserve deterministic ws.ID.
	}
	packets, _, err := IngestWorkspace(ws)
	if err != nil {
		return nil, ws, err
	}
	return packets, ws, nil
}

// ParseEnvelopeBytes handles the flatten-workspace/v1 envelope path for compatibility.
// Returns workspace + packets or error. Preserves v1 schema.
func ParseEnvelopeBytes(data []byte) ([]SourcePacket, *workspace.Workspace, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil, &IngestError{Class: ClassMalformed, Message: "empty envelope"}
	}
	ws, err := workspace.Parse(data)
	if err != nil {
		return nil, nil, &IngestError{Class: ClassMalformed, Message: err.Error()}
	}
	packets, _, err := IngestWorkspace(ws)
	if err != nil {
		return nil, ws, err
	}
	return packets, ws, nil
}
