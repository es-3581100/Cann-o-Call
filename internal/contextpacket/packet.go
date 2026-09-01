package contextpacket

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Version is the canonical ContextPacket version.
const Version = "v1"

// Bounds for evidence — enforced by Validate and construction helpers.
const (
	MaxEvidence     = 5
	MaxExcerptBytes = 2048
)

// Lineage carries bounded actor lineage/budget for governed activation.
// It mirrors internal/actorstub.Activation lineage fields.
type Lineage struct {
	LineageID       string `json:"lineage_id"`
	ParentActorID   string `json:"parent_actor_id,omitempty"`
	Depth           int    `json:"depth"`
	RemainingBudget int    `json:"remaining_budget"`
}

// EvidenceItem is a single bounded, addressable evidence reference.
// It is traceable to source/node via NodeID/SourceID/Locator/ContentHash.
type EvidenceItem struct {
	NodeID      string `json:"node_id"`
	SourceID    string `json:"source_id"`
	Locator     string `json:"locator"`
	ContentHash string `json:"content_hash"`
	Excerpt     string `json:"excerpt,omitempty"`
	Reference   string `json:"reference,omitempty"`
	MetadataRef string `json:"metadata_ref,omitempty"`
}

// ScoreComponents preserves distinct raw score components for audit.
// Primary/secondary/meta matches are raw counts; semantic/raw/activation are weighted.
type ScoreComponents struct {
	SemanticScore       float64 `json:"semantic_score"`
	RawScore            float64 `json:"raw_score"`
	StructuralRelevance float64 `json:"structural_relevance"`
	GraphProximity      float64 `json:"graph_proximity"`
	SourceConfidence    float64 `json:"source_confidence"`
	Coverage            float64 `json:"coverage"`
	ActivationScore     float64 `json:"activation_score"`
	PrimaryMatches      int     `json:"primary_matches"`
	SecondaryMatches    int     `json:"secondary_matches"`
	MetaMatches         int     `json:"meta_matches"`
}

// ContextPacket is the bounded, typed message delivered to an actor.
// It never embeds the whole workspace/graph — only bounded evidence refs.
type ContextPacket struct {
	RequestID     string          `json:"request_id"`
	Query         string          `json:"query"`
	Intent        string          `json:"intent,omitempty"`
	WorkspaceID   string          `json:"workspace_id"`
	TargetNodeID  string          `json:"target_node_id"`
	TargetActorID string          `json:"target_actor_id,omitempty"`
	Evidence      []EvidenceItem  `json:"evidence"`
	Scores        ScoreComponents `json:"score_components"`
	Lineage       Lineage         `json:"lineage"`
	Timestamp     time.Time       `json:"timestamp"`
	Version       string          `json:"version"`
}

// Validate checks bounded invariants and traceability.
func (p ContextPacket) Validate() error {
	if strings.TrimSpace(p.RequestID) == "" {
		return fmt.Errorf("request_id required")
	}
	if strings.TrimSpace(p.Query) == "" && strings.TrimSpace(p.Intent) == "" {
		return fmt.Errorf("query/intent required")
	}
	if strings.TrimSpace(p.WorkspaceID) == "" {
		return fmt.Errorf("workspace_id required")
	}
	if strings.TrimSpace(p.TargetNodeID) == "" && strings.TrimSpace(p.TargetActorID) == "" {
		return fmt.Errorf("target actor/node identity required")
	}
	if strings.TrimSpace(p.Version) == "" {
		return fmt.Errorf("version required")
	}
	if p.Version != Version {
		return fmt.Errorf("version must be %q", Version)
	}
	if p.Timestamp.IsZero() {
		return fmt.Errorf("timestamp required")
	}
	if strings.TrimSpace(p.Lineage.LineageID) == "" {
		return fmt.Errorf("lineage_id required")
	}
	if p.Lineage.Depth < 0 {
		return fmt.Errorf("depth must be >=0")
	}
	if p.Lineage.RemainingBudget < 0 {
		return fmt.Errorf("remaining_budget must be >=0")
	}
	if len(p.Evidence) > MaxEvidence {
		return fmt.Errorf("evidence exceeds max %d: %d", MaxEvidence, len(p.Evidence))
	}
	// Evidence must be deterministically ordered by NodeID for canonical form.
	if !isSortedByNodeID(p.Evidence) {
		return fmt.Errorf("evidence must be sorted by node_id")
	}
	seenNode := make(map[string]bool, len(p.Evidence))
	for i, ev := range p.Evidence {
		if err := ev.Validate(); err != nil {
			return fmt.Errorf("evidence[%d]: %w", i, err)
		}
		if seenNode[ev.NodeID] {
			return fmt.Errorf("evidence[%d]: duplicate node_id %q", i, ev.NodeID)
		}
		seenNode[ev.NodeID] = true
	}
	return nil
}

// Validate checks evidence bounded invariants.
func (e EvidenceItem) Validate() error {
	if strings.TrimSpace(e.NodeID) == "" {
		return fmt.Errorf("node_id required")
	}
	if strings.TrimSpace(e.SourceID) == "" {
		return fmt.Errorf("source_id required")
	}
	if strings.TrimSpace(e.Locator) == "" {
		return fmt.Errorf("locator required")
	}
	if strings.TrimSpace(e.ContentHash) == "" {
		return fmt.Errorf("content_hash required")
	}
	if !isSHA256(e.ContentHash) {
		return fmt.Errorf("content_hash must be sha256 hex")
	}
	if len([]byte(e.Excerpt)) > MaxExcerptBytes {
		return fmt.Errorf("excerpt exceeds %d bytes: %d", MaxExcerptBytes, len([]byte(e.Excerpt)))
	}
	return nil
}

// isSHA256 checks 64-char hex sha256.
func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// isSortedByNodeID reports whether evidence is sorted asc by NodeID.
func isSortedByNodeID(ev []EvidenceItem) bool {
	for i := 1; i < len(ev); i++ {
		if ev[i-1].NodeID > ev[i].NodeID {
			return false
		}
	}
	return true
}

// SortEvidence sorts evidence deterministically by NodeID.
func SortEvidence(ev []EvidenceItem) {
	sort.Slice(ev, func(i, j int) bool { return ev[i].NodeID < ev[j].NodeID })
}

// BoundedExcerpt returns excerpt truncated to MaxExcerptBytes without breaking UTF-8.
func BoundedExcerpt(s string) string {
	b := []byte(s)
	if len(b) <= MaxExcerptBytes {
		return s
	}
	trunc := b[:MaxExcerptBytes]
	// Trim to last valid UTF-8 boundary.
	for len(trunc) > 0 && !utf8.Valid(trunc) {
		trunc = trunc[:len(trunc)-1]
	}
	return string(trunc)
}
