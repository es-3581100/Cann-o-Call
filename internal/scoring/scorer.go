package scoring

import (
	"encoding/hex"
	"sort"
	"strings"
	"unicode"

	"flatten-workspace/internal/contextpacket"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
)

// BaselineScorer is a deterministic, stateless baseline scorer.
//
// Hierarchy: primary 1.0 (content), secondary 0.5 (structure), meta 0.25 (metadata).
// Max raw 12 = 4*1 + 8*0.5 + 16*0.25; max semantic 8 = 4*1 + 8*0.5.
// SemanticScore excludes meta so metadata-only tokens do not dominate.
type BaselineScorer struct{}

// ScoreComponents is an alias for the canonical contextpacket score fields.
type ScoreComponents = contextpacket.ScoreComponents

// ScoredNode is a scored file node with its packet and components.
type ScoredNode struct {
	Node       graph.Node                    `json:"node"`
	Packet     ingest.SourcePacket           `json:"packet"`
	Components contextpacket.ScoreComponents `json:"components"`
}

// TokenizeQuery tokenizes query deterministically: lowercase, split on non-alphanumeric, dedup, sort.
func TokenizeQuery(q string) []string {
	return tokenize(q)
}

// Score scores a single node deterministically against queryTokens (already tokenized, deduped, sorted).
// Only file-type semantics are scored; other node types return zero components with deterministic factors.
func (s *BaselineScorer) Score(node graph.Node, pkt ingest.SourcePacket, queryTokens []string) contextpacket.ScoreComponents {
	if len(queryTokens) == 0 {
		return zeroComponents()
	}

	// Deduplicate query tokens (caller may have already) and cap.
	// Ensure deterministic set.
	qSet := dedupSorted(queryTokens)
	if len(qSet) == 0 {
		return zeroComponents()
	}

	// Primary: NormalizedText tokens (content) up to 4 distinct matches.
	primarySet := tokenSet(pkt.Content.NormalizedText)
	primaryMatches := countMatches(qSet, primarySet)
	if primaryMatches > 4 {
		primaryMatches = 4
	}

	// Secondary: structural location (SourceLocator path tokens) up to 8 distinct.
	secondarySet := tokenSet(node.SourceLocator)
	secondaryMatches := countMatches(qSet, secondarySet)
	if secondaryMatches > 8 {
		secondaryMatches = 8
	}

	// Meta: metadata fields (Title, MIMEType, Language, DocumentProperties, OpenGraph, RawProperties, Author, etc.) up to 16.
	metaSet := metaTokenSet(pkt.Metadata)
	metaMatches := countMatches(qSet, metaSet)
	if metaMatches > 16 {
		metaMatches = 16
	}

	semanticScore := float64(primaryMatches)*1.0 + float64(secondaryMatches)*0.5
	if semanticScore > 8 {
		semanticScore = 8
	}
	rawScore := semanticScore + float64(metaMatches)*0.25
	if rawScore > 12 {
		rawScore = 12
	}

	structuralRelevance := 0.85
	if secondaryMatches > 0 {
		structuralRelevance = 1.0
	}

	graphProximity := 1.0

	sourceConfidence := 1.0
	if !isSHA256(node.ContentHash) && !isSHA256(pkt.Content.ContentHash) {
		// If no valid content hash available, penalize slightly.
		if strings.TrimSpace(node.ContentHash) == "" && strings.TrimSpace(pkt.Content.ContentHash) == "" {
			sourceConfidence = 0.5
		} else if !isSHA256(node.ContentHash) && strings.TrimSpace(node.ContentHash) != "" {
			sourceConfidence = 0.5
		}
	}
	// Also require node ContentHash to match packet when both present; mismatch reduces confidence.
	if isSHA256(node.ContentHash) && isSHA256(pkt.Content.ContentHash) && !strings.EqualFold(node.ContentHash, pkt.Content.ContentHash) {
		// Different hashes suggests stale mapping; half confidence.
		sourceConfidence = 0.5
	}

	coverage := 1.0

	activationScore := semanticScore * structuralRelevance * graphProximity * sourceConfidence * coverage

	return contextpacket.ScoreComponents{
		SemanticScore:       semanticScore,
		RawScore:            rawScore,
		StructuralRelevance: structuralRelevance,
		GraphProximity:      graphProximity,
		SourceConfidence:    sourceConfidence,
		Coverage:            coverage,
		ActivationScore:     activationScore,
		PrimaryMatches:      primaryMatches,
		SecondaryMatches:    secondaryMatches,
		MetaMatches:         metaMatches,
	}
}

// ScoreGraph scores all file nodes in graph deterministically.
// It sorts nodes by NodeID before scoring and sorts results deterministically.
func (s *BaselineScorer) ScoreGraph(g *graph.Graph, packets []ingest.SourcePacket, query string) []ScoredNode {
	if g == nil {
		return nil
	}
	queryTokens := TokenizeQuery(query)
	if len(queryTokens) == 0 {
		// Still return empty scored set deterministically (no activation without query).
		return nil
	}

	// Map packets by SourceID for O(1) lookup.
	packetBySource := make(map[string]ingest.SourcePacket, len(packets))
	for _, p := range packets {
		packetBySource[p.Identity.SourceID] = p
	}

	// Collect file nodes sorted by NodeID for deterministic iteration.
	nodes := g.SortedNodes()
	// Filter to file nodes only.
	fileNodes := make([]graph.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeType == graph.NodeTypeFile {
			fileNodes = append(fileNodes, n)
		}
	}

	out := make([]ScoredNode, 0, len(fileNodes))
	for _, n := range fileNodes {
		pkt, ok := packetBySource[n.SourceID]
		if !ok {
			// No packet for node: score with empty packet (meta zero).
			pkt = ingest.SourcePacket{}
		}
		comp := s.Score(n, pkt, queryTokens)
		out = append(out, ScoredNode{
			Node:       n,
			Packet:     pkt,
			Components: comp,
		})
	}

	// Sort results deterministically: ActivationScore desc, NodeID asc.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Components.ActivationScore == out[j].Components.ActivationScore {
			if out[i].Components.RawScore == out[j].Components.RawScore {
				return out[i].Node.NodeID < out[j].Node.NodeID
			}
			return out[i].Components.RawScore > out[j].Components.RawScore
		}
		return out[i].Components.ActivationScore > out[j].Components.ActivationScore
	})

	return out
}

// CoverageFor computes aggregate coverage diversity for a set of scored nodes.
// coverage = (uniqueSourceIDs + uniqueLocators) / (2 * total) — bounded 0..1, deterministic.
// For per-node scoring coverage remains 1.0; orchestrator may recompute aggregate.
func CoverageFor(scored []ScoredNode) float64 {
	if len(scored) == 0 {
		return 0
	}
	srcSet := make(map[string]bool, len(scored))
	locSet := make(map[string]bool, len(scored))
	for _, s := range scored {
		srcSet[s.Node.SourceID] = true
		locSet[s.Node.SourceLocator] = true
	}
	uSrc := float64(len(srcSet))
	uLoc := float64(len(locSet))
	total := float64(len(scored))
	return (uSrc/total + uLoc/total) / 2.0
}

// --- tokenization helpers ---

func tokenize(s string) []string {
	// Lowercase, split on non-alphanumeric (unicode letters/digits).
	lower := strings.ToLower(s)
	var b strings.Builder
	for _, r := range lower {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			b.WriteRune(' ')
		}
	}
	fields := strings.Fields(b.String())
	if len(fields) == 0 {
		return nil
	}
	// Dedup and sort.
	set := make(map[string]bool, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			set[f] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func tokenSet(s string) map[string]bool {
	toks := tokenize(s)
	set := make(map[string]bool, len(toks))
	for _, t := range toks {
		set[t] = true
	}
	return set
}

func metaTokenSet(m ingest.Metadata) map[string]bool {
	var combined strings.Builder
	combined.WriteString(m.Title)
	combined.WriteString(" ")
	combined.WriteString(m.MIMEType)
	combined.WriteString(" ")
	combined.WriteString(m.Language)
	combined.WriteString(" ")
	combined.WriteString(m.Author)
	combined.WriteString(" ")
	combined.WriteString(m.SourceTool)
	combined.WriteString(" ")
	combined.WriteString(m.DetectedKind)
	combined.WriteString(" ")
	combined.WriteString(m.Encoding)
	combined.WriteString(" ")
	for _, v := range m.DocumentProperties {
		combined.WriteString(v)
		combined.WriteString(" ")
	}
	for _, v := range m.OpenGraph {
		combined.WriteString(v)
		combined.WriteString(" ")
	}
	for _, v := range m.RawProperties {
		combined.WriteString(v)
		combined.WriteString(" ")
	}
	return tokenSet(combined.String())
}

func dedupSorted(tokens []string) []string {
	set := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" {
			set[t] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func countMatches(queryTokens []string, targetSet map[string]bool) int {
	count := 0
	for _, q := range queryTokens {
		if targetSet[q] {
			count++
		}
	}
	return count
}

func zeroComponents() contextpacket.ScoreComponents {
	return contextpacket.ScoreComponents{
		SemanticScore:       0,
		RawScore:            0,
		StructuralRelevance: 0.85,
		GraphProximity:      1.0,
		SourceConfidence:    1.0,
		Coverage:            1.0,
		ActivationScore:     0,
		PrimaryMatches:      0,
		SecondaryMatches:    0,
		MetaMatches:         0,
	}
}

func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
