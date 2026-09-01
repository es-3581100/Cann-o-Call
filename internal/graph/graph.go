package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"flatten-workspace/internal/ingest"
)

// NodeType constants.
const (
	NodeTypeSource    = "source"
	NodeTypeFile      = "file"
	NodeTypeDirectory = "directory"
	NodeTypeChunk     = "chunk"
	NodeTypeMetadata  = "metadata"
)

// EdgeType constants — explicit, never imply semantic truth from coexistence.
const (
	EdgeContains    = "contains"
	EdgeDerivedFrom = "derived_from"
	EdgeSource      = "source"
	EdgeParent      = "parent"
	EdgeStructural  = "structural"
	EdgeHyperlink   = "hyperlink"
	EdgeReference   = "reference"
	EdgeSemantic    = "semantic"
)

// Provenance mirrors ingest.Provenance but stored per graph element for rebuild verification.
type Provenance struct {
	WorkspaceID       string `json:"workspace_id"`
	ExtractionID      string `json:"extraction_id"`
	ExtractionVersion string `json:"extraction_version"`
	SourceLocator     string `json:"source_locator"`
	EventID           string `json:"event_id,omitempty"`
}

// Node satisfies the typed contract: node_id, source_id, source_sha256, source_locator,
// node_type, content identity/hash, semantic content/reference, metadata reference,
// parent/containment, provenance, created/derived event identities.
type Node struct {
	NodeID         string     `json:"node_id"`
	SourceID       string     `json:"source_id"`
	SourceSHA256   string     `json:"source_sha256"`
	SourceLocator  string     `json:"source_locator"`
	NodeType       string     `json:"node_type"`
	ContentID      string     `json:"content_id,omitempty"`
	ContentHash    string     `json:"content_hash,omitempty"`
	ContentRef     string     `json:"content_ref,omitempty"`
	MetadataRef    string     `json:"metadata_ref,omitempty"`
	ParentID       string     `json:"parent_id,omitempty"`
	Provenance     Provenance `json:"provenance"`
	CreatedEventID string     `json:"created_event_id,omitempty"`
	DerivedFromID  string     `json:"derived_from_id,omitempty"`
	// SemanticContentRef is a reference (not embedded blob) to semantic content where large.
	SemanticRef string `json:"semantic_ref,omitempty"`
}

// Edge is deterministic typed relationship.
type Edge struct {
	EdgeID         string     `json:"edge_id"`
	FromID         string     `json:"from_id"`
	ToID           string     `json:"to_id"`
	EdgeType       string     `json:"edge_type"`
	Provenance     Provenance `json:"provenance"`
	CreatedEventID string     `json:"created_event_id,omitempty"`
}

// Graph holds nodes and edges deterministically. Nodes map is keyed by NodeID but exposed ordered.
type Graph struct {
	WorkspaceID string          `json:"workspace_id"`
	Version     string          `json:"version"`
	Nodes       map[string]Node `json:"nodes"`
	Edges       []Edge          `json:"edges"`
	Hash        string          `json:"hash"`
}

// DeterministicNodeID for file/source nodes.
func DeterministicNodeID(sourceID, sourceSHA, locator, nodeType, extractionVersion string) string {
	if extractionVersion == "" {
		extractionVersion = ingest.ExtractionVersion
	}
	payload := strings.Join([]string{sourceID, strings.ToLower(sourceSHA), locator, nodeType, extractionVersion}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// DeterministicEdgeID
func DeterministicEdgeID(fromID, toID, edgeType, workspaceID string) string {
	payload := strings.Join([]string{fromID, toID, edgeType, workspaceID}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// HashGraph computes deterministic hash over sorted nodes+edges via canonical JSON sorted keys.
func HashGraph(g *Graph) string {
	clone := *g
	clone.Hash = ""
	// Ensure canonical ordering: nodes sorted by key, edges sorted by from/to/type.
	nodesOrdered := map[string]Node{}
	keys := make([]string, 0, len(clone.Nodes))
	for k := range clone.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		nodesOrdered[k] = clone.Nodes[k]
	}
	clone.Nodes = nodesOrdered
	sort.Slice(clone.Edges, func(i, j int) bool {
		if clone.Edges[i].FromID == clone.Edges[j].FromID {
			if clone.Edges[i].ToID == clone.Edges[j].ToID {
				return clone.Edges[i].EdgeType < clone.Edges[j].EdgeType
			}
			return clone.Edges[i].ToID < clone.Edges[j].ToID
		}
		return clone.Edges[i].FromID < clone.Edges[j].FromID
	})
	b, err := canonicalJSON(mustJSON(clone))
	if err != nil {
		// fallback
		sum := sha256.Sum256([]byte(fmt.Sprintf("%v", clone)))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// canonicalJSON sorts object keys recursively — same as transition package.
func canonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := writeCanonical(&out, v); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeCanonical(out *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if x {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		b, _ := json.Marshal(x)
		out.Write(b)
	case json.Number:
		if _, err := json.Marshal(x); err != nil {
			return err
		}
		out.WriteString(x.String())
	case []any:
		out.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeCanonical(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			b, _ := json.Marshal(k)
			out.Write(b)
			out.WriteByte(':')
			if err := writeCanonical(out, x[k]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JSON value %T", v)
	}
	return nil
}

// SortedNodes returns nodes sorted by NodeID.
func (g *Graph) SortedNodes() []Node {
	keys := make([]string, 0, len(g.Nodes))
	for k := range g.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Node, 0, len(keys))
	for _, k := range keys {
		out = append(out, g.Nodes[k])
	}
	return out
}

// SortedEdges returns edges sorted.
func (g *Graph) SortedEdges() []Edge {
	cp := append([]Edge(nil), g.Edges...)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].FromID == cp[j].FromID {
			if cp[i].ToID == cp[j].ToID {
				return cp[i].EdgeType < cp[j].EdgeType
			}
			return cp[i].ToID < cp[j].ToID
		}
		return cp[i].FromID < cp[j].FromID
	})
	return cp
}
