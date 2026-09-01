package graph

import (
	"path"
	"sort"
	"strings"

	"flatten-workspace/internal/ingest"
)

// Build deterministically creates a typed context graph from source packets.
// - Same bytes + extraction version + workspace identity => same NodeIDs/ContentHashes/Edges/Hash.
// - Filesystem traversal order or map iteration does not affect graph: sorts inputs.
// - Preserves provenance and two-stream separation (content vs metadata refs).
func Build(packets []ingest.SourcePacket) (*Graph, error) {
	if packets == nil {
		packets = []ingest.SourcePacket{}
	}
	// Canonical ordering: sort packets by SourceID.
	cp := append([]ingest.SourcePacket(nil), packets...)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Identity.SourceID < cp[j].Identity.SourceID
	})

	workspaceID := ""
	if len(cp) > 0 {
		workspaceID = cp[0].Identity.WorkspaceID
		// If multiple workspaceIDs present, keep first but include in hash; builder stays workspace-agnostic.
	}

	g := &Graph{
		WorkspaceID: workspaceID,
		Version:     ingest.ExtractionVersion,
		Nodes:       map[string]Node{},
		Edges:       []Edge{},
	}

	// Track directory nodes to create structural hierarchy.
	dirNodes := map[string]string{} // dir path -> nodeID

	var ensureDir func(dirPath string, prov Provenance) string
	ensureDir = func(dirPath string, prov Provenance) string {
		if dirPath == "" || dirPath == "." || dirPath == "/" {
			return ""
		}
		if id, ok := dirNodes[dirPath]; ok {
			return id
		}
		// Build parent first.
		parentDir := path.Dir(dirPath)
		var parentID string
		if parentDir != "." && parentDir != "/" && parentDir != dirPath {
			parentID = ensureDir(parentDir, prov)
		}
		// Directory node ID is deterministic from workspace+path (no source SHA — directory is structural).
		nodeID := DeterministicNodeID(workspaceID, "", dirPath, NodeTypeDirectory, ingest.ExtractionVersion)
		node := Node{
			NodeID:        nodeID,
			SourceID:      "", // structural, no single source
			SourceSHA256:  "",
			SourceLocator: dirPath,
			NodeType:      NodeTypeDirectory,
			ParentID:      parentID,
			Provenance:    prov,
		}
		g.Nodes[nodeID] = node
		dirNodes[dirPath] = nodeID
		// Edge parent -> dir
		if parentID != "" {
			eID := DeterministicEdgeID(parentID, nodeID, EdgeContains, workspaceID)
			g.Edges = append(g.Edges, Edge{
				EdgeID:     eID,
				FromID:     parentID,
				ToID:       nodeID,
				EdgeType:   EdgeContains,
				Provenance: prov,
			})
		}
		return nodeID
	}

	// For stable edge ordering later, collect then sort.

	for _, pkt := range cp {
		locator := pkt.Identity.Locator
		if locator == "" {
			locator = pkt.Identity.SourceRef
		}
		dir := path.Dir(locator)
		prov := Provenance{
			WorkspaceID:       pkt.Provenance.WorkspaceID,
			ExtractionID:      pkt.Provenance.ExtractionID,
			ExtractionVersion: pkt.Provenance.ExtractionVersion,
			SourceLocator:     locator,
			EventID:           pkt.Provenance.EventID,
		}
		// Ensure directory hierarchy nodes.
		var dirNodeID string
		if dir != "." && dir != "/" {
			dirNodeID = ensureDir(dir, prov)
		}

		// Source node — one per packet (provenance-bearing)
		sourceNodeID := DeterministicNodeID(pkt.Identity.SourceID, pkt.Identity.SourceSHA256, locator, NodeTypeSource, ingest.ExtractionVersion)
		if _, exists := g.Nodes[sourceNodeID]; !exists {
			g.Nodes[sourceNodeID] = Node{
				NodeID:         sourceNodeID,
				SourceID:       pkt.Identity.SourceID,
				SourceSHA256:   pkt.Identity.SourceSHA256,
				SourceLocator:  locator,
				NodeType:       NodeTypeSource,
				ContentID:      pkt.Content.ContentID,
				ContentHash:    pkt.Content.ContentHash,
				ContentRef:     pkt.Content.Ref,
				MetadataRef:    pkt.Metadata.SourceID, // reference, not embedded
				SemanticRef:    pkt.Content.Ref,
				Provenance:     prov,
				CreatedEventID: pkt.Provenance.EventID,
			}
		}

		// File node — semantic content node
		fileNodeID := DeterministicNodeID(pkt.Identity.SourceID, pkt.Content.ContentHash, locator, NodeTypeFile, ingest.ExtractionVersion)
		if _, exists := g.Nodes[fileNodeID]; !exists {
			g.Nodes[fileNodeID] = Node{
				NodeID:         fileNodeID,
				SourceID:       pkt.Identity.SourceID,
				SourceSHA256:   pkt.Identity.SourceSHA256,
				SourceLocator:  locator,
				NodeType:       NodeTypeFile,
				ContentID:      pkt.Content.ContentID,
				ContentHash:    pkt.Content.ContentHash,
				ContentRef:     pkt.Content.Ref,
				MetadataRef:    pkt.Metadata.SourceID,
				ParentID:       dirNodeID,
				Provenance:     prov,
				CreatedEventID: pkt.Provenance.EventID,
				DerivedFromID:  sourceNodeID,
			}
		}

		// Metadata node — separate stream reference
		metaNodeID := DeterministicNodeID(pkt.Identity.SourceID, pkt.Identity.SourceSHA256, locator+":meta", NodeTypeMetadata, ingest.ExtractionVersion)
		if _, exists := g.Nodes[metaNodeID]; !exists {
			g.Nodes[metaNodeID] = Node{
				NodeID:         metaNodeID,
				SourceID:       pkt.Identity.SourceID,
				SourceSHA256:   pkt.Identity.SourceSHA256,
				SourceLocator:  locator,
				NodeType:       NodeTypeMetadata,
				MetadataRef:    pkt.Metadata.SourceID,
				Provenance:     prov,
				CreatedEventID: pkt.Provenance.EventID,
			}
		}

		// Edges:
		// source -> file (derived_from)
		g.Edges = append(g.Edges, Edge{
			EdgeID:     DeterministicEdgeID(sourceNodeID, fileNodeID, EdgeDerivedFrom, workspaceID),
			FromID:     sourceNodeID,
			ToID:       fileNodeID,
			EdgeType:   EdgeDerivedFrom,
			Provenance: prov,
		})
		// file -> metadata (reference)
		g.Edges = append(g.Edges, Edge{
			EdgeID:     DeterministicEdgeID(fileNodeID, metaNodeID, EdgeReference, workspaceID),
			FromID:     fileNodeID,
			ToID:       metaNodeID,
			EdgeType:   EdgeReference,
			Provenance: prov,
		})
		// source -> metadata (source edge)
		g.Edges = append(g.Edges, Edge{
			EdgeID:     DeterministicEdgeID(sourceNodeID, metaNodeID, EdgeSource, workspaceID),
			FromID:     sourceNodeID,
			ToID:       metaNodeID,
			EdgeType:   EdgeSource,
			Provenance: prov,
		})
		// directory contains file
		if dirNodeID != "" {
			g.Edges = append(g.Edges, Edge{
				EdgeID:     DeterministicEdgeID(dirNodeID, fileNodeID, EdgeContains, workspaceID),
				FromID:     dirNodeID,
				ToID:       fileNodeID,
				EdgeType:   EdgeContains,
				Provenance: prov,
			})
			g.Edges = append(g.Edges, Edge{
				EdgeID:     DeterministicEdgeID(fileNodeID, dirNodeID, EdgeParent, workspaceID),
				FromID:     fileNodeID,
				ToID:       dirNodeID,
				EdgeType:   EdgeParent,
				Provenance: prov,
			})
		}
		// file hyperlink edge if HTML content contains href? Extract naive hyperlinks for explicit edge type test.
		// Only for HTML packets where content contains "http" or href.
		if pkt.Identity.SourceType == ingest.SourceTypeHTML && strings.Contains(strings.ToLower(pkt.Content.Text), "href=") {
			links := extractLinks(pkt.Content.Text)
			for _, link := range links {
				// hyperlink edge from file node to derived target ref (external)
				targetID := DeterministicNodeID(link, "", link, "external", ingest.ExtractionVersion)
				// Ensure external ref node exists as generic external? We add edge only, not node, to keep graph minimal.
				g.Edges = append(g.Edges, Edge{
					EdgeID:     DeterministicEdgeID(fileNodeID, targetID, EdgeHyperlink, workspaceID),
					FromID:     fileNodeID,
					ToID:       targetID,
					EdgeType:   EdgeHyperlink,
					Provenance: prov,
				})
			}
		}
	}

	// Stable edge dedup + sorting.
	edgeSeen := map[string]Edge{}
	for _, e := range g.Edges {
		if _, ok := edgeSeen[e.EdgeID]; !ok {
			edgeSeen[e.EdgeID] = e
		}
	}
	g.Edges = make([]Edge, 0, len(edgeSeen))
	for _, e := range edgeSeen {
		g.Edges = append(g.Edges, e)
	}
	sort.Slice(g.Edges, func(i, j int) bool {
		if g.Edges[i].FromID == g.Edges[j].FromID {
			if g.Edges[i].ToID == g.Edges[j].ToID {
				return g.Edges[i].EdgeType < g.Edges[j].EdgeType
			}
			return g.Edges[i].ToID < g.Edges[j].ToID
		}
		return g.Edges[i].FromID < g.Edges[j].FromID
	})

	g.Hash = HashGraph(g)
	return g, nil
}

// ExtractLinks naive for hyperlink edge creation.
func extractLinks(html string) []string {
	var out []string
	lower := strings.ToLower(html)
	idx := 0
	for {
		i := strings.Index(lower[idx:], "href=")
		if i == -1 {
			break
		}
		abs := idx + i + len("href=")
		if abs >= len(html) {
			break
		}
		rest := strings.TrimSpace(html[abs:])
		if rest == "" {
			break
		}
		quote := rest[0]
		var val string
		if quote == '"' || quote == '\'' {
			end := strings.IndexByte(rest[1:], quote)
			if end == -1 {
				break
			}
			val = rest[1 : 1+end]
			idx = abs + 1 + end + 1
		} else {
			end := strings.IndexAny(rest, " \t\n>")
			if end == -1 {
				val = rest
				idx = len(html)
			} else {
				val = rest[:end]
				idx = abs + end
			}
		}
		val = strings.TrimSpace(val)
		if val != "" {
			out = append(out, val)
		}
		if idx >= len(html) {
			break
		}
	}
	// dedup
	seen := map[string]bool{}
	uniq := []string{}
	for _, s := range out {
		if !seen[s] {
			seen[s] = true
			uniq = append(uniq, s)
		}
	}
	sort.Strings(uniq)
	return uniq
}

// Rebuild is idempotent — same as Build, discards projection and re-derives.
func Rebuild(packets []ingest.SourcePacket) (*Graph, error) {
	return Build(packets)
}
