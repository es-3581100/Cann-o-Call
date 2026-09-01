package graph

import (
	"sort"
	"testing"

	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/workspace"
)

func TestCH05_G01_EmptyGraph(t *testing.T) {
	g, err := Build(nil)
	if err != nil {
		t.Fatalf("Build empty: %v", err)
	}
	if len(g.Nodes) != 0 {
		t.Fatalf("expected 0 nodes got %d", len(g.Nodes))
	}
	if len(g.Edges) != 0 {
		t.Fatalf("expected 0 edges")
	}
	if g.Hash == "" {
		t.Fatalf("hash empty")
	}
}

func TestCH05_G02_SingleTextGraph(t *testing.T) {
	pkt, _ := ingest.IngestBytes("wsG", "hello.txt", "", []byte("hello"), "hello.txt")
	g, err := Build([]ingest.SourcePacket{*pkt})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(g.Nodes) == 0 {
		t.Fatalf("expected nodes")
	}
	if len(g.Edges) == 0 {
		t.Fatalf("expected edges")
	}
	// Check node contract
	foundFile := false
	for _, n := range g.Nodes {
		if n.NodeType == NodeTypeFile {
			foundFile = true
			if n.SourceID == "" || n.SourceSHA256 == "" || n.SourceLocator == "" || n.ContentHash == "" || n.ContentID == "" {
				t.Fatalf("file node missing required fields %#v", n)
			}
			if n.Provenance.WorkspaceID != "wsG" {
				t.Fatalf("provenance missing")
			}
		}
	}
	if !foundFile {
		t.Fatalf("file node not found")
	}
}

func TestCH05_G03_DeterministicNodeIdentity(t *testing.T) {
	pkt, _ := ingest.IngestBytes("wsD", "a.txt", "", []byte("data"), "a.txt")
	g1, _ := Build([]ingest.SourcePacket{*pkt})
	g2, _ := Build([]ingest.SourcePacket{*pkt})
	if g1.Hash != g2.Hash {
		t.Fatalf("hash not deterministic %s vs %s", g1.Hash, g2.Hash)
	}
	if len(g1.Nodes) != len(g2.Nodes) {
		t.Fatalf("node count mismatch")
	}
	ids1 := sortedNodeIDs(g1)
	ids2 := sortedNodeIDs(g2)
	for i := range ids1 {
		if ids1[i] != ids2[i] {
			t.Fatalf("node id mismatch %s vs %s", ids1[i], ids2[i])
		}
	}
}

func TestCH05_G05_DeterministicGraphRebuild(t *testing.T) {
	pkts := makePackets("wsR", []string{"a.txt", "b.txt"}, []string{"alpha", "beta"})
	g1, _ := Build(pkts)
	g2, _ := Rebuild(pkts)
	if g1.Hash != g2.Hash {
		t.Fatalf("rebuild hash mismatch")
	}
	// Discard and rebuild -> same
	g3, _ := Build(pkts)
	if g1.Hash != g3.Hash {
		t.Fatalf("second build hash mismatch")
	}
}

func TestCH05_G06_RepeatedIngestIdempotentViaStore(t *testing.T) {
	store := NewStore("wsI")
	pkt, _ := ingest.IngestBytes("wsI", "file.txt", "", []byte("content"), "file.txt")
	g1, dup1, err := store.Apply(*pkt)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if dup1 {
		t.Fatalf("first apply should not be dup")
	}
	g2, dup2, err := store.Apply(*pkt)
	if err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if !dup2 {
		t.Fatalf("second apply should be idempotent dup")
	}
	if g1.Hash != g2.Hash {
		t.Fatalf("idempotent hash mismatch")
	}
	if len(store.Packets()) != 1 {
		t.Fatalf("packet count should remain 1")
	}
}

func TestCH05_G07_ChangedBytesNewIdentity(t *testing.T) {
	pkt1, _ := ingest.IngestBytes("wsC", "file.txt", "", []byte("v1"), "file.txt")
	pkt2, _ := ingest.IngestBytes("wsC", "file.txt", "", []byte("v2"), "file.txt")
	if pkt1.Content.ContentHash == pkt2.Content.ContentHash {
		t.Fatalf("hash should differ")
	}
	g1, _ := Build([]ingest.SourcePacket{*pkt1})
	g2, _ := Build([]ingest.SourcePacket{*pkt2})
	if g1.Hash == g2.Hash {
		t.Fatalf("graph hash should differ for changed content")
	}
	// Different node IDs due to content hash part
	ids1 := sortedNodeIDs(g1)
	ids2 := sortedNodeIDs(g2)
	if len(ids1) != len(ids2) {
		// ok
	}
	mismatch := false
	for i := range ids1 {
		if ids1[i] != ids2[i] {
			mismatch = true
			break
		}
	}
	if !mismatch {
		t.Fatalf("node ids should differ for changed bytes")
	}
}

func TestCH05_G10_StructuralParentChildEdge(t *testing.T) {
	pkt, _ := ingest.IngestBytes("wsP", "dir/sub/file.txt", "", []byte("content"), "dir/sub/file.txt")
	g, _ := Build([]ingest.SourcePacket{*pkt})
	// Expect directory nodes for dir and dir/sub and contains edges
	dirFound := false
	containsFound := false
	parentFound := false
	for _, n := range g.Nodes {
		if n.NodeType == NodeTypeDirectory && n.SourceLocator == "dir" {
			dirFound = true
		}
	}
	for _, e := range g.Edges {
		if e.EdgeType == EdgeContains {
			containsFound = true
		}
		if e.EdgeType == EdgeParent {
			parentFound = true
		}
	}
	if !dirFound {
		t.Fatalf("directory node not found")
	}
	if !containsFound {
		t.Fatalf("contains edge not found")
	}
	if !parentFound {
		t.Fatalf("parent edge not found")
	}
}

func TestCH05_G11_ExplicitEdgeTypes(t *testing.T) {
	pkt, _ := ingest.IngestBytes("wsE", "a.txt", "", []byte("hi"), "a.txt")
	g, _ := Build([]ingest.SourcePacket{*pkt})
	types := map[string]bool{}
	for _, e := range g.Edges {
		types[e.EdgeType] = true
		if e.EdgeType == "" {
			t.Fatalf("edge type empty")
		}
	}
	// Should have derived_from, reference, source, plus contains/parent if dir; at least derived_from, reference, source
	for _, want := range []string{EdgeDerivedFrom, EdgeReference, EdgeSource} {
		if !types[want] {
			t.Fatalf("edge type %s missing, have %v", want, types)
		}
	}
	// Hyperlink edge for HTML
	htmlPkt, _ := ingest.IngestBytes("wsE", "page.html", "", []byte(`<a href="https://example.com">x</a>`), "page.html")
	g2, _ := Build([]ingest.SourcePacket{*htmlPkt})
	hasHyper := false
	for _, e := range g2.Edges {
		if e.EdgeType == EdgeHyperlink {
			hasHyper = true
			break
		}
	}
	if !hasHyper {
		t.Fatalf("hyperlink edge expected for HTML")
	}
}

func TestCH05_G12_StableOrdering(t *testing.T) {
	// Build with shuffled packet order still yields same hash due to sorting
	pkts := makePackets("wsO", []string{"c.txt", "a.txt", "b.txt"}, []string{"c", "a", "b"})
	g1, _ := Build(pkts)
	// Shuffle
	shuffled := []ingest.SourcePacket{pkts[2], pkts[0], pkts[1]}
	g2, _ := Build(shuffled)
	if g1.Hash != g2.Hash {
		t.Fatalf("stable ordering failed %s vs %s", g1.Hash, g2.Hash)
	}
	// Edges also sorted
	edges1 := g1.SortedEdges()
	edges2 := g2.SortedEdges()
	if len(edges1) != len(edges2) {
		t.Fatalf("edge count mismatch")
	}
	for i := range edges1 {
		if edges1[i].EdgeID != edges2[i].EdgeID {
			t.Fatalf("edge order not stable")
		}
	}
}

func TestCH05_G10_MetadataSeparationInGraph(t *testing.T) {
	pkt, _ := ingest.IngestBytes("wsM", "doc.html", "", []byte(`<title>T</title>< meta property="og:title" content="OG"><body>hello</body>`), "doc.html")
	g, _ := Build([]ingest.SourcePacket{*pkt})
	hasMetaNode := false
	for _, n := range g.Nodes {
		if n.NodeType == NodeTypeMetadata {
			hasMetaNode = true
			if n.MetadataRef == "" {
				t.Fatalf("metadata node missing ref")
			}
			// metadata node should not have ContentHash same as file? It may share source but separate
		}
		if n.NodeType == NodeTypeFile {
			if n.MetadataRef == "" {
				t.Fatalf("file node missing metadata ref")
			}
			// File node's content hash should be over stripped text, not include title metadata ingestion as keyword? Already separated in ingest
		}
	}
	if !hasMetaNode {
		t.Fatalf("metadata node missing")
	}
}

func TestCH05_GraphReferencesNotEmbeddingBlobs(t *testing.T) {
	large := make([]byte, 70*1024)
	for i := range large {
		large[i] = 'x'
	}
	pkt, _ := ingest.IngestBytes("wsB", "large.txt", "", large, "large.txt")
	if pkt.Content.Ref == "" {
		t.Fatalf("large should have ref")
	}
	g, _ := Build([]ingest.SourcePacket{*pkt})
	for _, n := range g.Nodes {
		if n.NodeType == NodeTypeFile {
			if n.ContentRef == "" {
				t.Fatalf("file node should have content ref for large blob")
			}
		}
	}
}

func TestCH05_WorkspaceIngestToGraphDeterminism(t *testing.T) {
	ws1 := &workspace.Workspace{ID: "wsW", Files: map[string]*workspace.File{
		"a.txt": {Path: "a.txt", Data: []byte("hello"), SHA256: ingest.ComputeSHA256([]byte("hello")), Verified: true},
		"b.md":  {Path: "b.md", Data: []byte("# title"), SHA256: ingest.ComputeSHA256([]byte("# title")), Verified: true},
	}}
	ws2 := &workspace.Workspace{ID: "wsW", Files: map[string]*workspace.File{
		"b.md":  {Path: "b.md", Data: []byte("# title"), SHA256: ingest.ComputeSHA256([]byte("# title")), Verified: true},
		"a.txt": {Path: "a.txt", Data: []byte("hello"), SHA256: ingest.ComputeSHA256([]byte("hello")), Verified: true},
	}}
	pkts1, _, _ := ingest.IngestWorkspace(ws1)
	pkts2, _, _ := ingest.IngestWorkspace(ws2)
	g1, _ := Build(pkts1)
	g2, _ := Build(pkts2)
	if g1.Hash != g2.Hash {
		t.Fatalf("workspace map iteration order should not affect graph hash")
	}
}

func makePackets(wsID string, paths []string, contents []string) []ingest.SourcePacket {
	var out []ingest.SourcePacket
	for i, p := range paths {
		pkt, _ := ingest.IngestBytes(wsID, p, "", []byte(contents[i]), p)
		out = append(out, *pkt)
	}
	return out
}

func sortedNodeIDs(g *Graph) []string {
	ids := make([]string, 0, len(g.Nodes))
	for id := range g.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
