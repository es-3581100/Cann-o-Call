package graph

import (
	"sort"
	"sync"

	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/transition"
)

// Store holds the projected graph derived only from Rust-acknowledged accepted ingest events.
// It remains rebuildable: discard -> replay history -> same hash.
type Store struct {
	mu          sync.RWMutex
	graph       *Graph
	packets     map[string]ingest.SourcePacket // keyed by SourceID
	workspaceID string
}

func NewStore(workspaceID string) *Store {
	return &Store{
		graph: &Graph{
			WorkspaceID: workspaceID,
			Version:     ingest.ExtractionVersion,
			Nodes:       map[string]Node{},
			Edges:       []Edge{},
		},
		packets:     map[string]ingest.SourcePacket{},
		workspaceID: workspaceID,
	}
}

// Apply advances the projection only after successful authority admission.
// If the packet duplicates existing bytes (same SourceID/ContentHash), it is idempotent — no new version.
func (s *Store) Apply(pkt ingest.SourcePacket) (*Graph, bool, error) {
	if err := pkt.Validate(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Idempotent dedup: same SourceID and same ContentHash -> no change.
	if existing, ok := s.packets[pkt.Identity.SourceID]; ok {
		if existing.Content.ContentHash == pkt.Content.ContentHash && existing.Identity.SourceSHA256 == pkt.Identity.SourceSHA256 {
			// Duplicate exact bytes — idempotent, return existing graph unchanged but signal idempotent true.
			// Re-hash to ensure determinism.
			return s.graph, true, nil
		}
		// Same SourceID but different hash should not happen because SourceID includes hash; but handle as update.
		// Actually changed bytes create new SourceID, so this path corresponds to same path changed bytes handled via new identity; treat as replacement.
	}
	s.packets[pkt.Identity.SourceID] = pkt
	// Rebuild entire graph deterministically from all packets (ensures edges/nodes dedup).
	list := make([]ingest.SourcePacket, 0, len(s.packets))
	for _, p := range s.packets {
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Identity.SourceID < list[j].Identity.SourceID })
	g, err := Build(list)
	if err != nil {
		// Failure must not partially advance: revert packet insertion.
		delete(s.packets, pkt.Identity.SourceID)
		return nil, false, err
	}
	s.graph = g
	if s.workspaceID == "" {
		s.workspaceID = g.WorkspaceID
	}
	return g, false, nil
}

// RebuildFromAccepted rebuilds store from authority checkpoint/history.
// Implements the rebuildable invariant: accepted events/history -> discard -> rebuild -> same graph.
func (s *Store) RebuildFromAccepted(accepted []transition.AcceptedTransition) (*Graph, error) {
	packets, err := ingest.PacketsFromHistory(accepted)
	if err != nil {
		return nil, err
	}
	// Deterministic: sort before build.
	sort.Slice(packets, func(i, j int) bool { return packets[i].Identity.SourceID < packets[j].Identity.SourceID })
	g, err := Build(packets)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.packets = map[string]ingest.SourcePacket{}
	for _, p := range packets {
		s.packets[p.Identity.SourceID] = p
	}
	s.graph = g
	if len(packets) > 0 {
		s.workspaceID = packets[0].Identity.WorkspaceID
	}
	return g, nil
}

// Graph returns current projected graph snapshot (copy for race safety).
func (s *Store) Graph() *Graph {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.graph == nil {
		return &Graph{Nodes: map[string]Node{}, Edges: []Edge{}}
	}
	// Deep copy via struct copy + map copy.
	cp := *s.graph
	cp.Nodes = make(map[string]Node, len(s.graph.Nodes))
	for k, v := range s.graph.Nodes {
		cp.Nodes[k] = v
	}
	cp.Edges = append([]Edge(nil), s.graph.Edges...)
	return &cp
}

// Packets returns sorted packets copy.
func (s *Store) Packets() []ingest.SourcePacket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ingest.SourcePacket, 0, len(s.packets))
	for _, p := range s.packets {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Identity.SourceID < out[j].Identity.SourceID })
	return out
}
