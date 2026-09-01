package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/contextpacket"
	"flatten-workspace/internal/graph"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/scoring"
	"flatten-workspace/internal/transition"
)

// QueryResult is the deterministic result of a query -> score -> actor -> result pipeline.
type QueryResult struct {
	RequestID            string                           `json:"request_id"`
	Query                string                           `json:"query"`
	WorkspaceID          string                           `json:"workspace_id"`
	CandidatesConsidered int                              `json:"candidates_considered"`
	Selected             []scoring.ScoredNode             `json:"selected"`
	Packets              []contextpacket.ContextPacket    `json:"packets"`
	Activations          []*actorstub.Activation          `json:"activations"`
	Results              map[string]actorstub.ActorResult `json:"results"`
	RejectedCount        int                              `json:"rejected_count"`
	Coverage             float64                          `json:"coverage"`
}

// QueryInfo exposes last query observability without leaking internals.
type QueryInfo struct {
	RequestID            string  `json:"request_id"`
	Query                string  `json:"query"`
	CandidatesConsidered int     `json:"candidates_considered"`
	Selected             int     `json:"selected"`
	Packets              int     `json:"packets"`
	Activations          int     `json:"activations"`
	Rejected             int     `json:"rejected"`
	Active               int     `json:"active"`
	Completed            int     `json:"completed"`
	Coverage             float64 `json:"coverage"`
}

// Orchestrator proves ingest->graph->score->actor->result and
// ingest->graph->score->actor proposal->Go->Rust->projection without LLM.
type Orchestrator struct {
	workspaceID string
	graphStore  *graph.Store
	authority   *transition.Authority
	actors      *actorstub.Controller
	scorer      scoring.BaselineScorer
	selectCfg   scoring.Config

	mu sync.Mutex

	lastRequest    string
	lastQuery      string
	lastCandidates []scoring.ScoredNode
	lastSelected   []scoring.ScoredNode
	lastPackets    []contextpacket.ContextPacket
	lastInfo       QueryInfo

	packets map[string]ingest.SourcePacket
}

// New creates an orchestrator. workspaceID is required. store, authority, ctrl may be nil only for tests
// but Query/Ingest will fail-closed if they are nil. cfg zero values are replaced by bounded defaults.
func New(workspaceID string, store *graph.Store, auth *transition.Authority, ctrl *actorstub.Controller, cfg scoring.Config) *Orchestrator {
	cfg = cfg.WithDefaults()
	return &Orchestrator{
		workspaceID: workspaceID,
		graphStore:  store,
		authority:   auth,
		actors:      ctrl,
		scorer:      scoring.BaselineScorer{},
		selectCfg:   cfg,
		packets:     map[string]ingest.SourcePacket{},
	}
}

// Ingest proposes packet via Go admission authority (requires Rust ACK), then applies to graphStore on success.
// Duplicate TransitionID is idempotent. Fail-closed on Rust unavailable; no graph update on error.
// Actors are NOT activated on ingest alone.
func (o *Orchestrator) Ingest(packet ingest.SourcePacket) (*graph.Graph, error) {
	if strings.TrimSpace(o.workspaceID) == "" {
		return nil, fmt.Errorf("workspace_id required")
	}
	if o.authority == nil {
		return nil, fmt.Errorf("authority is nil")
	}
	if o.graphStore == nil {
		return nil, fmt.Errorf("graph store is nil")
	}
	if err := packet.Validate(); err != nil {
		return nil, fmt.Errorf("packet invalid: %w", err)
	}
	if packet.Identity.WorkspaceID != o.workspaceID {
		return nil, fmt.Errorf("packet workspace %q does not match orchestrator %q", packet.Identity.WorkspaceID, o.workspaceID)
	}
	// Propose via Go authority -> Rust ACK.
	accepted, err := ingest.ProposeIngest(o.authority, packet)
	if err != nil {
		return nil, err
	}
	// Apply to projection only after ACK. Duplicate is still idempotent.
	_ = accepted
	g, _, err := o.graphStore.Apply(packet)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.packets[packet.Identity.SourceID] = packet
	o.mu.Unlock()
	return g, nil
}

// Query is the no-mutation observation path: query -> score -> select -> activate -> bounded result.
// It never mutates canonical state durably. Use ProposeMutation for the mutating path.
func (o *Orchestrator) Query(requestID, query, parentActorID string) (*QueryResult, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("request_id required")
	}
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query required")
	}
	if strings.TrimSpace(o.workspaceID) == "" {
		return nil, fmt.Errorf("workspace_id required")
	}
	if o.graphStore == nil {
		return nil, fmt.Errorf("graph store is nil")
	}
	if o.actors == nil {
		return nil, fmt.Errorf("actor controller is nil")
	}

	g := o.graphStore.Graph()
	packets := o.graphStore.Packets()

	scored := o.scorer.ScoreGraph(g, packets, query)
	candidatesConsidered := len(scored)

	selected := scoring.SelectCandidates(scored, o.selectCfg)

	queryTokens := scoring.TokenizeQuery(query)

	// Build activations and packets deterministically.
	var (
		activations   []*actorstub.Activation
		ctxPackets    []contextpacket.ContextPacket
		results       = map[string]actorstub.ActorResult{}
		rejectedCount int
	)

	// Ensure deterministic iteration: selected already sorted by scoring.SelectCandidates.
	for _, sn := range selected {
		nodeID := sn.Node.NodeID
		act := o.actors.ActivateWithParent(parentActorID, o.workspaceID, "live_context.query", nodeID)
		if act == nil {
			// Within-window dedup suppressed.
			rejectedCount++
			continue
		}
		activations = append(activations, act)
		if strings.HasPrefix(act.Status, "rejected_") {
			rejectedCount++
			continue
		}
		// Success: active.
		pkt, err := buildContextPacket(requestID, query, o.workspaceID, sn, queryTokens, act)
		if err != nil {
			// Validation failed: mark actor failed and continue deterministically.
			o.actors.Fail(act.ID, fmt.Sprintf("packet validate: %v", err))
			rejectedCount++
			continue
		}
		ctxPackets = append(ctxPackets, pkt)

		// Deterministically evaluate bounded evidence -> ActorResult.
		res := buildActorResult(act, pkt, sn)
		o.actors.RecordResult(res)
		o.actors.Complete(act.ID)
		results[act.ID] = res
	}

	// Sort packets by target node id for determinism.
	sort.Slice(ctxPackets, func(i, j int) bool { return ctxPackets[i].TargetNodeID < ctxPackets[j].TargetNodeID })
	// Activations already in selected order (deterministic), but sort copy by ID for observability stability.
	sort.Slice(activations, func(i, j int) bool { return activations[i].ID < activations[j].ID })

	coverage := scoring.CoverageFor(selected)

	qr := &QueryResult{
		RequestID:            requestID,
		Query:                query,
		WorkspaceID:          o.workspaceID,
		CandidatesConsidered: candidatesConsidered,
		Selected:             selected,
		Packets:              ctxPackets,
		Activations:          activations,
		Results:              results,
		RejectedCount:        rejectedCount,
		Coverage:             coverage,
	}

	// Observability.
	o.mu.Lock()
	o.lastRequest = requestID
	o.lastQuery = query
	o.lastCandidates = append([]scoring.ScoredNode(nil), scored...)
	o.lastSelected = append([]scoring.ScoredNode(nil), selected...)
	o.lastPackets = append([]contextpacket.ContextPacket(nil), ctxPackets...)
	active, completed, rejected := countActivationStatuses(activations, rejectedCount)
	o.lastInfo = QueryInfo{
		RequestID:            requestID,
		Query:                query,
		CandidatesConsidered: candidatesConsidered,
		Selected:             len(selected),
		Packets:              len(ctxPackets),
		Activations:          len(activations),
		Rejected:             rejected,
		Active:               active,
		Completed:            completed,
		Coverage:             coverage,
	}
	o.mu.Unlock()

	return qr, nil
}

// ProposeMutation maps an ActorResult to a ProposedTransition via Go authority -> Rust ACK -> projection.
// Actors never call Rust directly. Returns AcceptedTransition on success; invalid/ Rust unavailable fail-closed.
func (o *Orchestrator) ProposeMutation(actorID string, resultData json.RawMessage, admissionData json.RawMessage) (transition.AcceptedTransition, error) {
	if o.authority == nil {
		return transition.AcceptedTransition{}, fmt.Errorf("authority is nil")
	}
	if strings.TrimSpace(actorID) == "" {
		return transition.AcceptedTransition{}, fmt.Errorf("actor_id required")
	}
	if len(resultData) == 0 {
		return transition.AcceptedTransition{}, fmt.Errorf("result_data required")
	}
	if len(admissionData) == 0 {
		admissionData = json.RawMessage(`{}`)
	}
	// Validate resultData is canonical JSON.
	if _, err := canonicalJSON(resultData); err != nil {
		return transition.AcceptedTransition{}, fmt.Errorf("result_data malformed: %w", err)
	}
	if _, err := canonicalJSON(admissionData); err != nil {
		return transition.AcceptedTransition{}, fmt.Errorf("admission_data malformed: %w", err)
	}

	// Lookup descriptor for target node identity and lineage.
	var targetNode string
	if o.actors != nil {
		if desc, ok := o.actors.Descriptor(actorID); ok {
			// NodeID field in descriptor actually stores workspaceID; use Path which holds query nodeID.
			if desc.Path != "" {
				targetNode = desc.Path
			} else if desc.NodeID != "" {
				targetNode = desc.NodeID
			}
		}
	}
	if targetNode == "" {
		// Fallback to actorID as node key if not found; still deterministic.
		targetNode = actorID
	}

	// Deterministic transition identity: workspace + mutation + actorID + hash(resultData)
	hash := sha256.Sum256(resultData)
	hashHex := hex.EncodeToString(hash[:])
	transitionID := ingest.DeterministicSourceID(o.workspaceID, "transition", actorID, hashHex, ingest.ExtractionVersion)
	proposalID := "proposal-" + transitionID[:16]
	requestID := "request-" + transitionID[:16]
	prior := o.authority.Projection().Ref()

	proposal := transition.ProposedTransition{
		TransitionID:  transitionID,
		ProposalID:    proposalID,
		RequestID:     requestID,
		Prior:         prior,
		Operation:     "upsert",
		Entity:        o.workspaceID,
		Node:          targetNode,
		ResultData:    json.RawMessage(cloneRaw(resultData)),
		AdmissionData: json.RawMessage(cloneRaw(admissionData)),
	}
	return o.authority.Propose(proposal)
}

// LastQueryInfo returns observability snapshot of last Query.
func (o *Orchestrator) LastQueryInfo() QueryInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastInfo
}

// GetGraph returns current graph snapshot.
func (o *Orchestrator) GetGraph() *graph.Graph {
	if o.graphStore == nil {
		return &graph.Graph{Nodes: map[string]graph.Node{}, Edges: []graph.Edge{}}
	}
	return o.graphStore.Graph()
}

// ListPackets returns sorted packets copy from store and local ingest tracking.
func (o *Orchestrator) ListPackets() []ingest.SourcePacket {
	if o.graphStore == nil {
		o.mu.Lock()
		defer o.mu.Unlock()
		out := make([]ingest.SourcePacket, 0, len(o.packets))
		for _, p := range o.packets {
			out = append(out, p)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Identity.SourceID < out[j].Identity.SourceID })
		return out
	}
	return o.graphStore.Packets()
}

// LastCandidates returns copy of last scored candidates.
func (o *Orchestrator) LastCandidates() []scoring.ScoredNode {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]scoring.ScoredNode(nil), o.lastCandidates...)
}

// LastSelected returns copy of last selected.
func (o *Orchestrator) LastSelected() []scoring.ScoredNode {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]scoring.ScoredNode(nil), o.lastSelected...)
}

// LastPackets returns copy of last context packets.
func (o *Orchestrator) LastPackets() []contextpacket.ContextPacket {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]contextpacket.ContextPacket(nil), o.lastPackets...)
}

// --- helpers ---

func buildContextPacket(requestID, query, workspaceID string, sn scoring.ScoredNode, queryTokens []string, act *actorstub.Activation) (contextpacket.ContextPacket, error) {
	evidence := buildEvidence(sn.Node, sn.Packet, queryTokens)
	// Ensure sorted by NodeID.
	contextpacket.SortEvidence(evidence)

	lineage := contextpacket.Lineage{
		LineageID:       act.LineageID,
		ParentActorID:   act.ParentActorID,
		Depth:           act.Depth,
		RemainingBudget: act.RemainingBudget,
	}

	pkt := contextpacket.ContextPacket{
		RequestID:     requestID,
		Query:         query,
		WorkspaceID:   workspaceID,
		TargetNodeID:  sn.Node.NodeID,
		TargetActorID: act.ID,
		Evidence:      evidence,
		Scores:        sn.Components,
		Lineage:       lineage,
		Timestamp:     time.Now().UTC(),
		Version:       contextpacket.Version,
	}
	if err := pkt.Validate(); err != nil {
		return contextpacket.ContextPacket{}, err
	}
	return pkt, nil
}

func buildEvidence(node graph.Node, pkt ingest.SourcePacket, queryTokens []string) []contextpacket.EvidenceItem {
	// Bounded excerpt source: NormalizedText preferred, fallback to Text.
	src := pkt.Content.NormalizedText
	if strings.TrimSpace(src) == "" {
		src = pkt.Content.Text
	}
	excerpt := contextpacket.BoundedExcerpt(src)
	contentHash := node.ContentHash
	if !isSHA256(contentHash) {
		contentHash = pkt.Content.ContentHash
	}
	if !isSHA256(contentHash) {
		// Fallback: compute hash of excerpt.
		sum := sha256.Sum256([]byte(excerpt))
		contentHash = hex.EncodeToString(sum[:])
	}
	locator := node.SourceLocator
	if locator == "" {
		locator = pkt.Identity.Locator
	}
	if locator == "" {
		locator = pkt.Identity.SourceRef
	}
	ev := contextpacket.EvidenceItem{
		NodeID:      node.NodeID,
		SourceID:    pkt.Identity.SourceID,
		Locator:     locator,
		ContentHash: strings.ToLower(contentHash),
		Excerpt:     excerpt,
		Reference:   pkt.Content.Ref,
		MetadataRef: node.MetadataRef,
	}
	// Bound MaxEvidence=5: single evidence suffices for determinism; additional evidence
	// would be neighboring nodes but we keep 1 for minimal bounded slice.
	// Validate will ensure MaxEvidence.
	out := []contextpacket.EvidenceItem{ev}
	// Already sorted (single).
	return out
}

func buildActorResult(act *actorstub.Activation, pkt contextpacket.ContextPacket, sn scoring.ScoredNode) actorstub.ActorResult {
	// EvidenceRefs traceable to source/node.
	refs := make([]string, 0, len(pkt.Evidence))
	for _, ev := range pkt.Evidence {
		refs = append(refs, ev.NodeID)
		refs = append(refs, "evidence://"+ev.SourceID)
	}
	if len(refs) == 0 {
		refs = []string{"evidence://" + act.ID}
	}
	// Observations: bounded excerpts joined.
	var obsParts []string
	for _, ev := range pkt.Evidence {
		if ev.Excerpt != "" {
			obsParts = append(obsParts, ev.Excerpt)
		}
	}
	observations := strings.Join(obsParts, "\n---\n")
	if observations == "" {
		observations = pkt.Query
	}
	// Confidence telemetry: activationScore /12 bounded 0..1 (max raw 12)
	conf := sn.Components.ActivationScore / 12.0
	if conf < 0 {
		conf = 0
	}
	if conf > 1 {
		conf = 1
	}
	// Keep distinct from dormantActor default 0.42; use computed but ensure not bypass.
	return actorstub.ActorResult{
		ActorID:         act.ID,
		LineageID:       act.LineageID,
		NodeID:          pkt.TargetNodeID,
		Status:          "observation",
		Observations:    observations,
		Result:          "processed:" + pkt.Query,
		Confidence:      conf,
		EvidenceRefs:    refs,
		ProposedActions: []string{"no_op"},
		CreatedAt:       time.Now().UTC(),
	}
}

func countActivationStatuses(activations []*actorstub.Activation, dedupRejected int) (active, completed, rejected int) {
	for _, a := range activations {
		switch {
		case a.Status == "active":
			active++
		case a.Status == "completed" || a.Status == "observation":
			completed++
		case strings.HasPrefix(a.Status, "rejected_"):
			rejected++
		default:
			// Treat other terminal as rejected for observability.
			if strings.HasPrefix(a.Status, "rejected") {
				rejected++
			}
		}
	}
	// Add nil dedup rejections that had no activation object.
	rejected += dedupRejected - countRejectedInList(activations)
	if rejected < 0 {
		rejected = dedupRejected
	}
	// After RecordResult+Complete, actors become completed, so adjust if we already completed.
	// For Query path we called Complete immediately, so statuses in activations may still be active at capture time;
	// count completed from results? Keep as above.
	return
}

func countRejectedInList(list []*actorstub.Activation) int {
	c := 0
	for _, a := range list {
		if strings.HasPrefix(a.Status, "rejected_") {
			c++
		}
	}
	return c
}

func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func cloneRaw(b json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), b...) }

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	// Use same canonical as transition/authority but minimal: decode and re-encode sorted.
	// For RawMessage validation we just ensure it is valid JSON.
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	// Re-marshal to ensure valid.
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
