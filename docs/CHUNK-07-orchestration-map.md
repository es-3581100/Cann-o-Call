# CHUNK-07 Orchestration Compatibility Map — HEAD 754e3a2894aab1a5dcd3c44b2a7d3def6c665620

## 1. Current contracts before edits

### Ingest (`internal/ingest`)
- Types: `SourceIdentity`, `SourcePacket{Identity,Content,Metadata,Provenance}`, `Content{SourceID,ContentID,ContentHash,Text,NormalizedText,Ref}`, `Metadata{SourceID,Title,MIMEType,Language,DocumentProperties,RawProperties,OpenGraph}`
- Identity: `DeterministicSourceID(workspaceID,sourceType,sourceRef,sha,version)`, `DeterministicContentID(sourceID,contentHash)`, `ComputeSHA256`
- Adapters: registry `Markdown|HTML|JSON|YAML|Text|Generic`, `AdapterFor(locator,media)` deterministic priority, `Inspect/Extract/Normalize` separation (content vs metadata)
- Entry: `IngestBytes(workspaceID,sourceRef,hint,data,locator)→*SourcePacket` validates `IsSafePath`, `MaxBytes 8MiB`, splits inspect/extract/normalize, sets `Content.Ref` for >64KiB or binary, propagates `Verified` flag, enforces `Validate()` deterministic.
- `IngestWorkspace(ws *Workspace)→[]SourcePacket` sorted by `SourceID`, carries `quarantined` list, skips malformed per-file.
- `IngestZipBytes` / `ParseEnvelopeBytes` delegate to `workspace.FromZipBytes`/`Parse`.
- Transition binding: `ProposeIngest(authority,*packet)→AcceptedTransition` via `authority.Propose` with `transitionID=DeterministicSourceID(ws,"transition",sourceID,contentHash)`, `operation upsert, entity=workspaceID, node=sourceID`, `ResultData={packet}`.
- History helpers: `PacketFromAccepted`, `PacketsFromHistory` (skip non-ingest).

### Graph (`internal/graph`)
- Types: `Graph{WorkspaceID,Version,Nodes map[string]Node,Edges []Edge,Hash}`, `Node{NodeID,SourceID,SourceSHA256,SourceLocator,NodeType,ContentID,ContentHash,ContentRef,MetadataRef,ParentID,Provenance,CreatedEventID,DerivedFromID}`, `Edge{EdgeID,FromID,ToID,EdgeType,Provenance}`
- NodeTypes: source,file,directory,chunk,metadata; EdgeTypes: contains,derived_from,source,parent,structural,hyperlink,reference,semantic
- Deterministic IDs: `DeterministicNodeID(sourceID,sourceSHA,locator,nodeType,version)`, `DeterministicEdgeID(from,to,type,workspaceID)`, `HashGraph` via `canonicalJSON` sorted keys.
- Builder: `Build(packets []SourcePacket)→*Graph` sorts packets by SourceID, creates directory hierarchy nodes, source/file/metadata nodes per packet, edges (derived_from, reference, source, contains, parent, hyperlink if HTML href), dedups edges by EdgeID, sorts, hashes.
- Store: `Store{graph, packets map[SourceID]Packet, workspaceID, mu RWMutex}` with `Apply(pkt)→(Graph,bool,error)` idempotent dedup, rebuild from sorted packets; `RebuildFromAccepted([]AcceptedTransition)` via `PacketsFromHistory`; `Graph()`, `Packets()` copies; thread-safe.

### Actor (`internal/actorstub`)
- Types: `Activation{ID,WorkspaceID,TriggerAction,Path,NodeID,LineageID,ParentActorID,Depth,Budget,RemainingBudget,ActivationCount,CreatedAt,LastActivatedAt,ExpiresAt,TTL,Status,ContentID,Notes}`, `ActorResult{ActorID,LineageID,NodeID,Status,Observations,Result,Confidence,EvidenceRefs,ProposedActions,CreatedAt}`, `Config{MaxActive,MaxDepth,MaxBudget,TTL}` defaults 16/8/32/5m
- Controller: `New/WITHConfig → ActorSystem+RootContext+descriptors+dedup+live+timers+results` local protoactor only, no remoting.
- Activate flow: `cleanLocked(now)` expiry, global dedup key `ws\x00action\x00path` for roots, lineage-scoped dedup + cycle check `nodeID==parent.nodeID && action==parent.action && path==parent.path → rejected_cycle`, depth >maxDepth → rejected_depth, budget<0 → rejected_budget_exhausted, activeCount>=maxActive → rejected_budget, else active + `spawnLocked` with `OneForOneStrategy(3,10s,StopDirective)` + `AfterFunc(ttl, expireActor)`.
- Parent propagation: child.LineageID=parent.LineageID, Depth=parent.Depth+1, Remaining=parent.Remaining-1, lineage-scoped duplicate live check.
- Lifecycle: `spawnLocked` anon spawn, `expireActor`→expired+Stop+dedup delete, `Complete/Fail/RecordResult/GetResult/IsLive/LiveCount/Descriptor/DescriptorCount/List/Shutdown/handleStopped/recordFailure/Send` all `mu` guarded, `Shutdown` stops timers/pids + `system.Shutdown`.
- Actor: `dormantActor{id,controller}` Receive on `Started/Stopped/ProcessMessage/string` → bounded Emit `ActorResult` (telemetry Confidence 0.42) + `RecordResult` + `ctx.Stop` → passivation; panic isolated via supervision, never mutates canonical state.
- Server coupling: `maybeActivateActor` only on `build_ledger.*` or `quarantine.blob.admitted` after `recordTransition` admission; not wired to ingest/graph.

### Transition / Durability (`internal/transition`, `internal/eventlog`, `sidecar`)
- Authority: `Authority{mu,writer DurableWriter,policy,Projection,accepted map[transitionID]record}` `Propose` does `canonicalProposal` validation (IDs, operation in {upsert,restore,import}, canonicalJSON Result/Admission) → duplicate check (same ID same request→duplicate true else Conflict) → stale prior check (`Prior != current.Ref()`) → policy hook → `apply` (clone, upsert/replace Nodes sorted entity/node, Version++, Hash) → `writer.Append(event{ID=transitionID, Type=transition.authority.accepted, WorkspaceID=Entity, Action=transition.accepted, Details={accepted_transition:payload}})` → require Rust ack `ValidateRustAcknowledgement` → `durableBinding` → advance state.
- Rebuild: `NewFromEventLog(log,policy)`→`Rebuild(events,writer,policy)` validates history via `ValidateHistory`, replays `transition.authority.accepted` only, checks canonicalProposal, id binding, stale, result hash, admission match.
- Eventlog: `Service{mu,path,lastHash,seq,sidecarURL,client}` `New(dir,url)` loads ledger.jsonl hash chain, `Append` requires `RustAcknowledgement{id,seq,hash}` for accepted transitions if sidecarURL set, else fail-closed, computes `hashEvent` (Sha256 of clone without Hash/RustAck), `forward` POST `/events` expects `status recorded` and matching id/seq/hash, `ValidateRustAcknowledgement` enforces id/seq/hash sha256, `hashEvent`, `Verify`, `List`, `RustAcknowledgedEvents`.
- Sidecar Rust (`sidecar/src/main.rs`): axum, hash-chained `ledger.jsonl` with `prev_hash`, sequence, checkpoints, strict startup verification, `GENESIS_HASH genesis`.

### Server query/request paths
- No current live-context scoring endpoint. Workspace CRUD: `POST /api/workspaces` (envelope), `POST /api/workspaces/from-zip`, `GET /api/workspaces`, `GET /api/workspaces/{id}/tree|file|zip|envelope|snapshot|replay/verify`, `POST /api/workspaces/{id}/materialize|bind|build-ledger/*`, quarantine, snapshots, checkpoints, receipts. All mutations via `recordTransition→Authority.Propose→eventlog.Append→Rust ACK→projection`.
- Query-like surfaces: `/api/workspaces/{id}/tree` lists deterministic nodes, `/file` fetches raw content, but no `query→score→actor` path. New slice must add scoring/selection/orchestration without bypassing Go authority.

## 2. Orchestration map for CHUNK-07

### Pipeline
```
ingest source → IngestBytes→SourcePacket → ProposeIngest(Authority)→Rust ACK→Graph.Store.Apply → Graph nodes (dormant)
      |
      +--- query (request_id, query/intent, workspace_id) → BaselineScorer over Graph nodes (deterministic 4/8/16 + coverage, metadata separated)
            → Candidate selection (score→sort deterministic→threshold→max candidates→max actors, ties deterministic)
            → Go governed activation (Controller.ActivateWithParent, checks dedup/active/lineage/depth/budget/cycle/TTL) → Proto.Actor materialize
            → Bounded ContextPacket (request_id, query/intent, workspace_id, target actor/node, selected evidence {nodeID,sourceID,locator,contentHash,bounded excerpt/reference}, score components, lineage {lineage_id,parent_actor_id,depth,remaining_budget}, timestamp/version) → actor evaluates → ActorResult (actor_id,lineage_id,target/source,evidence refs,score inputs/components,observations,confidence telemetry,status,proposed transition/action,timestamp) traceable to source/node
            → no-mutation path: result returned, actor passivates (Complete), no canonical mutation unless event recorded
            → mutating path: ActorResult→ProposedTransition→Authority.Propose→Rust ACK→projection update (invalid→rejected no event, Rust unavailable fail-closed, retry idempotent, actors never call Rust directly)
```

### Inputs
- Graph inputs: `graph.Store` packets + `Build` nodes/edges, `Provenance`, content Text vs Metadata separation, content hashes/refs, structural parent directory.
- Query inputs: `request_id` (idempotent), `workspace_id`, `query` string / intent identity, scoring config (threshold, max candidates, max actors per request), remaining budget/depth from lineage.
- Scoring candidates: all `NodeTypeFile` (or file+chunk) nodes with source identity; not directories/metadata.

### Activation gate
- Scoring nominates; Go decides. High score NOT authorization. Flow: score→qualifies (≥threshold) → sort deterministic (score desc, nodeID asc) → cap maxCandidates → for each candidate, `Controller.ActivateWithParent(parent?,workspaceID,action="live_context.query",path=nodeID)` → respects CHUNK-04 dedup, active limit, lineage, depth, budget, cycle, TTL → Proto.Actor materializes only if ACCEPT.

### Actor message payload
- `ContextPacket` (typed, bounded) NOT whole workspace/graph. Evidence bounded: `MaxEvidence=5` per packet, `MaxExcerptBytes=2KiB`, references via nodeIDs/sourceIDs/content hashes, e.g. `evidence://sourceID`. Must be deterministic ordering.

### Result path
- Actor evaluates bounded evidence only, returns `ActorResult` with evidence traceable to `SourceIdentity/SourceID/NodeID`. Confidence telemetry only, never bypasses activation limits/transition authority/budget/depth/Rust durability/policy.
- Use/extend CHUNK-04 `ActorResult` (add lineage propagation, evidence refs, score components, proposed transition). Existing `RecordResult/Complete` already manages passivation.

### Mutation path
- Mutating: `ActorResult.ProposedActions` → `ProposedTransition{TransitionID,ProposalID,RequestID,Prior,Operation=upsert,Entity=workspaceID,Node=targetNodeID,ResultData,AdmissionData}` → `Authority.Propose` → `eventlog.Append` forward Rust → `ValidateRustAcknowledgement` → advance `Projection`. Invalid→RejectClass `Invalid/Stale/Policy/Malformed`, no durable event, projection unchanged. Rust unavailable→Durable rejection fail-closed. Retry idempotent via `TransitionID` duplicate detection. Actors never call Rust/eventlog directly.

### Boundaries preserved
- Go orchestration/admission, Proto.Actor bounded lifecycle, Rust durable, Graph rebuildable projection, Actors evaluate bounded evidence only. No actor direct mutation/append/self-authorize/recursive without Go bounds.
- Not begin CHUNK-06 PureGo/native, LLM embeddings, vector DB redesign, remoting/clustering, UI/brand, Zero, distributed graph, multi-workspace merge/fork.
- Not expand CHUNK-05 limitations beyond correctness (keep PDF/DOCX/OCR defer, >8MiB streaming defer).

### Configuration defaults (bounded)
- Scoring/selection: `threshold=1.0`, `maxCandidates=10`, `maxActorsPerRequest=3`, `maxEvidencePerPacket=5`, `maxExcerptBytes=2048`; already `Controller` defaults `MaxActive=16, MaxDepth=8, MaxBudget=32, TTL=5m`. No unbounded goroutine/actor creation.

## 3. Freshness
- Graph Store deterministic via sorted SourceID.
- Eventlog/Transition rebuild deterministic via canonicalJSON sorted keys, sorted Nodes, validated Rust ack.
- Actor dedup deterministic window, sorted activation list, deterministic lineage IDs.
- New scorer/selection must be deterministic: same graph + same request + same config → same ordering, raw scores, selected set, ContextPacket evidence.

## 4. Risks/limitations
- `workspace.Parse` Tree walk uses map range unsorted (minor determinism gap) but graph builder re-sorts; acceptable for this slice.
- Large blobs remain reference-only; scorer must handle bounded excerpts, not embed whole blob.
- In-memory dormant descriptors remain ephemeral (CHUNK-04 limitation); restart loses them but graph rebuild recovers.
