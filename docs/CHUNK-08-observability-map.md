# CHUNK-08 Observability Compatibility Map — HEAD ded76ab

## 1. Current observability before edits (Phase 1)

### CLI / Binary
- Binary: `flatten-workspace` only (go build -o bin/flatten-workspace .). No cann-o-call alias. main.go spawns HTTP server on ADDR=:8080.
- Makefile targets: tidy, run (go run .), build, test (go test ./...). No operator commands.
- Help: none beyond HTTP health.

### Server endpoints (internal/server/server.go)
- Health: GET /api/health
- Workspaces: POST /api/workspaces (envelope), POST /api/workspaces/from-zip, GET /api/workspaces, GET /api/workspaces/{id}, /tree, /file, /zip, /envelope, /snapshot, /replay/verify, /verification-report, /quarantine, /actors
- Receipts: GET /api/receipts, GET /api/receipts/verify, GET /api/receipts/{receiptID}/diff
- Ledger: GET /api/ledger/verify
- Checkpoints/Snapshots: POST /api/checkpoints, POST /api/snapshots/verify
- Materialize/Bind/Build-ledger: POST /api/workspaces/{id}/materialize|bind|build-ledger/*
- Quarantine: POST /api/workspaces/{id}/quarantine/*
- UI: GET /ui/*, POST /ui/*
- No unified /api/status, /api/tasks, /api/ledger/status, /api/query, /api/ingest for operator CLI.

### Eventlog / Durability (internal/eventlog)
- Service{mu, path ledger.jsonl, lastHash genesis, seq, sidecarURL RUST_LEDGER_URL, client 10s}
- Append: sha256 hash chain, forward POST sidecarURL/events, require RustAcknowledgement{id,seq,hash} for transition.authority.accepted. Fail-closed if Rust unavailable.
- Verify(): checks seq, prev_hash, hash, RustAck presence. List(): best-effort legacy. RustAcknowledgedEvents(): GET sidecar /events rebases seq/lastHash.
- No progress/task lifecycle; only durable events.

### Receipts (internal/receipts)
- Service dir/data/receipts, Receipt{id,seq,prev_hash,hash,EventID,FilesChanged}, Verify() chain vs legacy.
- List sorted CreatedAt desc. No per-task terminal receipt binding Rust ACK display beyond hash chain.

### Actor runtime (internal/actorstub)
- Activation{ID,WorkspaceID,TriggerAction,Path,NodeID,LineageID,ParentActorID,Depth,Budget,RemainingBudget,ActivationCount,CreatedAt,ExpiresAt,TTL,Status,Notes}
- Controller: ActorSystem+RootContext+live PIDs+timers+dedup+results, Config defaults 16/8/32/5m.
- Methods: Activate, ActivateWithParent, List(wsID), Descriptor, GetResult, LiveCount, IsLive, DescriptorCount. Metrics derived via counting statuses but no unified ActorMetrics type.
- maybeActivateActor only on build_ledger.* or quarantine.blob.admitted after admission.

### Orchestrator (internal/orchestrator)
- QueryResult{request_id,query,workspace_id,candidates_considered,selected,packets,activations,results,rejected_count,coverage}
- QueryInfo{request_id,query,candidates_considered,selected,packets,activations,rejected,active,completed,coverage}
- Methods: Ingest(packet)->ProposeIngest->Rust ACK->graphStore.Apply; Query->Score->Select->ActivateWithParent->ContextPacket->RecordResult+Complete->QueryResult.
- Last* observers: LastQueryInfo, LastCandidates, LastSelected, LastPackets, ListPackets, GetGraph.
- No task identity, no progress packet, no ledger/graph unified metrics.

### Graph (internal/graph)
- Graph{WorkspaceID,Version,Nodes,Edges,Hash}, Node/Edge types, Deterministic IDs, HashGraph via canonicalJSON.
- Store{graph, packets map, workspaceID, mu RWMutex} with Apply, RebuildFromAccepted, Graph(), Packets().
- No metrics projection exposed via HTTP; only via store accessors.

### Ingest (internal/ingest)
- SourceIdentity/Packet, IngestBytes, IngestWorkspace, IngestZipBytes, ParseEnvelopeBytes.
- No progress signals; single-shot.

### Transition Authority (internal/transition)
- Authority{mu,writer,policy,Projection,accepted}, Propose validates canonicalProposal, duplicate, stale, policy, apply, Append, ValidateRustAcknowledgement, durableBinding.
- Projection deterministic. No task wrapping.

### Logging / Env / Config
- Logging: log.Printf in main.go only. No structured progress.
- Env: ADDR, DATA_DIR, RUST_LEDGER_URL, AUTHORITY_TOKEN, ALLOW_ABSOLUTE_ROOT, ACTOR_MAX_COUNT, ACTOR_TTL, ACTOR_MAX_DEPTH, ACTOR_MAX_BUDGET.
- No progress rendering, no error taxonomy normalization beyond transition RejectClass.

### Existing progress signals (duplicate/ad-hoc)
- None unified. Each subsystem reports errors via own RejectClass/IngestError. No task lifecycle.

## 2. Gap vs CHUNK-08 target
- Missing typed bounded ProgressPacket/ReceiptPacket
- Missing stable task identity for long ops
- Missing unified operator CLI (status, ingest, query, actor list/inspect, ledger status/verify, replay, snapshot, task status)
- Missing deterministic JSON mode, renderer, error taxonomy mapping
- Missing observability metrics aggregation (actor/ledger/graph/context)
- Missing receipt/event binding validation (no fake ACK)
- Missing restart truthfulness (transient abandoned)
- Missing bounded warnings/errors, bounded registry cleanup, race safety

## 3. Compatibility preservation
- Keep bin/flatten-workspace; add bin/cann-o-call symlink/copy for alias.
- Keep all existing HTTP endpoints; add new /api/status, /api/tasks*, /api/ledger/status, /api/query, /api/ingest as additive.
- Keep receipts.Service distinct from new progress.Store; avoid breaking chain.
- Keep Authority/Proto.Actor/Rust boundaries: progress observes only.
- Keep deterministic hashing (sha256, canonicalJSON sorted keys, sorted nodes).
- Not building CHUNK-06 plugins, UI/brand, remoting, OTEL stack, Zero, distributed metrics.

## 4. New surface (after CHUNK-08)
- internal/progress: ProgressPacket, ReceiptPacket/DurableRef, Task, Registry (bounded 128, mutex, MarkAbandoned), RenderProgress/RenderReceipt, error taxonomy.
- internal/observability: ActorMetricsFromController, LedgerMetricsFromService, GraphMetricsFromStore, ContextMetricsFromOrchestrator, CollectSystemStatus.
- internal/server: Tasks *Registry, ProgressReceipts *Store, graphStores/orchestrators maps, handleStatus/LedgerStatus/OrchestratorStatus, handleListTasks/GetTask, handleIngest/Query, receipt binding.
- internal/cli: Run() dispatcher, status/ingest/query/actor/ledger/replay/snapshot/task handlers, --json deterministic, stderr diagnostics.
- main.go: CLI mode if args[0] matches command, else server; supports both binary names.
