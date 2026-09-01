# CHUNK-06 Capability / Executor Compatibility Map — HEAD 60845a75d2ea8c932df716f4671daef280a49e27

## 1. Current execution contracts before edits (Phase 1)

### CLI execution surfaces (internal/cli/cli.go, main.go)
- Binary `flatten-workspace` / `cann-o-call` symlink. main.go: if args[0] in {status,ingest,query,actor,ledger,replay,snapshot,task} → cli.Run() else server.
- CLI ops: status, ingest <path> [--workspace <id>], query <query> [--workspace <id>], actor list/inspect, ledger status/verify, replay verify, snapshot <wsID>, task status/list — all via HTTP `/api/*` with `--json` deterministic mode, error taxonomy `invalid_input`, `admission_rejected` etc. No capability/plugin commands yet. CLI is read-only observe + request ingest/query; does not execute arbitrary binaries.

### Server operation handlers (internal/server/*)
- `POST /api/workspaces/from-zip` → `opImportZip` → `workspace.FromZipBytes` → `recordTransition(workspace.imported_zip)` → `Authority.Propose` → Rust ACK → projection
- `POST /api/workspaces` (envelope) → legacy `createWorkspace` / `jsonCreateWorkspaceFromEnvelope`
- `POST /api/workspaces/{id}/materialize` → `opMaterialize` → `recordTransition(workspace.materialized)` → `materialize.WriteWorkspace(root)`
- `POST /api/workspaces/{id}/bind` → `opBindWorkspace` → `projectroot.Verify` → `recordTransition(workspace.binding.recorded)`
- `POST /api/workspaces/{id}/build-ledger/*` (state, events, runs, receipts, verification) → `opUpdateState/opAppendEvent/opCreateDocument` → `candidateUpsert/Append` → `recordTransition(build_ledger.*)` → `workspace.UpsertFile/AppendToFile`
- `POST /api/workspaces/{id}/quarantine/decisions|blobs/{id}/admit|reject` → `quarantine.Store` then `recordTransition(quarantine.*)`
- `GET /api/workspaces/{id}/file|tree|zip|envelope|snapshot|replay/verify|quarantine|actors` — read-only
- `POST /api/ingest` and `POST /api/query` → `orchestrator.Ingest/Query` → `ingest.ProposeIngest → Authority.Propose → graphStore.Apply` and `scorer.ScoreGraph → Select → actorstub.ActivateWithParent → contextpacket.Validate → RecordResult`
- `GET /api/tasks`, `GET /api/tasks/{id}`, `GET /api/status`, `GET /api/ledger/status` — progress observability
- No plugin registry, no dynamic capability loading. All mutations go through `recordTransition` → `s.proposeTransition` → `Authority.Propose` → Rust ACK.

### Plugin-related code
- None. No plugin loader, no dynamic discovery, no `plugin.Open`, no `dlopen`. Search `grep -R plugin` empty. This is greenfield bounded registry.

### External process invocation
- Only `internal/eventlog/sidecar_integration_test.go` uses `exec.Command` to spawn `cargo run --manifest-path sidecar/Cargo.toml` on random port for tests; guarded by `syscall.Setpgid`, bounded timeout 10s HTTP client. No production `exec.Command`; No shell string `sh -c`. Must introduce allowlist-based executor if process capability added, per directive.

### Filesystem access
- `workspace.FromZipBytes`: reads ZIP bytes, `IsSafePath` rejects absolute/.. / null, quarantined entries deterministic, safe entries stored in-memory `Workspace.Files`.
- `workspace.UpsertFile/AppendToFile`: checks `IsSafePath`, calls `RecalcCounts`; no disk writes beyond in-memory.
- `materialize.WriteWorkspace`: only writer to host filesystem; checks `filepath.Abs`, `AllowAbsoluteRoot` guard, `strings.HasPrefix(absRoot, absBase+sep)`, `filepath.Rel` traversal check per file, `os.MkdirAll 0755`, `os.WriteFile 0644`, sorted paths deterministic.
- `eventlog.Service`: `os.MkdirAll(dir,0755)`, `os.OpenFile(ledger.jsonl, APPEND|CREATE|WRONLY,0644)`, `os.OpenFile(RDONLY)`, hash chaining.
- `receipts.Service`, `quarantine.Store`, `progress.Store`, `snapshot`: similar `os.MkdirAll`/`os.WriteFile` bounded to `DATA_DIR`.
- `ingest.Ingest*` helpers: read local files via `os.ReadAll` with size bound checked by parser, not unbounded.
- No arbitrary workdir, no symlink following beyond materialize's `filepath.Join`/`Rel` checks; symlink behavior not explicit yet.

### Archive/import/export helpers
- `workspace/zip.go`: `FromZipBytes`, `IsSafePath`, `validateEntryName`, `sha256Hex`, `LanguageFromPath`, `MediaTypeFromPath`.
- `workspace/parser.go`: `Parse`, `FromEnvelope` (yaml).
- `materialize/materialize.go`: `WriteWorkspace` deterministic.
- `snapshot/snapshot.go`: snapshot read/write via `os` to `data/snapshots`.
- `replay/replay.go`: `Verify` hash chain vs eventlog.
- `import.go`, `ledger.go`, `report.go`, `diff.go`: wrap authority/replay.

### Existing PureGo/native experiments
- None. No `import "github.com/ebitengine/purego"`, no `cgo`, no `plugin`, no `unsafe` dlopen patterns. `grep -R purego|dlopen|cgo` empty. Greenfield membrane.

### Actor proposed-actions/result contract (internal/actorstub/actor.go)
- `ActorResult{ActorID,LineageID,NodeID,Status,Observations,Result,Confidence float64,EvidenceRefs,ProposedActions,CreatedAt}` — Confidence telemetry only, never authorizes.
- Produced in `dormantActor.Receive` after bounded `ProcessMessage{Payload,From}` handling; deterministic stop/passivation.
- `Controller.ActivateWithParent` enforces `maxDepth=8,maxBudget=32,maxActive=16,cycle detection,dedup`, propagates `LineageID,Depth,RemainingBudget,ActivationCount`.
- No direct transition mutation; actors emit results → server/orchestrator decides admission. ProposedActions are `[]string{"no_op"}` bounded.
- Must preserve: capability request must not replenish budget/create new lineage, must carry actor lineage.

### Progress/receipt integration (internal/progress/*, internal/receipts/*, internal/eventlog/*)
- `progress.Registry` bounded 128, mutex, `Create/Transition/Update`, `MarkAbandoned` on restart, phases `pending|running|terminal`, statuses `pending|running|complete|failed|rejected`.
- `progress.Task{Packet:ProgressPacket{TaskID,Operation,Phase,Status,StartedAt,UpdatedAt,Completed,Total,Warnings[10],Errors[10],Actor,Graph,Context,Ledger}}`
- `receipts.Receipt{ID,WorkspaceID,Action,Status,AuthoritySource,EventID,FilesChanged,Details}` durably written to `data/receipts` after `Authority.Propose` ACK; receipts bound to `DurableBinding{EventID,Sequence,EventHash,RustAck}`.
- `observability` collects `ActorMetrics, LedgerMetrics, GraphMetrics, ContextMetrics` via `progress.Registry` + `eventlog.Service` + `graph.Store` + `orchestrator.QueryInfo`.
- Current: progress is observability only, never authority; receipts require Rust ACK; `handleTaskReceipt` exposes `RustAcknowledgement` truthfully.

### Transition/eventlog authority (internal/transition/authority.go, internal/eventlog/eventlog.go)
- `transition.Authority{mu,writer DurableWriter,policy,state Projection,accepted map}`; `Propose` canonicalProposal → duplicate vs conflict → stale → policy → `apply` → `writer.Append` → `ValidateRustAcknowledgement` → advance projection. `Rebuild`/`Restore`/`Import` are proposal-only.
- `eventlog.Service{mu,path ledger.jsonl,lastHash genesis,seq,sidecarURL,client 10s}`; `Append` hash `PrevHash`, forward `POST sidecarURL/events`, require `RustAcknowledgement{id,seq,hash SHA256}` matching; fail-closed `ErrDurableRecorderUnavailable`.
- `sidecar/src/main.rs` Rust hash-chained ledger, checkpoints.
- Invariant: Go admission + Rust durable ACK required; executor success ≠ transition acceptance; fail-closed if Rust unavailable.

## 2. Classification

| Surface | Class | Notes |
|---|---|---|
| `GET /api/workspaces/{id}/file|tree|zip|envelope`, `GET /api/receipts/verify`, `GET /api/status`, `actor list/inspect`, `snapshot` read | **READ** (T1) | No mutation, no durable write; may observe canonical state only |
| `workspace.FromZipBytes` parse in-memory, `IsSafePath` validation, `orchestrator.Query` scoring/selection/contextpacket build | **READ** (T1) | Bounded deterministic transform; does not mutate canonical projection |
| `progress.Registry` updates, `observability` aggregation | **READ** (T1) | In-memory observability, not authority |
| `workspace.UpsertFile/AppendToFile` in-memory + `materialize.WriteWorkspace` under `DATA_DIR/materialized/WsID` | **BOUNDED_LOCAL** (T2) | Local filesystem side-effect under explicit authority, scoped to workspace root / data dir; does not bypass canonical admission |
| `opImportZip` local file count extraction, `candidateUpsert/Append` before admission | **BOUNDED_LOCAL** (T2) | Prepare in-memory patch; durable ACK required before `store.Add/ws.Files` mutate visibly |
| `Authority.Propose` → `eventlog.Append` (Rust ACK) → `projection apply` → `Receipts.Save` | **CANONICAL** (T3) | Only path that durably mutates canonical state; requires Go admission + Rust ACK |
| `actorstub.ActivateWithParent` lineage/depth/budget enforcement | **BOUNDED_LOCAL** (T2) | Local actor lifecycle, not canonical; bounded by Config |
| `exec.Command` in sidecar integration test | **BOUNDED_LOCAL** (T2) | Test-only, not production; explicit binary (`cargo`) not arbitrary shell |
| Arbitrary shell `sh -c <user>`, `dlopen/dlsym` on user path, dynamic plugin discovery from user dirs | **UNSUPPORTED** | Must remain rejected; violates allowlist/traversal/privilege invariants |
| PureGo/native execution if added | **PRIVILEGED** (T3) | Requires explicit allowlist, isolated package, context timeout, typed I/O, no authority over canonical state |

## 3. Gaps vs CHUNK-06 target

- No typed capability descriptor/registry/executor abstraction; mutations go via hardcoded `recordTransition` strings.
- No graded tier → admission mapping; tier must determine admission path not actor confidence.
- No `CapabilityRequest/Result` types with validation, unknown ID fail-closed, schema/timeouts/resource bounds.
- No bounded deterministic registry (duplicate/deterministic listing/disabled/unknown/bounded size).
- No reference executors (file metadata, hash, transform, workspace info) proving interface pure-Go.
- No native/PureGo membrane (isolated package, allowlist, context timeout, no arbitrary lib path).
- No process/filesystem bounded executors with path normalization/traversal/allowlist/typed argv.
- No actor → capability request → Go admission flow preserving lineage/budget/cycle.
- No mutating vs read-only path separation (CapabilityResult → ProposedTransition vs receipt only).
- No timeout/input/output/registry bounds, no normalized error taxonomy bridge to progress.
- No progress/receipt wiring for capability tasks.

## 4. Preservation & compatibility

- Keep `flatten-workspace` module, `go 1.25`, `protoactor-go`, `AppAuthority`(Go) + `DurableWriter`(Rust) + `Proto.Actor` + `Context graph rebuildable` + `Progress/receipts observability only`.
- Keep all existing `internal/server` endpoints additive; do not alter `recordTransition`/`proposeTransition`/`materialize.WriteWorkspace` contracts.
- Registry static/bounded registration via `init`/explicit `Register`, no runtime filesystem scan.
- Capabilities execute work; they do NOT decide canonical change — `Executor` interface has no `Propose` authority, policy above executors.
- Determinism via `sort.Strings` on registry listing, descriptor JSON canonical via sorted keys, `canonicalJSON` reuse.
- Security: external output is untrusted data; do not interpret as authorization; output cannot change tier/register capabilities/increase budget/bypass Go/bypass Rust/claim ACK/modify progress authority.

## 5. New surface after CHUNK-06 (planned)

- `internal/capability`: `Descriptor{ID,Name,Version,Kind,Tier,Risk,InputSchema,OutputSchema,Timeout,ResourceBounds{MaxInputBytes,MaxOutputBytes,MaxConcurrency},Mutating,Native,Enabled,Provenance}`, `Request{RequestID,CapabilityID,WorkspaceID,ActorID,LineageID,Inputs,EvidenceRefs,RequestedOperation,Timestamp,Version}`, `Result{RequestID,CapabilityID,Status,Outputs,EvidenceRefs,StartedAt,FinishedAt,Elapsed,Warnings,Errors,ProposedTransition}`, `ErrorCode` enum, `Executor` interface `Describe() Descriptor` + `Execute(ctx, Request) (Result,error)`, `Registry` bounded mutex map, `Admission{Authorize(Request,Descriptor,Actor) error}`, `Manager{Registry,Executors,Auditor,Progress}`.
- `internal/capability/native`: membrane package, allowlist `map[library]map[symbol]func`, explicit I/O structs, `context.WithTimeout`, no user-supplied library path.
- `internal/capability/executors`: reference pure-Go executors (`cap.file.metadata`, `cap.hash.bytes`, `cap.transform.upper`, `cap.workspace.info`) with filesystem scope `/data/workspaces/<wsID>` + traversal rejection.
- Server integration: `POST /api/capabilities/execute` read-only vs mutating split, capability task lifecycle via `progress.Registry`, receipt only where `Mutating=false`, `Authority.Propose` where `Mutating=true` + Rust ACK.
- Actor integration: `ActorCapabilityBridge.RequestFromResult(actorID, resultData) -> CapabilityRequest` preserving `LineageID/Depth/RemainingBudget`.
- Error taxonomy: `unknown_capability, capability_disabled, capability_denied, invalid_input, timeout, resource_limit, execution_failed, native_unavailable, admission_rejected, durable_recorder_unavailable, durable_append_failed`.
- Tests: 36-case matrix covering registry, validation, timeouts, bounds, traversal, allowlist, no shell, actor lineage, budget, read vs mutating, Rust ACK, progress truthfulness, concurrency, shutdown, restart, pipeline green.

