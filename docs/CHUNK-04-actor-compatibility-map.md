# CHUNK-04 Actor Compatibility Map — HEAD 76c7a509

## 1. Current Contract (internal/actorstub/actor.go)

### Package: `flatten-workspace/internal/actorstub`
- **Types**
  - `Activation { ID, WorkspaceID, TriggerAction, Path, LineageID, Depth, Budget, Status, CreatedAt, ExpiresAt, Notes }`
  - `Controller { maxActive int, ttl time.Duration, activations []*Activation, dedup map[string]time.Time, counter int, mu sync.Mutex }`
- **Constructor**: `New(maxActive int, ttl time.Duration) *Controller` — defaults 16 / 5m if <=0
- **Methods**
  - `Activate(workspaceID, action, path string) *Activation`
    - Lazy expiry via `clean(now)` — only on Activate/List, not deterministic stop
    - Dedup key = `workspaceID\x00action\x00path`, window = ttl, returns nil if dedup hit
    - Counts `active` statuses, rejects with `Status=rejected_budget` if >=maxActive
    - Fabricates `ID=actor-%06d`, `LineageID=lineage-%06d`, `Depth=1`, `Budget=1` placeholder — **no propagation**
    - ExpiresAt = now+ttl, Status=active else rejected_budget
  - `List(workspaceID string) []*Activation` — filters by workspaceID if non-empty, calls clean
  - `clean(now)` — marks active+expired → expired
- **Tests** (`actor_test.go`): dedup suppression, maxActive bound (1 active, second rejected)
- **Concurrency**: mutex guards all state, no data race in stub; but no PID/lifecycle
- **Gaps acknowledged** (per system_notes / CHUNK-04_STATUS PARTIAL):
  - No ActorSystem, Props, PID, message receive, lifecycle start/stop/passivation
  - No dormant descriptor (metadata-only dormant state)
  - No governed activation through Go authority
  - No lineage/depth/budget propagation (placeholder)
  - No supervision, no deterministic stop (lazy only), no failure isolation
  - Budget/depth hard limits not enforced beyond count

### Server Integration (`internal/server/actors.go` + `server.go`)
- `Server.Actors *actorstub.Controller` nullable
- `maybeActivateActor(workspaceID, action, details)` — called from `recordTransition` AFTER authority admission + receipt save
  - Guard: triggers only if `strings.HasPrefix(action, "build_ledger.")` or `action=="quarantine.blob.admitted"`
  - Extracts `path` from details["path"] or details["target_path"]
  - Calls `s.Actors.Activate(...)` ignoring return except discarded `_`
- Endpoints:
  - `GET /api/workspaces/{id}/actors` → `jsonWorkspaceActors` → `List(ws.ID)`
  - `GET /ui/workspaces/{id}/actors` → `uiWorkspaceActors`
  - `POST /api/workspaces/...` materialize etc via `recordTransition` → maybeActivate
- `server.New()`:
  - Reads env `ACTOR_MAX_COUNT` (int, default 16), `ACTOR_TTL` (duration, default 5m)
  - `actorstub.New(actorMax, actorTTL)` stored in `Actors`
  - No `ACTOR_MAX_DEPTH`, `ACTOR_MAX_BUDGET`, dedup window, lineage, supervision config
  - No shutdown hook for actors

### Config/Env
- Existing: `ACTOR_MAX_COUNT`, `ACTOR_TTL`, `DATA_DIR`, `RUST_LEDGER_URL`, `AUTHORITY_TOKEN`
- Missing: `ACTOR_MAX_DEPTH`, `ACTOR_MAX_BUDGET`, `ACTOR_DEDUP_TTL` (reuse TTL), `ACTOR_SUPERVISION` etc. Must reuse existing pattern `strconv.Atoi` / `time.ParseDuration` with defaults.

### Authority/Eventlog/Replay Interactions
- Authority (`internal/transition`): `Authority.Propose` is Go admission gate, followed by `eventlog.Service.Append` requiring Rust ACK; projection derived only after durable ACK. Actor stub currently runs AFTER admission, not before — no proposal path. No eventlog interaction from actorstub.
- Eventlog (`internal/eventlog`): hash-chained JSONL `ledger.jsonl`, `RustAcknowledgement` required for `transition.authority.accepted`, `ValidateRustAcknowledgement`, `ValidateHistory`. Actors must not bypass.
- Replay (`internal/replay`, `internal/transition/rebuild.go`): `NewFromEventLog`, `Rebuild`, `VerifyCheckpoint` — actors are ephemeral, not replayed.
- Projection (`internal/projection`): `BuildLedgerFromWorkspace` — actors should trigger transition proposals that update projection via authority, not direct mutation.

## 2. Behaviors to Preserve (Acceptance Compatibility)

| Behavior | Current | Required after CHUNK-04 | Migration Strategy |
|---|---|---|---|
| Dedup | key=(ws,action,path), window=ttl, return nil | Same key, bounded window, suppress duplicate live actors, reject/suppress recursive self-reactivation | Keep `Activate` facade, add dedup map with expiry, make deterministic via stop; add window config if needed |
| Max active count | count active, reject_budget if >=16 | Bound total active actors, new config `max_active` (reuse `ACTOR_MAX_COUNT`), hard limit | Keep counting active, add eviction/stop logic, add `ACTOR_MAX_COUNT` reuse |
| TTL | ExpiresAt=now+ttl, lazy expiry on next List/Activate | Deterministic stop/passivation on expiry via timer + Proto.Actor lifecycle, not just lazy | Retain ExpiresAt/TTL fields, add timer Stop, status expired/passivated |
| Activation listing | `List(wsID) []*Activation` JSON | Same signature, same JSON shape extended with new fields optional, filter same | Retain `Activation` as alias to extended descriptor; add fields additive |
| Server API compat | `GET /api/workspaces/{id}/actors`, `maybeActivateActor` after admission | Preserve endpoints, preserve post-admission trigger, add governed activation request → Go validation → materialization | Facade/adapter: `actorstub.Controller` keeps Activate/List methods, delegates to inner `Runtime` |
| Producer/Consumer | No lineage/budget propagation | Add lineage/depth/budget propagation | Extend fields, keep old defaults for non-lineage roots |

## 3. New CHUNK-04 Requirements to Add (Not Breaking Compat)

- Dormant descriptor metadata: actor_id, node_id/subject_id, lineage_id, parent_actor_id optional, depth, remaining_budget, activation_count, created_at, last_activated_at, ttl, status, descriptor/content identity
- Governed activation: request -> Go validation -> lineage/depth/budget/dedup checks -> ACCEPT|REJECT -> Proto.Actor materialization -> bounded processing -> result/proposal -> stop/passivate
- Hard limits: lineage depth, activation budget, duplicate, cycle/repeated subject, total active, TTL count — defaults `max_depth=8`, `max_budget=32`, `max_active=16`, `ttl=5m` (env: ACTOR_MAX_DEPTH, ACTOR_MAX_BUDGET)
- Lineage propagation: child.lineage==parent.lineage, child.depth==parent.depth+1, child.remaining_budget<parent.remaining_budget; reject depth>max_depth, exhausted budget, invalid lineage
- Cycle control: same activation identity within lineage/window no duplicate live actors
- Supervision: panic/failure not crash runtime, observable failure, bounded retry only if justified, prefer stop/fail-closed
- Deterministic lifecycle: completed→stop, TTL expired→stop, rejected/failed→stop, shutdown→terminate cleanly via Proto.Actor lifecycle
- Typed result: actor_id, lineage_id, source/node identity, status, observations/result, confidence telemetry, evidence refs, proposed next action(s), created_at
- Authority invariant: actors observe/evaluate/emit proposals but NOT directly mutate state, bypass Go admission, append durable directly, spawn unbounded trees; result -> Go transition -> Rust ACK -> projection
- 20-case matrix + go test -race + verification ladder

## 4. Files Touched (Anticipated)

- `go.mod`, `go.sum` — add `github.com/asynkron/protoactor-go`
- `internal/actorstub/actor.go` — rewrite to add ActorSystem, Props, PID, descriptor, governed activation, lineage, supervision, deterministic stop
- `internal/actorstub/runtime.go` (new) or expanded actor.go — ActorSystem holder, Props factory, message types, result types
- `internal/actorstub/config.go` (new) — env parsing for maxDepth/budget/active/ttl
- `internal/server/server.go` — wire new Controller with ActorSystem, read new env vars, add Shutdown hook, preserve New() signature
- `internal/server/actors.go` — keep maybeActivateActor facade, add ActivateWithGovernance variant internally if needed
- Tests: `internal/actorstub/actor_test.go` expand, new `chunk04_test.go` for 20 cases

## 5. Non-Goals / Out of Scope

- Proto.Actor remoting/distribution, Kotlin/.NET, CHUNK-07 scoring, PureGo/Zero, UI/brand, plugin runtime, remote cluster, consensus — must not be enabled.

## 6. Verification Checklist Before Edit

- [x] Current stub mapped
- [ ] New runtime keeps Activate/List JSON compatible (additive fields)
- [ ] maybeActivateActor call sites unchanged
- [ ] env pattern reused
- [ ] Authority reuse: transition.Authority + eventlog Rust ACK preserved
