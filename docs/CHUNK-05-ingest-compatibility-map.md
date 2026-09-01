# CHUNK-05 Ingest Compatibility Map — HEAD 7a41d2a32b9d54334c3c3702f9c6ab9d3ca82e65

## 1. Existing ingest contract (map before edits)

### Workspace ingest
- **Files**: `internal/workspace/{model,zip,parser,files,tree,quarantine_blob}.go`
- **Constants**: `FormatV1 = flatten-workspace/v1`, `ModeZipStructure = zip-structure`
- **Types**:
  - `Workspace{ID, Format, Mode, Source, Files map[string]*File, Tree, Directories, Issues, Quarantined, QuarantineDecisions, QuarantinedBlobs, Binding, CreatedAt}`
  - `Source{Name, SHA256, FileCount, DirectoryCount, ArchiveMemberCount, UnsafeEntryCount}`
  - `File{Path, Size, SHA256, DeclaredSHA256, Encoding, Kind, Language, MediaType, Verified, Data}`
  - `FileMeta{Content, Encoding, Kind, Language, MediaType, Path, SHA256, Size}`
  - `Envelope{Format, Mode, Source, Tree, Manifest, QuarantinedEntries}`
- **ZIP path** (`FromZipBytes`): sha256 archive → Source.SHA256 → Workspace.ID; iterates zip entries; `IsSafePath` rejects absolute/.. / null / non-normalized; unsafe entries quarantined: count++ + `Quarantined[]` + `QuarantinedBlobs{ID=DeterministicQuarantineID(wsSHA, rawName, hash), SafePath, Reason, Data}`; safe entries: read all, sha256Hex, encoding utf8/base64, `LanguageFromPath`, `MediaTypeFromPath`, `addParents`; dirs sorted; counts recalc; ID = source SHA.
- **Parser path** (`Parse`/`FromEnvelope`): yaml unmarshal Envelope → validate FormatV1/ModeZipStructure, Tree non-empty; walk Tree deterministically (map iter via range but child map iteration not sorted — note determinism gap); validate entry name, decode Content(base64/utf8), sha256 verify, `IsSafePath`, duplicate detection, fileCount/dirCount, manifest cross-check, Issues appended not hard failure; ID = Source.SHA256 or deriveID (hash sorted paths+hashes).
- **File helpers**: `UpsertFile`, `AppendToFile`, `RecalcCounts`, `addParents`, `LanguageFromPath`, `MediaTypeFromPath` (covers yaml/md/json/jsonl/go/rs/html/js/css→octet fallback).
- **Tree**: `TreeNodes()` builds sorted paths then nested builder dirs sorted, files sorted by Path → TreeNode{dir,file} deterministic.
- **Quarantine blob**: `DeterministicQuarantineID(wsSHA, originalPath, sha) = quarantine-<hex8 truncated sha256>`, `SafeQuarantinePath` → `quarantine/<id>-<base>` uniqueness vs Files map.
- **Safety**: `IsSafePath`, `validateEntryName`, size/sha mismatch → Verified=false + Issues, not abort; traversal `../evil.txt`, absolute `/etc/passwd`, malformed envelope → quarantined or error.

### Server ingest surfaces
- `internal/server/{server,import,quarantine,ledger,replay,snapshot,materialize,binding}.go`
- `POST /api/workspaces/from-zip` + `POST /api/workspaces` (envelope) → `opImportEnvelope` → `workspace.Parse` → `scanBuildLedgerSecrets` (policy.ScanBytes on build-ledger/*) → `recordTransition` (`workspace.imported_envelope`) → `recordBuildLedgerBaseline` → `store.Add` (visible only after durable admission)
- `recordTransition` → `transition.Authority.Propose` → `eventlog.Append` (requires Rust ACK) → receipt → `maybeActivateActor` (post-admission hook)
- `GET /api/workspaces/{id}/envelope`, `/tree`, `/file`, `/zip`, `/snapshot`, `/replay/verify`
- Binding/quarantine/build-ledger mutations all via `recordTransition` → authority.

### Durable authority (preserve)
- `internal/transition/authority.go` — `Authority{mu, writer DurableWriter, policy, state Projection, accepted map}`; `Propose` canonicalProposal → stale/prior check → policy → `apply` → `writer.Append` (Rust ACK required) → ValidateRustAcknowledgement → advance state.
- `internal/eventlog/eventlog.go` — `Append` hashes prevHash+payload, forwards to `RUST_LEDGER_URL/events` if configured, requires `RustAcknowledgement{id,seq,hash}` matching id/seq and sha256 hash; sidecar unavailable → `ErrDurableRecorderUnavailable`.
- `internal/transition/rebuild.go` — `NewFromEventLog`/`Rebuild`/`VerifyCheckpoint` deterministic via sorted Nodes (entity,node) and cloneProjection hash.
- `internal/projection`, `internal/replay`, `internal/snapshot` — all deterministic via `sort.Strings` on paths.
- `sidecar/` Rust ledger — hash-chained JSONL, `prev_hash`, sequence, checkpoints; `sidecar/src/main.rs`.
- **Invariant**: Go admission authority + Rust durable ACK → projection update; bypass = FAIL.

### Actor boundary (CHUNK-04)
- `internal/actorstub/actor.go` local Proto.Actor runtime, `Controller{ActorSystem, RootContext, live PIDs, descriptors}`.
- `internal/server/actors.go` `maybeActivateActor` triggers only on `build_ledger.` prefix or `quarantine.blob.admitted` AFTER admission.
- Ingest must NOT spuriously activate actors.

### Tests / determinism inventory
- `workspace/zip_test.go` safe/traversal cases.
- `eventlog/eventlog_test.go`, `transition/authority_test.go`, `snapshot/*`, `replay/*`, `projection/*`, `actorstub/*` all use sorted collections; one gap: `FromEnvelope` walks `env.Tree` via range without sorting → potential non-determinism (to be fixed via sorted walk in CHUNK-05 builder, not retro change tree parsing beyond compatibility).
- `gofmt`, `go vet`, `go test -race` must remain PASS; cargo paths must remain PASS.

## 2. Classification before edits

| Area | Status | Rationale |
|---|---|---|
| `workspace.FromZipBytes`, `IsSafePath`, `LanguageFromPath`, `MediaTypeFromPath`, `sha256Hex`, `QuarantinedBlob` logic | **preserve** | Proven, covers ZIP traversal, mime/lang, hashing; reuse for source identity |
| `workspace.Parse` / `FromEnvelope` envelope structure `flatten-workspace/v1` + `zip-structure` | **preserve/migrate** | Canonical import baseline; keep format/mode validation, Issues pattern, deriveID fallback; CHUNK-05 graph reads from Workspace.Files without altering this path |
| `Envelope.Tree/Manifest` projection, `TreeNodes` sorting | **preserve** | Manifest cross-check + sorted TreeNodes required for compatibility test 20 |
| `eventlog.Service`, `transition.Authority`, `sidecar` Rust ACK chain | **preserve** | CHUNK-02/03/04 invariants; CHUNK-05 ingest mutations must go through same Append → ACK → projection gate |
| `server.opImportEnvelope` / `recordTransition` / `store.Add` ordering | **preserve** | Visible-after-durable invariant |
| `actorstub.Controller` + `maybeActivateActor` gating | **preserve** | CHUNK-04 lifecycle; CHUNK-05 graph must not auto-wake actors unless explicit bounded hook documented |
| `projection.BuildFromFiles` / `replay.BuildLedgerProjectionFromEvents` | **preserve** | Deterministic baseline; new graph projection is parallel, not replacement |
| ZIP/file traversal ordering, Go map iteration | **migrate** | Ensure new ingest/graph builder uses canonical sorted ordering; fix FromEnvelope child iteration to sorted keys where touched |
| Large artifact embedding | **replace** | Current `File.Data` holds bytes; new SourcePacket/Graph node will hold ContentRef/MetadataRef + content hash, not embedded blobs for large artifacts |
| Source identity / metadata extraction / graph types | **replace/new** | Not present; define typed SourceIdentity, SourcePacket (content vs metadata), Adapter boundary, Graph Node/Edge, deterministic Builder, rebuild from history |
| Unsupported formats (PDF/DOCX/OCR) | **defer** | Adapter boundary defined, fallback `generic` adapter handles opaque bytes; do not pull heavy deps |

## 3. Compatibility map for CHUNK-05 slice

### To define (new, no conflict)
- `internal/ingest` package:
  - `SourceType` constants (`file`, `zip_archive`, `workspace_file`, `text`, `markdown`, `json`, `yaml`, `html`, `generic`)
  - `ExtractionVersion = "1"` + `ExtractionIdentity`
  - `SourceIdentity{SourceID, SourceType, SourceRef, SourceSHA256, WorkspaceID, ExtractionVersion, Locator, Span}`
  - `Metadata{SourceID, Author, CreatedAt, ModifiedAt, MIMEType, Language, Title, Properties map[string]string, RawHeaders map[string]string, DetectedKind}`
  - `Content{SourceID, ContentHash, Encoding, Text, NormalizedText, ContentRef, Size}`
  - `SourcePacket{Identity, Content, Metadata, Provenance}`
  - `Provenance{WorkspaceID, ExtractionID, ExtractionVersion, SourceLocator, DerivedEventID}`
  - Adapter interface: `Inspect(data []byte, locator string) (Metadata, error)` → metadata, `Extract(data []byte, locator string) (Content, error)` → content, `Normalize(c Content) (Content, error)` → canonical; registry with deterministic ordering.
  - Deterministic helpers: `ComputeSourceSHA256([]byte)→hex`, `ComputeContentHash([]byte)→hex`, `DeterministicSourceID(workspaceID, sourceType, sourceRef, sha, version)→hex`, `DeterministicNodeID(sourceID, contentHash, version)→hex`
  - Ingest entry: `IngestWorkspace(ws *workspace.Workspace) ([]SourcePacket, []string, error)` + `IngestBytes(workspaceID, sourceRef, sourceType string, data []byte) (*SourcePacket, error)` with quarantine handling for unsafe paths, malformed envelopes, oversized inputs (bounded MaxBytes 8MiB default handled by caller), untrusted content preserved as data not instructions.
  - Duplicate/change semantics: same bytes → same SourceID/ContentHash (idempotent), same path changed bytes → new ContentHash+SourceID/version, same bytes different path → distinct SourceID (path is part of identity), explicitly tested.

- `internal/graph` (or `internal/contextgraph`) package:
  - `NodeType` constants (`source`, `file`, `directory`, `chunk`, `metadata`)
  - `Node{NodeID, SourceID, SourceSHA256, SourceLocator, NodeType, ContentID, ContentHash, ContentRef, MetadataRef, ParentID, Provenance, CreatedEventID, DerivedFromID}`
  - `EdgeType` constants (`contains`, `derived_from`, `source`, `parent`, `structural`, `hyperlink`, `reference`, `semantic`)
  - `Edge{EdgeID, FromID, ToID, EdgeType, Provenance, CreatedEventID}`
  - `Graph{Nodes map[string]Node, Edges []Edge, WorkspaceID, Version, Hash}` + deterministic `HashGraph` via canonicalJSON sorted keys.
  - Builder: `Build(packets []SourcePacket) (*Graph, error)` → sorts packets by SourceID, creates source node + content node + metadata node + directory parent nodes + contains/parent/derived_from/source edges; stable ordering via `sort.Strings`.
  - Rebuild: `RebuildFromPackets` same as Build (idempotent); `RebuildFromEvents(events []Event) (*Graph, error)` if events embed accepted ingest payloads; discarding projection and rebuilding yields same Hash.
  - Invariant: edges never imply semantic truth merely from coexistence; type explicit.

### Authority binding (must preserve)
- Every canonical ingest/graph mutation goes via `transition.Authority.Propose` with operation e.g. `context_graph.ingested` or `source.ingested` (chosen one, documented). Propose → eventlog.Append → Rust ACK → apply → graph projection update. Failures leave projection unchanged. Rust unavailable → fail-closed (`Durable` reject class).
- Graph creation itself → dormant descriptors only, not live actor spawn; `maybeActivateActor` trigger set limited to `build_ledger.*` and `quarantine.blob.admitted`; new `context_graph.` operations must NOT be in that allowlist unless documented bounded hook.

### Adapter boundary
- Inspect → metadata (author/timestamps/MIME/OG/properties) without touching content relevance.
- Extract → content (semantic text/structure) without metadata keywords.
- Normalize → canonical (trim, utf8 validate, html strip where applicable, json/yaml canonicalization).
- Keep separation visible in typed structs; do not inject metadata into content keywords by default.

### Determinism guarantees
- Input: same bytes + extraction version/config + workspace identity → same SourceIdentity/SourceID/ContentHash/NodeIDs/Edges/Hash.
- No `path/filepath.Walk` traversal order leakage; sort before hashing/ID generation.
- No Go map iteration in hash inputs; canonicalJSON with sorted keys.

### Workspace/ZIP handling preservation
- Reuse `IsSafePath`, `sha256Hex`, quarantine path protections; unsafe workspace entries remain quarantined per CHUNK-03, not ingested as graph nodes except as quarantined descriptors if exposed.

### Test matrix bridging (20 cases)
See Section 4 for mapping to new package behaviors.

## 4. 20-case test mapping (all must pass)
1. empty workspace → 0 packets, 0 nodes, empty graph hash `genesis`
2. single text source → 1 packet, nodes ≥1, content preserved
3. deterministic source hash → same bytes → same hex sha256
4. deterministic node identity → same inputs → same NodeID
5. deterministic graph rebuild → Build twice same inputs → same Hash
6. repeated ingest idempotency → re-Ingest same Workspace → same Graph, no dup nodes
7. changed bytes → new content hash / new SourceID version
8. path/provenance retained → SourceRef, locator, workspace identity in Node/Identity
9. metadata separated → Metadata struct ≠ Content.Text; HTML title not in content keywords injection
10. structural parent/child edge → directory contains file edge present
11. explicit edge types → edge.EdgeType ∈ {contains,derived_from,source,parent,structural,hyperlink,…}
12. stable ordering → Build after shuffling packet order → same sorted Nodes/Edges hash
13. ZIP traversal rejected/quarantined → `../evil` → quarantined, not ingested
14. malformed rejected → bad envelope/encoding → error with RejectClass malformed
15. unsupported handled → unknown extension → generic adapter, still ingested, not error
16. failure leaves projection unchanged → ingest error does not partial-update graph via authority
17. Rust unavailable fail-closed → no RUST_LEDGER_URL or sidecar down → Durable reject, no state advance
18. restart/replay reproduces graph → NewFromEventLog → same Graph Hash
19. graph creation does not spuriously activate actors → ingest does not increment Actor LiveCount
20. flatten-workspace/v1 compatibility → old `workspace.Parse` still round-trips via `ToEnvelope`/`Parse`

## 5. Files anticipated

| File | Action | Notes |
|---|---|---|
| `internal/ingest/*` | new | identity, packet, adapter, html/json/yaml adapters, service |
| `internal/graph/*` (or `internal/contextgraph/*`) | new | node, edge, graph, builder, rebuild, hash |
| `internal/server/ingest.go` | new | HTTP/API glue if needed, but must go through authority |
| `internal/transition` | preserve | no schema change; just new operation string |
| `docs/CHUNK-05-ingest-compatibility-map.md` | new (this file) | evidence of Phase-1 mapping |
