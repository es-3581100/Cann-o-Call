# Cann-o-Call v0.1.0-rc.1 — Release Candidate Notes

**Release-candidate source:** `544f066c4f5453419934347b04e1229c03582125`  
**Status:** verified system release candidate; packaging authorized.

> EVERY CALL. EVERY ACTOR. EVERY STATE. CONNECTED.

## What this candidate establishes

Cann-o-Call now has a verified local vertical system in which deterministic source ingestion produces a rebuildable context graph; bounded relevance scoring nominates actor work; Go governs activation, capabilities, and state transitions; Proto.Actor owns bounded local actor lifecycle/message execution; Rust durably records accepted events/evidence; and progress, receipts, CLI, and status APIs expose the system without becoming competing authority.

### Accepted subsystem checkpoints

- CHUNK-02 — durable ledger: PASS
- CHUNK-03 — Go transition authority + Rust durable acknowledgement: PASS
- CHUNK-04 — local Proto.Actor dormant runtime + governed activation: PASS
- CHUNK-05 — deterministic source ingestion + context graph: PASS
- CHUNK-06 — graded capability/executor plane + bounded native membrane: PASS
- CHUNK-07 — deterministic live context scoring + ingest-to-actor orchestration: PASS
- CHUNK-08 — progress, receipts, observability, operator CLI: PASS
- CHUNK-09 — final integration, authority audit, bounded repair, restart smoke: PASS

## Final authority model

```text
proposal / request
      ↓
Go validation + admission
      ↓
Rust durable acknowledgement
      ↓
Go operational projection / external effect
```

Proto.Actor, plugins/native capabilities, progress/receipts, CLI, graph state, and other projections cannot independently authorize canonical state mutation.

## Final integration repair

The final audit found and removed a stale materialization implementation. The sole production materialization flow is now:

```text
HTTP/UI handler
  → opMaterialize
  → deterministic planned materialization paths
  → Go transition authority
  → Rust durable ACK
  → WriteWorkspace
```

The committed repair also hardened deterministic parser ordering, excluded `bin/` build artifacts, and clarified bounded-native terminology.

## Validation at release-candidate source

The final independent acceptance reported PASS for:

- read-only `gofmt` check
- `go vet ./...`
- `go test -count=1 ./...`
- `go build ./...`
- full Go race suite
- `cargo fmt -- --check`
- `cargo check --locked`
- `cargo test --locked` (15 tests)
- `git fsck --no-dangling`
- isolated PID/listener-owned vertical smoke
- restart durability of accepted Rust history

## Accepted limitations

This RC intentionally does not claim unrestricted PureGo/dlopen, dynamic plugin marketplaces, actor remoting/clustering, PDF/DOCX/OCR expansion, graph snapshot files, large-source streaming above the existing bound, persistent in-flight task state, or a capability HTTP API.

The context graph/orchestrator is a rebuildable operational projection and is not itself durable authority.

## Licensing

The selected licensing direction is BSL 1.1 plus a small-business internal-production Additional Use Grant. The policy draft remains **review-only** until the licensor/copyright holder, Change Date, Change License, commercial contact, and legal review are completed. See `docs/legal/LICENSE-DECISION.md`.

A public release should not be published until an adopted `LICENSE`, `LICENSING.md`, and `NOTICE` pass the license-readiness gate.
