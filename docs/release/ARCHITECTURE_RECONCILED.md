# Cann-o-Call — Reconciled Release Architecture

Status: canonical release-candidate architecture.

## Authority boundaries

| Layer | Owns | Explicitly does not own |
|---|---|---|
| Go | transition admission, activation admission, capability admission, deterministic operational projections | durable accepted-event truth |
| Rust sidecar | durable accepted-event/evidence append, hash-chain verification, restart/replay integrity | policy/admission decisions |
| Proto.Actor | local bounded actor lifecycle, mailboxes, supervision, stop/passivation | canonical state authority |
| Context graph | deterministic operational/source projection | canonical persistence |
| Capability plane | bounded execution behind Go admission | authorization of canonical mutation |
| Progress/receipts/CLI | observability and operator projection | authority or competing ledger |
| Native membrane | allowlisted optional capability adapter | arbitrary dlopen/symbol execution or state authority |

## Canonical accepted mutation flow

```text
proposal
  → Go Authority.Propose
  → policy/stale/duplicate validation
  → Rust durable append + acknowledgement validation
  → Go projection advance
  → controlled external effect where applicable
  → truthful receipt
```

If Rust is unconfigured or unavailable, accepted mutation fails closed. No local canonical fallback is permitted.

## Actor flow

```text
source/context graph
  → deterministic 4/8/16 scoring
  → bounded selection
  → Go activation checks
  → Proto.Actor materialization
  → bounded ContextPacket
  → ActorResult
  → optional capability request / ProposedTransition
  → Go authority
```

Actor confidence is telemetry only.

## Scoring baseline

- primary: max 4 × 1.00
- secondary: max 8 × 0.50
- meta: max 16 × 0.25
- maximum raw score: 12

Metadata remains distinct from semantic relevance. Coverage/diversity limits duplicate amplification.

## Native/capability terminology

The RC contains a **bounded reference/native membrane** with pre-allowlisted operations. It does **not** contain unrestricted PureGo/dlopen, arbitrary library loading, arbitrary symbol execution, or arbitrary shell execution.

## Restart model

Rust accepted history and related durable receipts/materialized effects survive restart. Runtime actor descriptors, graph stores, orchestrators, and in-flight task state may be ephemeral/rebuildable according to their documented limits. Ephemeral projections are not competing truth.
