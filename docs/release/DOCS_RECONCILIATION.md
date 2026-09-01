# Documentation Reconciliation for RC

This file records terminology that should be treated as canonical from the RC boundary forward.

## Canonical terms

- **Go transition authority** — Go decides whether a proposed state transition is accepted.
- **Rust durable authority** — Rust makes accepted event/evidence history durable and verifies its integrity.
- **Proto.Actor lifecycle runtime** — actors execute messages/lifecycle only; actors are not state authority.
- **Rust-acknowledged mirror** — any Go JSONL accepted-event representation must not be described as a competing canonical ledger.
- **Bounded native membrane** — implemented reference membrane with explicit allowlist.
- **Unrestricted PureGo/dlopen** — NOT IMPLEMENTED in this RC.
- **Arbitrary shell bypass possible** — false.
- **Source-available** — correct licensing description before the BSL Change Date; do not market the BSL-covered release as OSI Open Source before conversion.

## Historical text that may remain for provenance

Early brainstorming may describe Rust as "system truth" or "canonical truth" in a broader sense. Preserve such documents as historical provenance, but do not use them to override the final split:

```text
Go   = accepted transition/state authority
Rust = durable accepted-event/evidence authority
```

Similarly, early PureGo discussion may imply a general plugin loader. The RC implements only a bounded, pre-allowlisted native capability membrane.

## UI / branding reconciliation

User-facing language should emphasize observable state, actors, messages, runtime, evidence, and explicit coordination rather than presenting Cann-o-Call as a generic AI assistant.

Use functional status colors consistently: cyan/green for healthy/active/complete, yellow for waiting/caution, red for failure/rejection.
