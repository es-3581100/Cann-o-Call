# Cann-o-Call v0.1.0-rc.2 — Public Release Candidate Notes

**Release base:** `b1a8221888d7835043482a2c3310927e9b8c6f8c`
**Status:** public-license-ready release candidate, pending separate publication authorization.

> EVERY CALL. EVERY ACTOR. EVERY STATE. CONNECTED.

## Licensing

Cann-o-Call 0.1.0-rc.2 is source-available under the Business Source License
1.1 with the Cann-o-Call Small-Business Production Use Grant. The Change Date
is 2030-09-01 and the Change License is Apache License, Version 2.0. See
`LICENSE` for binding terms and `LICENSING.md` for a plain-English summary.

## Runtime and authority model

This release preserves the accepted authority model: Go governs transition,
state, activation, and capability admission; Rust is the canonical durable
accepted-event and evidence authority; Proto.Actor provides bounded local
lifecycle and mailbox runtime; the context graph is a deterministic rebuildable
projection; and capabilities, progress, receipts, and CLI are non-authority
operator projections. Unrestricted PureGo/dlopen remains not implemented.

## Release scope

RC.2 reconciles licensing and release packaging only. RC.1 remains an immutable
historical technical and reproducibility checkpoint.
