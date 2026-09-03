# Cann-o-Call v0.1.0-rc.4 — Release Candidate Notes

RC.4 advances the operator experience while preserving the established Go
admission, Rust durable-evidence, and bounded local actor lifecycle boundaries.

## Highlights

### Terminal-first Flatten Workspace Studio

- `studio --once` renders a read-only Monitor and Ranger snapshot.
- Interactive Studio supports `monitor`, `ranger`, `refresh`, `select <index>`,
  `help`, and `quit`.
- Browser `/ui` is a secondary HTMX projection of the same `GET /api/studio`
  ViewModel.

Ranger includes workspaces, files, source nodes, graph nodes, actors, events,
receipts, tasks, and evidence/provenance.

### CLI startup behavior

- Root `--help` / `-h` no longer starts the server.
- Studio-specific help is available without a server.
- Missing-server Studio failures direct users to start the documented Go server.

### Public release surface

RC.4 reconciles release/version identity, licensing presentation,
stranger-path installation and run instructions, explicit Studio URL and binary
paths, supported-platform/tooling scope, and intentional unattached-collector
behavior.

## Verification

- `make build`
- `go test ./...`
- `go vet ./...`
- Rust build, all-feature tests, and Clippy with `-D warnings`
- Fresh-clone CLI, Studio, `/api/studio`, and `/ui` smoke checks

## Known limitations

- Capability and scoring collectors remain explicitly unavailable when
  unattached.
- Actor, graph, orchestrator, and in-flight task state are local/rebuildable.
- No arbitrary shell execution, dynamic plugin discovery, actor remoting,
  clustering, LLM embedding, or vector-database runtime is provided.
