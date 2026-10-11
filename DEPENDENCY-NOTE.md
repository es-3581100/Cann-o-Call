# Dependency note

`go.mod` and `go.sum` are the authoritative Go dependency manifests for this
repository. The direct runtime dependencies currently include Proto.Actor and
`gopkg.in/yaml.v3`.

For a fresh checkout, use the normal Go module workflow:

```bash
go mod download
go mod tidy
```

No vendored or reconstruction-only dependency shim is part of the canonical
source tree.
