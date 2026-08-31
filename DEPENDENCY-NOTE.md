# Dependency note

This reconstructed source preserves the transcript's declared dependency:

```text
gopkg.in/yaml.v3 v3.0.1
```

The reconstruction environment could not reach `proxy.golang.org`, so a separate
`validation-worktree`/`worktree` uses a clearly marked JSON-only offline shim only
to compile and exercise the Go implementation without changing this intended
source tree.

Before normal use, run `go mod tidy` in a network-enabled Go environment and
remove any reconstruction-only shim if copied from the validation tree.
