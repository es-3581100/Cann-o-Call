# Expected RC Artifacts

`build-reproducible.sh` / `package-rc.sh` are designed to emit an external release directory similar to:

```text
Cann-o-Call-<version>/
  cann-o-call
  msl-ledger-sidecar
  BUILDINFO.json
  RELEASE_NOTES.md
  LICENSE
  LICENSING.md
  NOTICE

Cann-o-Call-<version>-linux-amd64.tar.gz
Cann-o-Call-<version>-source.tar.gz
SHA256SUMS
```

The build step independently builds the Go and Rust binaries twice and compares SHA-256 digests before accepting either artifact as reproducible.

`BUILDINFO.json` records the source SHA, commit timestamp, Go version, Rust/Cargo versions, platform, and the reproducibility comparison results. Provenance lives outside the binary so deterministic build flags can avoid embedding environment-specific paths.

For an internal/private RC, the packaging script may include the review-only license draft under `legal-review/` rather than misrepresenting it as an adopted `LICENSE`.
