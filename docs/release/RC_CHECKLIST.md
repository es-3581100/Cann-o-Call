# Cann-o-Call v0.1.0-rc.1 — RC Checklist

## Source gate

- [ ] canonical root is `/home/sticky-ricky/repos/cann-o-call`
- [ ] HEAD is `544f066c4f5453419934347b04e1229c03582125` or an explicitly reviewed release-doc-only descendant
- [ ] working tree is clean
- [ ] `git fsck --no-dangling` passes
- [ ] no unexpected generated artifacts exist in the repository

## License gate

- [ ] license policy reviewed
- [ ] licensor/copyright holder filled
- [ ] release/version field filled
- [ ] Change Date filled
- [ ] Change License filled and reviewed for BSL compatibility
- [ ] commercial-license contact filled
- [ ] `LICENSE` contains no draft placeholders
- [ ] `LICENSING.md` exists
- [ ] `NOTICE` exists
- [ ] public release does not call the pre-Change-Date license OSI Open Source

## Build gate

- [ ] Go build A/B hashes match
- [ ] Rust sidecar build A/B hashes match
- [ ] source archive generated from exact HEAD
- [ ] artifact archive normalized by commit timestamp
- [ ] `SHA256SUMS` generated and verified
- [ ] `BUILDINFO.json` records toolchains and source commit

## Validation gate

- [ ] gofmt read-only check
- [ ] `go vet ./...`
- [ ] `go test -count=1 ./...`
- [ ] `go test -race -count=1 ./...`
- [ ] `go build ./...`
- [ ] `cargo fmt -- --check`
- [ ] `cargo check --locked`
- [ ] `cargo test --locked`
- [ ] isolated PID-owned smoke if release packaging changed runtime code

## Documentation/brand gate

- [ ] reconciled architecture docs present
- [ ] bounded native terminology used
- [ ] accepted limitations disclosed
- [ ] release notes present
- [ ] canonical brand reference retained
- [ ] logo/image provenance retained

## Tag gate

- [ ] package artifacts reviewed
- [ ] checksums reviewed
- [ ] tag points to reviewed clean commit
- [ ] annotated tag message names RC and source SHA
- [ ] tag created locally before any push
- [ ] human approval before pushing tag / publishing release
