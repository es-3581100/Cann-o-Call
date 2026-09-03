# RC Tagging

## Intended release candidate

The current release candidate is:

```text
v0.1.0-rc.4
```

The provided `scripts/release/tag-rc.sh` creates an annotated local tag only. It never pushes.

Expected normal sequence:

```bash
EXPECTED_HEAD="$(git rev-parse HEAD)" TAG=v0.1.0-rc.4 scripts/release/verify-release-head.sh
EXPECTED_HEAD="$(git rev-parse HEAD)" TAG=v0.1.0-rc.4 scripts/release/tag-rc.sh

git show --no-patch --decorate v0.1.0-rc.4
```

The `EXPECTED_HEAD` value is required and must be the exact clean commit being
tagged. `VERSION` supplies the default version and `TAG` defaults to
`v$VERSION`; neither replaces the explicit reviewed commit gate.

For cryptographically signed tags, set `SIGN_TAG=1` only when the local signing configuration is already established and trusted.

## Historical checkpoints

`v0.1.0-rc.1` is a permanent historical technical/reproducibility checkpoint
and remains attached to `b1a8221888d7835043482a2c3310927e9b8c6f8c`. Do not
move, retag, or delete it.

`v0.1.0-rc.2` and `v0.1.0-rc.3` are also immutable historical release
candidate tags. Do not move, retag, or delete them.
