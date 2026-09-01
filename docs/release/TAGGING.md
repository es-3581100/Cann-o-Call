# RC Tagging

## Current release candidate

The current release candidate is:

```text
v0.1.0-rc.2
```

The provided `scripts/release/tag-rc.sh` creates an annotated local tag only. It never pushes.

Expected normal sequence:

```bash
EXPECTED_HEAD="$(git rev-parse HEAD)" TAG=v0.1.0-rc.2 scripts/release/verify-release-head.sh
EXPECTED_HEAD="$(git rev-parse HEAD)" TAG=v0.1.0-rc.2 scripts/release/tag-rc.sh

git show --no-patch --decorate v0.1.0-rc.2
```

The `EXPECTED_HEAD` override must be the exact clean legal/release commit being
tagged. The default in `common.sh` remains the immutable RC.1 packaging base
(`b1a8221888d7835043482a2c3310927e9b8c6f8c`) for preflight reference.

For cryptographically signed tags, set `SIGN_TAG=1` only when the local signing configuration is already established and trusted.

## Historical RC.1 checkpoint

`v0.1.0-rc.1` is a permanent historical technical/reproducibility checkpoint
and remains attached to `b1a8221888d7835043482a2c3310927e9b8c6f8c`. Do not
move, retag, or delete it.
