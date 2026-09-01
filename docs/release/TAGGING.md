# RC Tagging

Recommended first RC tag:

```text
v0.1.0-rc.1
```

The provided `scripts/release/tag-rc.sh` creates an annotated local tag only. It never pushes.

Expected normal sequence:

```bash
EXPECTED_HEAD=544f066c4f5453419934347b04e1229c03582125 TAG=v0.1.0-rc.1 scripts/release/verify-release-head.sh
EXPECTED_HEAD=544f066c4f5453419934347b04e1229c03582125 TAG=v0.1.0-rc.1 scripts/release/tag-rc.sh

git show --no-patch --decorate v0.1.0-rc.1
```

If the release documentation itself is committed after `544f066c4f5453419934347b04e1229c03582125`, set `EXPECTED_HEAD` to that reviewed descendant before tagging. Do not tag a dirty working tree.

For cryptographically signed tags, set `SIGN_TAG=1` only when the local signing configuration is already established and trusted.
