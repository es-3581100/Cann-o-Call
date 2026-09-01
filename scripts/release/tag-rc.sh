#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
require_clean_head
cd "$ROOT"

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "ERROR: tag already exists: $TAG" >&2
  exit 50
fi

msg="Cann-o-Call $TAG release candidate

Source: $EXPECTED_HEAD
Authority: Go admission / Rust durable ACK / Proto.Actor lifecycle
Packaging: reproducible-build verification required before publication"

if [[ "${SIGN_TAG:-0}" == 1 ]]; then
  git tag -s "$TAG" -m "$msg"
else
  git tag -a "$TAG" -m "$msg"
fi

printf 'tag_created=%s\n' "$TAG"
printf 'tag_target=%s\n' "$(git rev-list -n1 "$TAG")"
echo 'NOTE: tag created locally only; no push was performed.'
