#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

cd "$ROOT"
printf 'canonical_root=%s\n' "$(readlink -f .)"
printf 'head=%s\n' "$(git rev-parse HEAD)"
printf 'branch=%s\n' "$(git symbolic-ref -q --short HEAD || true)"
require_clean_head

test -z "$(gofmt -l .)" || { echo 'ERROR: gofmt changes required' >&2; exit 22; }
git diff --check
git fsck --no-dangling
printf 'release_head_verified=true\n'
