#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
PROJECT_NAME="${PROJECT_NAME:-Cann-o-Call}"
VERSION="${VERSION:-$(tr -d '\n' < "$ROOT/VERSION")}"
TAG="${TAG:-v$VERSION}"
: "${EXPECTED_HEAD:?ERROR: set EXPECTED_HEAD to the exact clean commit being released}"

require_clean_head() {
  cd "$ROOT"
  local head
  head="$(git rev-parse HEAD)"
  [[ "$head" == "$EXPECTED_HEAD" ]] || {
    echo "ERROR: expected HEAD $EXPECTED_HEAD, found $head" >&2
    exit 20
  }
  [[ -z "$(git status --porcelain=v1)" ]] || {
    echo "ERROR: working tree is not clean" >&2
    git status --short >&2
    exit 21
  }
}

sha256_file() { sha256sum "$1" | awk '{print $1}'; }
