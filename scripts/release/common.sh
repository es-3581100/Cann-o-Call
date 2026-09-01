#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-Cann-o-Call}"
VERSION="${VERSION:-0.1.0-rc.1}"
TAG="${TAG:-v0.1.0-rc.1}"
EXPECTED_HEAD="${EXPECTED_HEAD:-544f066c4f5453419934347b04e1229c03582125}"
ROOT="${ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

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
