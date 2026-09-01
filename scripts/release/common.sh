#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-Cann-o-Call}"
VERSION="${VERSION:-0.1.0-rc.2}"
TAG="${TAG:-v0.1.0-rc.2}"
# Default to the immutable RC.1 packaging commit, the immediate pre-RC.2 base.
# RC.2 builds from the finalized legal commit must explicitly set EXPECTED_HEAD
# to that commit, preserving this checkpoint as the default preflight anchor.
EXPECTED_HEAD="${EXPECTED_HEAD:-b1a8221888d7835043482a2c3310927e9b8c6f8c}"
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
