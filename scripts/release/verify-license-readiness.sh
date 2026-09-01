#!/usr/bin/env bash
set -euo pipefail
ROOT="${ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$ROOT"

fail=0
for f in LICENSE LICENSING.md NOTICE; do
  if [[ ! -s "$f" ]]; then
    echo "LICENSE_GATE: missing $f" >&2
    fail=1
  fi
done

if [[ -f LICENSE ]]; then
  if grep -En '\[(COPYRIGHT HOLDER|YEAR|VERSION|YYYY-MM-DD|SELECT|COMMERCIAL|PUBLIC CREATOR|LEGAL ENTITY)' LICENSE >/dev/null; then
    echo 'LICENSE_GATE: LICENSE still contains unresolved placeholders' >&2
    fail=1
  fi
fi

if [[ $fail -ne 0 ]]; then
  echo 'license_readiness=FAIL' >&2
  exit 30
fi

echo 'license_readiness=PASS'
