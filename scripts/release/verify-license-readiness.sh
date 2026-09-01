#!/usr/bin/env bash
set -euo pipefail
ROOT="${ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$ROOT"

fail=0
for f in LICENSE LICENSING.md NOTICE CONTRIBUTING.md; do
  if [[ ! -s "$f" ]]; then
    echo "LICENSE_GATE: missing $f" >&2
    fail=1
  fi
done

require_text() {
  local file="$1" text="$2"
  if ! grep -Fq -- "$text" "$file"; then
    echo "LICENSE_GATE: $file missing required text: $text" >&2
    fail=1
  fi
}

if [[ -f LICENSE && -f LICENSING.md && -f NOTICE ]]; then
  require_text LICENSE 'Business Source License 1.1'
  require_text LICENSE 'Copyright 2026 Eric T. Sawtelle'
  require_text LICENSE 'Licensor: Eric T. Sawtelle'
  require_text LICENSE 'Licensed Work: Cann-o-Call 0.1.0-rc.2'
  require_text LICENSE 'Cann-o-Call Small-Business Production Use Grant'
  require_text LICENSE 'fewer than 50 full-time-equivalent'
  require_text LICENSE 'less than US $5,000,000 aggregate gross'
  require_text LICENSE 'Employee count and'
  require_text LICENSE 'all Affiliates'
  require_text LICENSE 'hosted SaaS offered to third parties'
  require_text LICENSE 'managed services'
  require_text LICENSE 'paid automation or agent operations for'
  require_text LICENSE 'white-label offerings'
  require_text LICENSE 'up to 30'
  require_text LICENSE 'may not materially expand the'
  require_text LICENSE 'Nonprofit organizations may qualify'
  require_text LICENSE 'Educational institutions may use'
  require_text LICENSE 'Change Date: 2030-09-01'
  require_text LICENSE 'Change License: Apache License, Version 2.0'
  require_text LICENSING.md 'https://github.com/es-3581100/Cann-o-Call'
  require_text NOTICE 'Cann-o-Call'
  require_text NOTICE 'Copyright 2026 Eric T. Sawtelle'
  require_text CONTRIBUTING.md 'External code contributions and'
  require_text docs/legal/LICENSE-DECISION.md '**Decision status:** `FINALIZED`'
fi

if grep -Ein '(will|shall|must).{0,80}(CLA|DCO|assign)|automatic copyright assignment' CONTRIBUTING.md >/dev/null; then
  echo 'LICENSE_GATE: CONTRIBUTING.md promises unapproved contributor terms' >&2
  fail=1
fi

if grep -En 'TODO|TBD|REVIEW_REQUIRED|SELECTED_PENDING_FINALIZATION|[Pp]laceholder|\[INSERT\]|FIXME' \
  LICENSE LICENSING.md NOTICE CONTRIBUTING.md docs/legal/LICENSE-DECISION.md >/dev/null; then
  echo 'LICENSE_GATE: current licensing documents contain unresolved markers' >&2
  fail=1
fi

if [[ $fail -ne 0 ]]; then
  echo 'license_readiness=FAIL' >&2
  exit 30
fi

echo 'license_readiness=PASS'
