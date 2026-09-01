#!/usr/bin/env bash
set -euo pipefail
DIR="${1:?usage: verify-artifacts.sh <artifact-dir>}"
cd "$DIR"
sha256sum -c SHA256SUMS
python3 - <<'PY'
import json
from pathlib import Path
j=json.loads(Path('BUILDINFO.json').read_text())
assert j['artifacts']['cann-o-call']['reproducible_pair_match'] is True
assert j['artifacts']['msl-ledger-sidecar']['reproducible_pair_match'] is True
assert len(j['source_head']) == 40
print('buildinfo_verification=PASS')
PY
