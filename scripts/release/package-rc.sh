#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
require_clean_head
cd "$ROOT"

RELEASE_VISIBILITY="${RELEASE_VISIBILITY:-internal}"
BUILD_DIR="${BUILD_DIR:-$HOME/cann-o-call-release/$VERSION}"
PACKAGE_DIR="${PACKAGE_DIR:-$HOME/cann-o-call-release/packages}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}"
mkdir -p "$PACKAGE_DIR"

if [[ ! -s "$BUILD_DIR/SHA256SUMS" ]]; then
  OUT_DIR="$BUILD_DIR" RELEASE_VISIBILITY="$RELEASE_VISIBILITY" "$SCRIPT_DIR/build-reproducible.sh"
fi
"$SCRIPT_DIR/verify-artifacts.sh" "$BUILD_DIR"

stage="$(mktemp -d "${TMPDIR:-/tmp}/cann-o-call-stage.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
name="Cann-o-Call-$VERSION-linux-$(go env GOARCH)"
mkdir -p "$stage/$name"
cp "$BUILD_DIR/cann-o-call" "$stage/$name/"
cp "$BUILD_DIR/msl-ledger-sidecar" "$stage/$name/"
cp "$BUILD_DIR/BUILDINFO.json" "$stage/$name/"
cp "$BUILD_DIR/SHA256SUMS" "$stage/$name/ARTIFACT-SHA256SUMS"

if [[ -f docs/release/RELEASE_NOTES_v0.1.0-rc.1.md ]]; then
  cp docs/release/RELEASE_NOTES_v0.1.0-rc.1.md "$stage/$name/RELEASE_NOTES.md"
fi

if [[ "$RELEASE_VISIBILITY" == public ]]; then
  "$SCRIPT_DIR/verify-license-readiness.sh"
  cp LICENSE LICENSING.md NOTICE "$stage/$name/"
else
  if [[ -f docs/legal/review/Cann-o-Call_BSL-1.1_Small-Business-License_DRAFT.md ]]; then
    mkdir -p "$stage/$name/legal-review"
    cp docs/legal/review/Cann-o-Call_BSL-1.1_Small-Business-License_DRAFT.md "$stage/$name/legal-review/"
  fi
fi

archive="$PACKAGE_DIR/$name.tar.gz"
(
  cd "$stage"
  tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner -cf - "$name" | gzip -n > "$archive"
)
cp "$BUILD_DIR/Cann-o-Call-$VERSION-source.tar.gz" "$PACKAGE_DIR/"
(
  cd "$PACKAGE_DIR"
  sha256sum "$name.tar.gz" "Cann-o-Call-$VERSION-source.tar.gz" | LC_ALL=C sort -k2 > SHA256SUMS
)
echo "package=$archive"
echo "checksums=$PACKAGE_DIR/SHA256SUMS"
