#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
require_clean_head
cd "$ROOT"

GOOS="${GOOS:-$(go env GOOS)}"
GOARCH="${GOARCH:-$(go env GOARCH)}"
RUST_BINARY_NAME="${RUST_BINARY_NAME:-msl-ledger-sidecar}"
RELEASE_VISIBILITY="${RELEASE_VISIBILITY:-internal}"
OUT_DIR="${OUT_DIR:-$HOME/cann-o-call-release/$VERSION}"
TMP_BASE="${TMP_BASE:-$(mktemp -d "${TMPDIR:-/tmp}/cann-o-call-repro.XXXXXX")}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}"
export SOURCE_DATE_EPOCH

if [[ "$RELEASE_VISIBILITY" == public ]]; then
  "$SCRIPT_DIR/verify-license-readiness.sh"
fi

mkdir -p "$OUT_DIR" "$TMP_BASE/go-a" "$TMP_BASE/go-b" "$TMP_BASE/rust-a" "$TMP_BASE/rust-b"
trap 'rm -rf "$TMP_BASE"' EXIT

GO_LDFLAGS="${GO_LDFLAGS:--buildid=}"
export CGO_ENABLED="${CGO_ENABLED:-$(go env CGO_ENABLED)}"

build_go() {
  local out="$1"
  GOOS="$GOOS" GOARCH="$GOARCH" go build \
    -trimpath -buildvcs=false -ldflags="$GO_LDFLAGS" \
    -o "$out" .
}

build_go "$TMP_BASE/go-a/cann-o-call"
build_go "$TMP_BASE/go-b/cann-o-call"
GO_SHA_A="$(sha256_file "$TMP_BASE/go-a/cann-o-call")"
GO_SHA_B="$(sha256_file "$TMP_BASE/go-b/cann-o-call")"
[[ "$GO_SHA_A" == "$GO_SHA_B" ]] || { echo 'ERROR: Go build is not reproducible' >&2; exit 40; }
cp "$TMP_BASE/go-a/cann-o-call" "$OUT_DIR/cann-o-call"
chmod 0755 "$OUT_DIR/cann-o-call"

export CARGO_INCREMENTAL=0
BASE_RUSTFLAGS="--remap-path-prefix=$ROOT=."
export RUSTFLAGS="${RUSTFLAGS:-$BASE_RUSTFLAGS}"
CARGO_TARGET_DIR="$TMP_BASE/rust-a" cargo build --locked --release --manifest-path sidecar/Cargo.toml
CARGO_TARGET_DIR="$TMP_BASE/rust-b" cargo build --locked --release --manifest-path sidecar/Cargo.toml
RUST_A="$TMP_BASE/rust-a/release/$RUST_BINARY_NAME"
RUST_B="$TMP_BASE/rust-b/release/$RUST_BINARY_NAME"
test -x "$RUST_A" && test -x "$RUST_B"
RUST_SHA_A="$(sha256_file "$RUST_A")"
RUST_SHA_B="$(sha256_file "$RUST_B")"
[[ "$RUST_SHA_A" == "$RUST_SHA_B" ]] || { echo 'ERROR: Rust build is not reproducible' >&2; exit 41; }
cp "$RUST_A" "$OUT_DIR/$RUST_BINARY_NAME"
chmod 0755 "$OUT_DIR/$RUST_BINARY_NAME"

git archive --format=tar --prefix="Cann-o-Call-$VERSION/" HEAD | gzip -n > "$OUT_DIR/Cann-o-Call-$VERSION-source.tar.gz"

python3 "$SCRIPT_DIR/provenance-manifest.py" \
  --root "$ROOT" --out "$OUT_DIR/BUILDINFO.json" --version "$VERSION" \
  --go-sha "$GO_SHA_A" --rust-sha "$RUST_SHA_A" \
  --goos "$GOOS" --goarch "$GOARCH" --source-date-epoch "$SOURCE_DATE_EPOCH"

(
  cd "$OUT_DIR"
  sha256sum cann-o-call "$RUST_BINARY_NAME" "Cann-o-Call-$VERSION-source.tar.gz" BUILDINFO.json \
    | LC_ALL=C sort -k2 > SHA256SUMS
)

printf 'reproducible_go_sha256=%s\n' "$GO_SHA_A"
printf 'reproducible_rust_sha256=%s\n' "$RUST_SHA_A"
printf 'output_dir=%s\n' "$OUT_DIR"
