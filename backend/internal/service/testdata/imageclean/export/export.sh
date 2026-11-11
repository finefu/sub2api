#!/usr/bin/env bash
# Regenerate the image-clean golden corpus from YeaGrow's real cleaners.
#
# Usage: export.sh <yeagrow checkout at the pinned SHA, with node_modules> [golang image]
#   - seeds:  Go stdlib encoders (seedgen) in a golang container
#   - corpus: YeaGrow's own tests/generated-image-metadata.test.ts and
#             tests/generated-image-jpeg-metadata.test.ts plus extra-cases.test.mts,
#             run with hooks.mjs so every sanitize call is recorded.
# Env: GOLDEN_MUTATIONS (mutations per seed, default 24), GOLDEN_CORPUS_DIR (output dir).
# Requires Node >= 22.15 (module.registerHooks, built-in type stripping).
set -euo pipefail
YEAGROW_DIR=$(cd "$1" && pwd)
GOLANG_IMAGE=${2:-golang:1.27.3}
HERE=$(cd "$(dirname "$0")" && pwd)
CORPUS=${GOLDEN_CORPUS_DIR:-$(cd "$HERE/.." && pwd)/corpus}  # override for exploratory runs (GOLDEN_MUTATIONS=N)
SEEDS=$(mktemp -d)
trap 'rm -rf "$SEEDS"' EXIT

docker run --rm -v "$HERE/seedgen:/seedgen:ro" -v "$SEEDS:/out" -w /seedgen "$GOLANG_IMAGE" \
  sh -c 'GOFLAGS=-mod=mod GO111MODULE=off go run main.go /out'

SHA=$(git -C "$YEAGROW_DIR" rev-parse HEAD)
rm -rf "$CORPUS" && mkdir -p "$CORPUS"
cd "$YEAGROW_DIR"
GOLDEN_OUT="$CORPUS" GOLDEN_YEAGROW_SHA="$SHA" YEAGROW_DIR="$YEAGROW_DIR" SEEDS_DIR="$SEEDS" \
  node --experimental-strip-types --import "$HERE/hooks.mjs" --test --test-isolation=none \
  tests/generated-image-metadata.test.ts tests/generated-image-jpeg-metadata.test.ts "$HERE/extra-cases.test.mts"
echo "corpus: $(ls "$CORPUS" | wc -l) files, yeagrow $SHA"
