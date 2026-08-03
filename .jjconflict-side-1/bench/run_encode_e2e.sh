#!/usr/bin/env bash
#
# Before/after end-to-end encode-stage benchmark for the ClickHouse ingester.
#
# Runs bench/clickhouse_encode_e2e.exs against the baseline SHA and the current
# branch HEAD, then prints a per-job comparison. The bench scripts are untracked,
# so they survive `git checkout` and run unmodified against both revisions.
#
# Usage: bench/run_encode_e2e.sh [baseline_sha]
set -euo pipefail

BASELINE="${1:-e38c122ac}"
# Results are transient comparison artifacts — keep them out of the repo tree,
# consistent with the other benches here (which persist nothing).
SAVE_DIR="${BENCH_SAVE_DIR:-${TMPDIR:-/tmp}/logflare_encode_e2e}"
export BENCH_SAVE_DIR="$SAVE_DIR"

ORIG_REF="$(git symbolic-ref --quiet --short HEAD || git rev-parse HEAD)"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "error: tracked working tree is dirty; commit or stash before benchmarking." >&2
  exit 1
fi

restore() {
  echo ">> restoring $ORIG_REF"
  git checkout --quiet "$ORIG_REF"
}
trap restore EXIT

mkdir -p "$SAVE_DIR"
rm -f "$SAVE_DIR"/encode_e2e_*.benchee

run_one() {
  local ref="$1" tag="$2"
  echo "============================================================"
  echo ">> $tag: $ref"
  echo "============================================================"
  git checkout --quiet "$ref"
  TAG="$tag" mix run --no-start bench/clickhouse_encode_e2e.exs
}

run_one "$BASELINE" before
run_one "$ORIG_REF" after

echo "============================================================"
echo ">> comparison (after vs before)"
echo "============================================================"
mix run --no-start bench/compare_encode_e2e.exs
