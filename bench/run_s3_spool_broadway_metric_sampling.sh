#!/usr/bin/env bash
#
# Repeated mirrored comparison of fixed-probability sampling rates for
# broadway.processor.message.stop.duration. Every condition retains the other
# three production Broadway metrics through the real OtelMetricExporter.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BENCH_FILE="bench/s3_spool_broadway_metric_sampling.exs"
SUMMARY_SCRIPT="bench/support/summarize_s3_spool_broadway_metric_sampling.py"
RESULT_DIR="${RESULT_DIR:-${TMPDIR:-/tmp}/logflare-s3-spool-broadway-metric-sampling-$(date +%Y%m%d-%H%M%S)}"
EVENTS="${EVENTS:-100000}"
WARMUPS="${WARMUPS:-4}"
TRIALS="${TRIALS:-7}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
BATCH_TIMEOUT_MS="${BATCH_TIMEOUT_MS:-60000}"
SCHEDULERS="${SCHEDULERS:-6}"
CYCLES="${CYCLES:-2}"

validate_positive() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]] || { echo "error: $2 must be a positive integer" >&2; exit 1; }
}

validate_non_negative() {
  [[ "$1" =~ ^[0-9]+$ ]] || { echo "error: $2 must be a non-negative integer" >&2; exit 1; }
}

run_block() {
  local denominator="$1"
  local block="$2"
  local output="$RESULT_DIR/sampling_${denominator}_${block}.log"

  printf '>> order_index=%s message_sample_denominator=%s block=%s\n' "$order_index" "$denominator" "$block"

  ERL_FLAGS="+S ${SCHEDULERS}:${SCHEDULERS} +sbwt none +sbwtdcpu none +sbwtdio none" \
    CONDITION=telemetry_on \
    TELEMETRY_HANDLER_STORE=ets \
    MESSAGE_SAMPLE_DENOMINATOR="$denominator" \
    MANUAL_BATCH_FLUSH=true \
    ORDER_INDEX="$order_index" \
    EVENTS="$EVENTS" \
    WARMUPS="$WARMUPS" \
    TRIALS="$TRIALS" \
    PAYLOAD_BYTES="$PAYLOAD_BYTES" \
    BATCH_TIMEOUT_MS="$BATCH_TIMEOUT_MS" \
    ../bin/x mix run --no-start "$BENCH_FILE" > "$output" 2>&1

  grep -E '^(config|handlers|sample|summary|metric_store) ' "$output"
  order_index=$((order_index + 1))
}

validate_positive "$EVENTS" EVENTS
validate_non_negative "$WARMUPS" WARMUPS
validate_positive "$TRIALS" TRIALS
validate_non_negative "$PAYLOAD_BYTES" PAYLOAD_BYTES
validate_positive "$BATCH_TIMEOUT_MS" BATCH_TIMEOUT_MS
validate_positive "$SCHEDULERS" SCHEDULERS
validate_positive "$CYCLES" CYCLES

if (( CYCLES > 2 )); then
  echo "error: CYCLES must not exceed 2 because block labels use the 26-letter alphabet" >&2
  exit 1
fi

if grep -Fq 'def __telemetry_enabled__?, do: false' deps/broadway/lib/broadway/topology/processor_stage.ex; then
  echo "error: Broadway dependency source is still patched with telemetry disabled" >&2
  exit 1
fi

mkdir -p "$RESULT_DIR"
printf 'result_dir=%s\n' "$RESULT_DIR"
printf 'design=repeated_mirrored cycles=%s conditions=none,10000,1000,100,10,1 spans=enabled handler_store=ets manual_batch_flush=true schedulers=%s events=%s warmups=%s trials_per_block=%s payload_bytes=%s batch_timeout_ms=%s\n' \
  "$CYCLES" "$SCHEDULERS" "$EVENTS" "$WARMUPS" "$TRIALS" "$PAYLOAD_BYTES" "$BATCH_TIMEOUT_MS"

letters=(a b c d e f g h i j k l m n o p q r s t u v w x y z)
conditions=(none 10000 1000 100 10 1)
block_index=0
order_index=1

for ((cycle = 1; cycle <= CYCLES; cycle++)); do
  for denominator in "${conditions[@]}"; do
    run_block "$denominator" "${letters[$block_index]}"
    block_index=$((block_index + 1))
  done

  for ((index = ${#conditions[@]} - 1; index >= 0; index--)); do
    denominator="${conditions[$index]}"
    run_block "$denominator" "${letters[$block_index]}"
    block_index=$((block_index + 1))
  done
done

python3 "$SUMMARY_SCRIPT" --output "$RESULT_DIR/summary.json" "$RESULT_DIR"/*.log \
  | tee "$RESULT_DIR/summary.txt"
printf 'summary_json=%s/summary.json\n' "$RESULT_DIR"
