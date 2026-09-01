#!/usr/bin/env bash
#
# Repeated mirrored comparison of fixed-probability sampling rates for
# broadway.processor.message.stop.duration in a minimal no-op Broadway topology
# or the ClickHouse Broadway processor path. Every block runs in a fresh BEAM and
# retains the other three production Broadway metrics through OtelMetricExporter.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -x "$ROOT/../bin/x" ]]; then
  mix_command=("$ROOT/../bin/x" mix)
elif command -v mix >/dev/null 2>&1; then
  mix_command=(mix)
else
  echo "error: mix is not available on PATH" >&2
  exit 127
fi

SCENARIO="${SCENARIO:-${1:-noop}}"
EVENT_TYPE="${EVENT_TYPE:-log}"
BENCH_FILE="bench/broadway_metric_sampling.exs"
SUMMARY_SCRIPT="bench/support/summarize_broadway_metric_sampling.py"

case "$SCENARIO" in
  noop)
    EVENT_TYPE=none
    EVENTS="${EVENTS:-100000}"
    PROCESSOR_CONCURRENCY="${PROCESSOR_CONCURRENCY:-1}"
    BATCHER_CONCURRENCY="${BATCHER_CONCURRENCY:-1}"
    ;;
  clickhouse)
    EVENTS="${EVENTS:-10000}"
    PROCESSOR_CONCURRENCY="${PROCESSOR_CONCURRENCY:-6}"
    BATCHER_CONCURRENCY="${BATCHER_CONCURRENCY:-4}"
    [[ "$EVENT_TYPE" =~ ^(log|metric|trace)$ ]] || {
      echo "error: EVENT_TYPE must be log, metric, or trace for clickhouse" >&2
      exit 1
    }
    ;;
  *)
    echo "error: SCENARIO must be noop or clickhouse" >&2
    exit 1
    ;;
esac

RESULT_DIR="${RESULT_DIR:-${TMPDIR:-/tmp}/logflare-broadway-metric-sampling-${SCENARIO}-${EVENT_TYPE}-$(date +%Y%m%d-%H%M%S)}"
WARMUPS="${WARMUPS:-2}"
TRIALS="${TRIALS:-5}"
BATCH_SIZE="${BATCH_SIZE:-$EVENTS}"
BATCH_TIMEOUT_MS="${BATCH_TIMEOUT_MS:-60000}"
PAYLOAD_SHAPE="${PAYLOAD_SHAPE:-realistic}"
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

  printf '>> order_index=%s scenario=%s event_type=%s message_sample_denominator=%s block=%s\n' \
    "$order_index" "$SCENARIO" "$EVENT_TYPE" "$denominator" "$block"

  ERL_FLAGS="+S ${SCHEDULERS}:${SCHEDULERS} +sbwt none +sbwtdcpu none +sbwtdio none" \
    SCENARIO="$SCENARIO" \
    EVENT_TYPE="$EVENT_TYPE" \
    TELEMETRY_HANDLER_STORE=ets \
    MESSAGE_SAMPLE_DENOMINATOR="$denominator" \
    ORDER_INDEX="$order_index" \
    EVENTS="$EVENTS" \
    WARMUPS="$WARMUPS" \
    TRIALS="$TRIALS" \
    PROCESSOR_CONCURRENCY="$PROCESSOR_CONCURRENCY" \
    BATCHER_CONCURRENCY="$BATCHER_CONCURRENCY" \
    BATCH_SIZE="$BATCH_SIZE" \
    BATCH_TIMEOUT_MS="$BATCH_TIMEOUT_MS" \
    PAYLOAD_SHAPE="$PAYLOAD_SHAPE" \
    "${mix_command[@]}" run --no-start "$BENCH_FILE" > "$output" 2>&1

  grep -E '^(config|handlers|sample|summary|metric_store) ' "$output"
  order_index=$((order_index + 1))
}

validate_positive "$EVENTS" EVENTS
validate_non_negative "$WARMUPS" WARMUPS
validate_positive "$TRIALS" TRIALS
validate_positive "$PROCESSOR_CONCURRENCY" PROCESSOR_CONCURRENCY
validate_positive "$BATCHER_CONCURRENCY" BATCHER_CONCURRENCY
validate_positive "$BATCH_SIZE" BATCH_SIZE
validate_positive "$BATCH_TIMEOUT_MS" BATCH_TIMEOUT_MS
validate_positive "$SCHEDULERS" SCHEDULERS
validate_positive "$CYCLES" CYCLES

if (( EVENTS % BATCH_SIZE != 0 )); then
  echo "error: EVENTS must be divisible by BATCH_SIZE" >&2
  exit 1
fi

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
printf 'design=repeated_mirrored cycles=%s conditions=none,10000,1000,100,10,1 scenario=%s event_type=%s spans=enabled handler_store=ets schedulers=%s events=%s warmups=%s trials_per_block=%s processor_concurrency=%s batcher_concurrency=%s batch_size=%s batch_timeout_ms=%s payload_shape=%s output_mode=in_memory\n' \
  "$CYCLES" "$SCENARIO" "$EVENT_TYPE" "$SCHEDULERS" "$EVENTS" "$WARMUPS" "$TRIALS" \
  "$PROCESSOR_CONCURRENCY" "$BATCHER_CONCURRENCY" "$BATCH_SIZE" "$BATCH_TIMEOUT_MS" "$PAYLOAD_SHAPE"

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
