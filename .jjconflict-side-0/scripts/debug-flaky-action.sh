#!/usr/bin/env bash
#
# Debug flaky GitHub Actions runs by repeatedly rerunning a workflow run
# until it fails (repro mode) or until a sample size is reached
# (sample mode), then surfacing artifacts and a desktop notification.
#
# Two modes:
#
#   repro  (default) — rerun until any matching job fails, then stop.
#                      Downloads run artifacts to ARTIFACT_ROOT/attempt-<N>.
#   sample           — rerun N times regardless of outcome, report pass/fail
#                      counts at the end. Useful for quantifying flake rate.
#
# Requires: gh (authenticated), jq.
#
# Usage:
#   scripts/debug-flaky-action.sh [run-id] [options]
#
# Examples:
#   # Loop the most recent run on the current branch, stop on any failure
#   scripts/debug-flaky-action.sh
#
#   # Loop a specific run, only counting failures whose job name matches "Playwright"
#   scripts/debug-flaky-action.sh 25466632470 --job-pattern Playwright
#
#   # Quantify flake rate across 30 attempts (don't stop on failure)
#   scripts/debug-flaky-action.sh --mode sample --cap 30
#
# Flags:
#   --cap N             Max attempts (default 10)
#   --poll-interval S   Seconds between polls of in-progress runs (default 30)
#   --job-pattern STR   Substring to match job names (default: any job)
#   --mode MODE         repro | sample (default: repro)
#   --artifact-root DIR Where to download artifacts (default $TMPDIR/repro-artifacts)
#   --no-notify         Disable desktop notifications (terminal bell still rings)
#   -h | --help         This help text

set -euo pipefail

# ---------- argument parsing ----------

CAP=10
POLL_INTERVAL=30
JOB_PATTERN=""
MODE="repro"
ARTIFACT_ROOT="${TMPDIR:-/tmp}"
ARTIFACT_ROOT="${ARTIFACT_ROOT%/}/repro-artifacts"
NOTIFY=1
RUN_ID=""

print_help() {
  # Print the leading comment block (everything from "Debug flaky" until the
  # first non-comment line) with the leading "# " stripped.
  awk '/^# Debug flaky/{p=1} p{if(!/^#/) exit; sub(/^# ?/, ""); print}' "$0"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cap) CAP="$2"; shift 2 ;;
    --poll-interval) POLL_INTERVAL="$2"; shift 2 ;;
    --job-pattern) JOB_PATTERN="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --artifact-root) ARTIFACT_ROOT="$2"; shift 2 ;;
    --no-notify) NOTIFY=0; shift ;;
    -h|--help) print_help; exit 0 ;;
    --) shift; break ;;
    -*)
      echo "Unknown flag: $1" >&2
      echo "Run with --help for usage." >&2
      exit 2
      ;;
    *)
      if [ -z "$RUN_ID" ]; then
        RUN_ID="$1"
      else
        echo "Unexpected positional arg: $1" >&2
        exit 2
      fi
      shift
      ;;
  esac
done

case "$MODE" in
  repro|sample) ;;
  *) echo "Invalid --mode: $MODE (expected repro|sample)" >&2; exit 2 ;;
esac

# ---------- dependency checks ----------

for tool in gh jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Missing required tool: $tool" >&2
    exit 1
  fi
done

# ---------- auto-detect run id ----------

if [ -z "$RUN_ID" ]; then
  branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
  if [ -z "$branch" ]; then
    echo "Could not determine current branch and no run-id provided." >&2
    exit 1
  fi
  RUN_ID=$(gh run list --branch "$branch" --limit 1 --json databaseId -q '.[0].databaseId' 2>/dev/null || true)
  if [ -z "$RUN_ID" ] || [ "$RUN_ID" = "null" ]; then
    echo "No runs found for branch '$branch'. Push a commit or pass a run id." >&2
    exit 1
  fi
fi

# ---------- helpers ----------

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

notify() {
  local title="$1"
  local message="$2"
  printf '\a'
  [ "$NOTIFY" = "0" ] && return
  if command -v osascript >/dev/null 2>&1; then
    osascript -e "display notification \"$message\" with title \"$title\"" >/dev/null 2>&1 || true
  elif command -v notify-send >/dev/null 2>&1; then
    notify-send "$title" "$message" >/dev/null 2>&1 || true
  fi
}

fetch_jobs() {
  gh run view "$RUN_ID" --json jobs -q '[.jobs[] | {name, status, conclusion}]'
}

# Returns: "running", "passed", "failed"
# When --job-pattern is set, only failures from matching jobs count as "failed".
# Non-matching failures (e.g., unrelated infrastructure jobs) are treated as "passed"
# for the purpose of the loop's stop condition.
classify() {
  local jobs="$1"
  local pattern="$2"
  local n_in_progress n_failed
  n_in_progress=$(printf '%s' "$jobs" | jq '[.[] | select(.status != "completed")] | length')
  if [ "$n_in_progress" -gt 0 ]; then
    echo "running"
    return
  fi
  if [ -n "$pattern" ]; then
    n_failed=$(printf '%s' "$jobs" | jq --arg p "$pattern" '[.[] | select(.conclusion == "failure" and (.name | contains($p)))] | length')
  else
    n_failed=$(printf '%s' "$jobs" | jq '[.[] | select(.conclusion == "failure")] | length')
  fi
  if [ "$n_failed" -gt 0 ]; then
    echo "failed"
  else
    echo "passed"
  fi
}

summarize() {
  printf '%s' "$1" | jq -r '[.[] | "\(.name)=\(.conclusion // .status)"] | join(" | ")'
}

fmt_elapsed() { local s="$1"; printf '%d:%02d' "$((s / 60))" "$((s % 60))"; }

# ---------- main loop ----------

log "==== debug-flaky-action ===="
log "  RUN_ID=$RUN_ID  CAP=$CAP  MODE=$MODE  POLL_INTERVAL=${POLL_INTERVAL}s"
log "  JOB_PATTERN='${JOB_PATTERN:-<any>}'  ARTIFACT_ROOT=$ARTIFACT_ROOT"

PASS=0
FAIL=0
N=1

while [ "$N" -le "$CAP" ]; do
  ATTEMPT_START=$(date +%s)
  POLL_COUNT=0
  log "---- attempt $N/$CAP — polling run $RUN_ID ----"

  while true; do
    POLL_COUNT=$((POLL_COUNT + 1))
    if ! JOBS=$(fetch_jobs 2>&1); then
      log "  poll #$POLL_COUNT — gh fetch FAILED:"
      printf '%s\n' "$JOBS" | sed 's/^/    /'
      log "  retrying after ${POLL_INTERVAL}s"
      sleep "$POLL_INTERVAL"
      continue
    fi
    ELAPSED=$(fmt_elapsed $(($(date +%s) - ATTEMPT_START)))
    STATE=$(classify "$JOBS" "$JOB_PATTERN")
    log "  poll #$POLL_COUNT [elapsed $ELAPSED] state=$STATE  $(summarize "$JOBS")"

    case "$STATE" in
      running)
        sleep "$POLL_INTERVAL"
        ;;
      passed)
        PASS=$((PASS + 1))
        log "attempt $N PASSED ($ELAPSED, $POLL_COUNT polls)"
        break
        ;;
      failed)
        FAIL=$((FAIL + 1))
        log "attempt $N FAILED ($ELAPSED, $POLL_COUNT polls)"
        printf '%s' "$JOBS" | jq -r '.[] | select(.conclusion == "failure") | "  \(.name): \(.conclusion)"'
        DEST="$ARTIFACT_ROOT/attempt-$N"
        mkdir -p "$DEST"
        log "downloading artifacts to $DEST"
        if gh run download "$RUN_ID" --dir "$DEST" 2>/dev/null; then
          file_count=$(find "$DEST" -type f | wc -l | tr -d ' ')
          log "downloaded $file_count files"
        else
          log "no artifacts available or download failed"
        fi
        if [ "$MODE" = "repro" ]; then
          notify "CI Flake Repro" "Failure on attempt $N — artifacts at $DEST"
          log "==== repro mode: stopping on first failure ===="
          exit 0
        fi
        break
        ;;
    esac
  done

  if [ "$N" -ge "$CAP" ]; then
    break
  fi

  N=$((N + 1))
  log "triggering rerun → attempt $N/$CAP"
  if ! gh run rerun "$RUN_ID" 2>&1; then
    log "gh run rerun failed — exiting"
    exit 1
  fi
  sleep 30  # let the rerun register before polling
done

# ---------- summary ----------

TOTAL=$((PASS + FAIL))
log "==== final: $PASS passed, $FAIL failed of $TOTAL ===="
if [ "$MODE" = "sample" ] && [ "$TOTAL" -gt 0 ]; then
  rate=$(awk "BEGIN { printf \"%.1f\", ($FAIL / $TOTAL) * 100 }")
  log "flake rate: ${rate}%"
fi
notify "CI Flake Repro" "Done: $PASS passed, $FAIL failed of $TOTAL"
