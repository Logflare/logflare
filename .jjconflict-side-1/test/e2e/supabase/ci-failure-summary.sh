#!/usr/bin/env bash
#
# Emit a Markdown failure summary suitable for $GITHUB_STEP_SUMMARY.
#
# Reads test-results/results.json (Playwright json reporter output) and
# prints, for each failed test:
#   - test title, file:line, classification
#   - the error message (collapsed)
#   - for `polling timeout` failures, the last N lines of the relevant
#     container logs from container-logs/<container>.log (also collapsed)
#
# Inputs (env / cwd):
#   - working dir must contain test-results/results.json (Playwright output)
#   - container-logs/ optional; populated by the workflow's "Capture
#     container logs" step on failure
#
# Output:
#   - Markdown to stdout. The workflow redirects this to $GITHUB_STEP_SUMMARY.

set -uo pipefail

RESULTS_JSON="test-results/results.json"
LOGS_DIR="container-logs"
LOG_TAIL_LINES=50

if [ ! -f "$RESULTS_JSON" ]; then
  echo "## Test run summary"
  echo
  echo "_No \`results.json\` produced — Playwright did not finish writing reports._"
  exit 0
fi

# Recursively walk suites/specs, emit one JSON object per failed result.
mapfile -t failures < <(jq -c '
  [ .. | objects | select(has("specs"))
    | .specs[]
    | { file, line, title } as $loc
    | .tests[]?
    | .results[]?
    | select(.status == "failed")
    | $loc + { error: (.error.message // "") }
  ]
  | unique_by(.file + "|" + (.line|tostring) + "|" + .title)
  | .[]
' "$RESULTS_JSON")

echo "## Test failures (${#failures[@]})"
echo

if [ "${#failures[@]}" -eq 0 ]; then
  echo "_Job failed but no individual test failures were recorded — likely a setup/install error before tests ran._"
  exit 0
fi

# Strip ANSI escapes (Playwright includes color codes in error.message).
strip_ansi() { sed -E $'s/\x1B\\[[0-9;]*[A-Za-z]//g'; }

classify() {
  local err="$1"
  if [[ "$err" == *"waitForLogs("*"timed out"* ]]; then
    local table
    table=$(printf '%s\n' "$err" | sed -nE 's/.*waitForLogs\(([a-z_]+),.*/\1/p' | head -1)
    echo "polling timeout (\`$table\`)"
  elif [[ "$err" == *"toContainText"* ]] && [[ "$err" == *"Timeout"* ]]; then
    echo "assertion race"
  elif [[ "$err" == *"failed:"* ]] || [[ "$err" == *"throw"* ]]; then
    echo "setup error"
  else
    echo "other"
  fi
}

# Map a logs table name to substrings of the container names whose logs are
# most likely to explain a polling timeout.
table_to_containers() {
  case "$1" in
    storage_logs)   echo "storage vector" ;;
    postgrest_logs) echo "rest vector" ;;
    realtime_logs)  echo "realtime vector" ;;
    edge_logs)      echo "edge-functions vector kong" ;;
    auth_logs)      echo "auth vector" ;;
    *)              echo "vector" ;;
  esac
}

extract_table() {
  printf '%s\n' "$1" | sed -nE 's/.*polling timeout \(`([a-z_]+)`\).*/\1/p'
}

for f in "${failures[@]}"; do
  title=$(printf '%s' "$f" | jq -r '.title')
  file=$(printf '%s' "$f" | jq -r '.file')
  line=$(printf '%s' "$f" | jq -r '.line // "?"')
  error=$(printf '%s' "$f" | jq -r '.error' | strip_ansi)
  classification=$(classify "$error")

  echo "### $title"
  echo
  echo "**Location**: \`$file:$line\`  "
  echo "**Classification**: $classification"
  echo
  echo "<details><summary>Error</summary>"
  echo
  echo '```'
  printf '%s\n' "$error" | head -40
  echo '```'
  echo
  echo "</details>"
  echo

  if [[ "$classification" == polling\ timeout* ]]; then
    table=$(extract_table "$classification")
    if [ -n "$table" ] && [ -d "$LOGS_DIR" ]; then
      echo "#### Container log tails (last $LOG_TAIL_LINES lines)"
      echo
      for pattern in $(table_to_containers "$table"); do
        for log in "$LOGS_DIR"/*"$pattern"*.log; do
          [ -f "$log" ] || continue
          name=$(basename "$log")
          echo "<details><summary><code>$name</code></summary>"
          echo
          echo '```'
          tail -n "$LOG_TAIL_LINES" "$log"
          echo '```'
          echo
          echo "</details>"
          echo
        done
      done
    fi
  fi
done

echo "---"
echo
echo "Full reports are in this run's artifacts: \`playwright-report-<browser>\` and \`container-logs-<browser>\`."
