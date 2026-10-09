#!/usr/bin/env bash
# Runs the ingest QA against a server started with scripts/qa/server.sh.
# Exits non-zero on any failure. Set QA_SKIP_SCREENSHOT=1 to skip the search UI capture.
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
source "$(git -C "$DIR" rev-parse --show-toplevel)/scripts/qa/env.sh"
QA="$REPO_ROOT/scripts/qa"

for _ in $(seq 1 120); do
  curl -sf "$LOGFLARE_URL/health" >/dev/null && break
  sleep 1
done
curl -sf "$LOGFLARE_URL/health" >/dev/null || { echo "QA_RESULT FAIL server not healthy at $LOGFLARE_URL"; exit 1; }

setup=$("$QA/remsh.sh" "$DIR/setup.exs")
main_token=$(grep -o 'MAIN_TOKEN=[^ ]*' <<<"$setup" | head -1 | cut -d= -f2)
main_id=$(grep -o 'MAIN_ID=[0-9]*' <<<"$setup" | head -1 | cut -d= -f2)
[ -n "$main_token" ] || { echo "$setup" | grep -E '\*\*|error' | head -20; echo "QA_RESULT FAIL setup"; exit 1; }

run_id="run$(date +%s)"
kinds=(error warn info)
channels=(http_token http_name websocket grpc)
failures=0

check() {
  if [ "$2" = "$3" ]; then echo "QA_CHECK PASS $1"; else echo "QA_CHECK FAIL $1 (got $2, want $3)"; failures=$((failures + 1)); fi
}

batch() {
  local events=()
  for kind in "${kinds[@]}"; do events+=("{\"message\":\"$kind from $1 $run_id\"}"); done
  local IFS=,
  echo "[${events[*]}]"
}

post() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "$LOGFLARE_URL/api/logs?$1" -H 'content-type: application/json' "${@:3}" -d "{\"batch\":$2}"
}

key=(-H "x-api-key: $QA_PUBLIC_TOKEN")
check "HTTP ingest by source token" "$(post "source=$main_token" "$(batch http_token)" "${key[@]}")" 200
check "HTTP ingest by source name" "$(post "source_name=qa_ingest_main" "$(batch http_name)" "${key[@]}")" 200
check "HTTP rejects unknown source" "$(post "source=00000000-0000-0000-0000-000000000000" "[]" "${key[@]}")" 401
check "HTTP rejects missing api key" "$(post "source=$main_token" "[]")" 401
check "HTTP rejects wrong api key" "$(post "source=$main_token" "[]" -H 'x-api-key: wrong')" 401

ws_url="${LOGFLARE_URL/http/ws}/logs/websocket?vsn=2.0.0&access_token=$QA_PUBLIC_TOKEN"
ws=$(node "$DIR/websocket.mjs" "$ws_url" "$main_token" "$(batch websocket)" || true)
check "WebSocket LogChannel ingest" "$ws" "websocket ok"

grpc_messages=$(for kind in "${kinds[@]}"; do printf '%s|' "$kind from grpc $run_id"; done)
grpc=$("$QA/remsh.sh" "$DIR/grpc.exs" "MESSAGES=${grpc_messages%|}" "API_KEY=$QA_PUBLIC_TOKEN" \
  "SOURCE_TOKEN=$main_token" "GRPC_PORT=$QA_GRPC_PORT" | grep -o 'grpc \(ok\|FAIL.*\)' | head -1 || true)
check "gRPC OTLP logs export" "$grpc" "grpc ok"

verify=$("$QA/remsh.sh" "$DIR/verify.exs" "RUN_ID=$run_id" | grep -oE 'QA_(CHECK|DIFF|RESULT) .*' || true)
echo "$verify"
grep -q '^QA_RESULT PASS' <<<"$verify" || failures=$((failures + 1))

if [ "${QA_SKIP_SCREENSHOT:-}" != 1 ]; then
  messages=$(for c in "${channels[@]}"; do for k in "${kinds[@]}"; do echo "$k from $c $run_id"; done; done)
  if INGEST_QA_SOURCE_ID="$main_id" INGEST_QA_RUN_ID="$run_id" INGEST_QA_MESSAGES="$messages" \
    npm --prefix "$REPO_ROOT/scripts/screenshot" run screenshot -- specs/ingest-search.spec.ts >/tmp/ingest-qa-screenshot.log 2>&1; then
    echo "QA_CHECK PASS search UI shows ingested events (verify scripts/screenshot/.generated/ingest-search-*.png)"
  else
    tail -30 /tmp/ingest-qa-screenshot.log
    echo "QA_CHECK FAIL search UI shows ingested events (log: /tmp/ingest-qa-screenshot.log)"
    failures=$((failures + 1))
  fi
fi

if [ "$failures" -eq 0 ]; then
  echo "QA_RUN $run_id PASS"
else
  echo "QA_RUN $run_id FAIL"
  exit 1
fi
