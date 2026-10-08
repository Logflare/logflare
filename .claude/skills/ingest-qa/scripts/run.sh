#!/usr/bin/env bash
# Runs the ingest QA against a server started with server.sh. Exits non-zero on any failure.
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
source "$DIR/env.sh"

base="http://localhost:$QA_PORT"
for _ in $(seq 1 120); do
  curl -sf "$base/health" >/dev/null && break
  sleep 1
done
curl -sf "$base/health" >/dev/null || { echo "QA_RESULT FAIL server not healthy at $base"; exit 1; }

setup=$("$DIR/remsh.sh" "$DIR/setup.exs")
main_token=$(grep -o 'QA_ENV MAIN_TOKEN=[^ ]*' <<<"$setup" | head -1 | cut -d= -f2)
[ -n "$main_token" ] || { echo "$setup" | grep -E '\*\*|error' | head -20; echo "QA_RESULT FAIL setup"; exit 1; }

run_id="run$(date +%s)"
kinds=(error warn info)
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
  curl -s -o /dev/null -w '%{http_code}' -X POST "$base/api/logs?$1" -H 'content-type: application/json' "${@:3}" -d "{\"batch\":$2}"
}

key=(-H "x-api-key: $QA_PUBLIC_TOKEN")
check "HTTP ingest by source token" "$(post "source=$main_token" "$(batch http_token)" "${key[@]}")" 200
check "HTTP ingest by source name" "$(post "source_name=qa_ingest_main" "$(batch http_name)" "${key[@]}")" 200
check "HTTP rejects unknown source" "$(post "source=00000000-0000-0000-0000-000000000000" "[]" "${key[@]}")" 401
check "HTTP rejects missing api key" "$(post "source=$main_token" "[]")" 401
check "HTTP rejects wrong api key" "$(post "source=$main_token" "[]" -H 'x-api-key: wrong')" 401

ws=$(node "$DIR/websocket.mjs" "ws://localhost:$QA_PORT/logs/websocket?vsn=2.0.0&access_token=$QA_PUBLIC_TOKEN" "$main_token" "$(batch websocket)" || true)
check "WebSocket LogChannel ingest" "$ws" "websocket ok"

grpc_messages=$(for kind in "${kinds[@]}"; do printf '%s|' "$kind from grpc $run_id"; done)
grpc=$("$DIR/remsh.sh" "$DIR/grpc.exs" "MESSAGES=${grpc_messages%|}" "API_KEY=$QA_PUBLIC_TOKEN" \
  "SOURCE_TOKEN=$main_token" "GRPC_PORT=$QA_GRPC_PORT" | grep -o 'grpc \(ok\|FAIL.*\)' | head -1 || true)
check "gRPC OTLP logs export" "$grpc" "grpc ok"

verify=$("$DIR/remsh.sh" "$DIR/verify.exs" "RUN_ID=$run_id" | grep -oE 'QA_(CHECK|DIFF|RESULT) .*' || true)
echo "$verify"

if [ "$failures" -eq 0 ] && grep -q '^QA_RESULT PASS' <<<"$verify"; then
  echo "QA_RUN $run_id PASS"
else
  echo "QA_RUN $run_id FAIL"
  exit 1
fi
