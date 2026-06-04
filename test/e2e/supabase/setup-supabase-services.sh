#!/usr/bin/env bash

set -euo pipefail

BASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)

source "$BASE_DIR/config.sh"

compose() {
  $BASE_DIR/bin/compose "$@"
}

endgroup() {
  [ "$GITHUB_ACTIONS" = "true" ] && echo "::endgroup::" || true
}
log() {
  [ "$GITHUB_ACTIONS" = "true" ] && echo -n "::group::" || true
  echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${PINK}[+]${RESET} $1"
}
warn() { echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${YELLOW}[!]${RESET} $1"; }
error() { echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${RED}[✗]${RESET} $1"; }

log "Cloning Supabase repository..."
if [ -d "$SUPABASE_DIR" ]; then
  warn "Directory '$SUPABASE_DIR' exists. Removing..."
  compose down -v
  sudo rm -rf "$SUPABASE_DIR"
fi

git clone --filter=blob:none --no-checkout "$SUPABASE_REPO" "$SUPABASE_DIR"
endgroup

cd "$SUPABASE_DIR"

log "Enabling sparse-checkout (cone mode)..."
git sparse-checkout set --cone "$SPARSE_PATH"
endgroup

log "Checking out $BRANCH..."
git checkout "$BRANCH"
endgroup

cd "$SPARSE_PATH"

log "Copying .env.example → .env..."
if [ ! -f ".env.example" ]; then
  error ".env.example not found!"
  exit 1
fi
cp .env.example .env
endgroup

log "add logs config..."
sh .run.sh config add logs

endgroup

  exited=$(compose ps --all --format '{{.Service}} {{.State}}' | awk '$2 == "exited" || $2 == "dead" {print $1}')

  if [ -n "$exited" ]; then
    error "Services exited/dead: $exited"
    for svc in $exited; do
      warn "Logs for $svc:"
      compose logs --no-log-prefix "$svc"
    done
    endgroup
    exit 1
  fi

  warn "compose up --wait reported failure but no containers exited; continuing."
  compose logs --no-log-prefix analytics
fi
endgroup

log "Waiting for Logflare to seed all sources..."

# Logflare's HTTP endpoint accepts requests as soon as Bandit binds, which
# happens before startup_tasks finishes seeding the supabase sources defined in
# lib/logflare/single_tenant.ex. A POST that lands in the seeding window
# returns 401, which Vector's HTTP sink marks as "not retriable" and drops —
# leading to flaky empty-table failures in the E2E suite (notably storage_logs).
#
# Probe each source until ingestion succeeds before declaring the stack ready.
# This only triggers a check, not re-seeding. If startup_tasks never finishes
# seeding a source (we have evidence of persistent 401 across full test runs;
# mechanism not yet proven), the probe times out at the deadline below and
# dumps the analytics tail. Strictly better than a silent flake; not a fix
# for the underlying issue, which is tracked separately.
ENV_FILE="$BASE_DIR/$SUPABASE_DIR/$SPARSE_PATH/.env"
if [ ! -f "$ENV_FILE" ]; then
  error "Cannot find .env at $ENV_FILE"
  exit 1
fi

# Don't `source` the .env file: docker-compose's .env format permits
# unquoted values with spaces, which bash interprets as commands.
LOGFLARE_PUBLIC_ACCESS_TOKEN=$(grep -E '^LOGFLARE_PUBLIC_ACCESS_TOKEN=' "$ENV_FILE" | head -1 | cut -d= -f2-)

if [ -z "${LOGFLARE_PUBLIC_ACCESS_TOKEN:-}" ]; then
  error "LOGFLARE_PUBLIC_ACCESS_TOKEN not found in $ENV_FILE"
  exit 1
fi

SOURCES=(
  "cloudflare.logs.prod"
  "postgres.logs"
  "deno-relay-logs"
  "deno-subhosting-events"
  "gotrue.logs.prod"
  "realtime.logs.prod"
  "storage.logs.prod.2"
  "postgREST.logs.prod"
  "pgbouncer.logs.prod"
)

probe_source() {
  local name="$1"
  local out
  out=$(docker exec supabase-analytics curl -s -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:4000/api/logs?source_name=$name" \
    -H "x-api-key: $LOGFLARE_PUBLIC_ACCESS_TOKEN" \
    -H "content-type: application/json" \
    --data '{"event_message":"stack-readiness-probe"}' 2>/dev/null) || out="000"
  echo "$out"
}

DEADLINE=$((SECONDS + 60))
for source in "${SOURCES[@]}"; do
  last_code=""
  while true; do
    last_code=$(probe_source "$source")
    case "$last_code" in
      2*) break ;;
    esac
    if [ "$SECONDS" -gt "$DEADLINE" ]; then
      error "Source '$source' not ready after 60s (last status: $last_code)."
      warn  "This usually means Logflare's startup_tasks crashed during seeding."
      warn  "Last 50 lines of analytics log:"
      compose logs --no-log-prefix --tail 50 analytics
      exit 1
    fi
    sleep 1
  done
done

log "All ${#SOURCES[@]} Logflare sources seeded and accepting events."
endgroup

log "Supabase stack is up! Access Supabase studio via ${CYAN}http://localhost:8000${RESET}"
log "Run E2E tests with '${GREEN}npm run playwright:test${RESET}'"
