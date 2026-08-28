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

# Replaces (or appends) KEY=VALUE in the compose .env file. Avoids sed
# replacement-string escaping issues with arbitrary values.
set_env_var() {
  local key="$1" value="$2"
  grep -vE "^${key}=" .env > .env.tmp || true
  mv .env.tmp .env
  printf '%s=%s\n' "$key" "$value" >> .env
}

case "$LOGFLARE_BACKEND" in
  postgres) ;;
  bigquery)
    # Only the service account key is required. GOOGLE_PROJECT_ID,
    # GOOGLE_PROJECT_NUMBER and GOOGLE_DATASET_ID_APPEND default to the CI
    # project/dataset via docker-compose.e2e.bigquery.yml and may be overridden
    # via the environment. Run with e.g.:
    #   LOGFLARE_BACKEND=bigquery \
    #   GOOGLE_CREDENTIALS_JSON="$(cat /path/to/gcloud.json)" \
    #   ./setup-supabase-services.sh
    if [ -z "${GOOGLE_CREDENTIALS_JSON:-}" ]; then
      error "LOGFLARE_BACKEND=bigquery requires: GOOGLE_CREDENTIALS_JSON"
      exit 1
    fi
    ;;
  *)
    error "Invalid LOGFLARE_BACKEND '$LOGFLARE_BACKEND' (expected 'postgres' or 'bigquery')"
    exit 1
    ;;
esac

log "Using Logflare backend: $LOGFLARE_BACKEND"
endgroup

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

if [ "$LOGFLARE_BACKEND" = "bigquery" ]; then
  log "Configuring BigQuery backend..."
  # .env.example ships these as literal placeholders (e.g.
  # GOOGLE_PROJECT_ID=GOOGLE_PROJECT_ID). Blank them so the non-empty
  # placeholder doesn't shadow the ${VAR:-default} defaults baked into
  # docker-compose.e2e.bigquery.yml. Overrides still work: a value set in the
  # environment wins over the (now empty) .env entry during compose
  # interpolation, so CI's per-job GOOGLE_DATASET_ID_APPEND still applies.
  set_env_var "GOOGLE_PROJECT_ID" ""
  set_env_var "GOOGLE_PROJECT_NUMBER" ""
  set_env_var "GOOGLE_DATASET_ID_APPEND" ""

  # Bind-mounted into the analytics container by docker-compose.e2e.bigquery.yml.
  # Must exist before `compose up`, otherwise docker creates a directory at the
  # mount target and Logflare boots without BigQuery credentials.
  printf '%s' "$GOOGLE_CREDENTIALS_JSON" > gcloud.json
  endgroup
fi

cd ../..

if [ "$LOGFLARE_BACKEND" = "bigquery" ]; then
  log "Verifying BigQuery compose overlay..."
  # Logflare picks the Postgres backend whenever POSTGRES_BACKEND_URL is
  # present (even empty), so the overlay's `!reset` must have removed it from
  # the merged config. `!reset` is silently ignored when multiple earlier -f
  # files contribute environment to the analytics service
  # (docker/compose#11816) — assert the rendered result instead of trusting it.
  rendered=$(compose config analytics)
  if grep -q "POSTGRES_BACKEND_URL" <<<"$rendered"; then
    error "POSTGRES_BACKEND_URL survived the BigQuery overlay; Logflare would boot in Postgres mode."
    error "Check docker-compose.e2e.bigquery.yml ordering and docker/compose#11816."
    exit 1
  fi
  for key in GOOGLE_PROJECT_ID GOOGLE_PROJECT_NUMBER GOOGLE_DATASET_ID_APPEND; do
    if ! grep -q "$key" <<<"$rendered"; then
      error "${key} missing from rendered analytics config."
      exit 1
    fi
    if grep -qE "${key}[[:space:]]*[:=][[:space:]]*\"?${key}\"?" <<<"$rendered"; then
      error "${key} resolved to its literal .env placeholder; it is shadowing the default in docker-compose.e2e.bigquery.yml."
      exit 1
    fi
  done
  if ! grep -q "gcloud.json" <<<"$rendered"; then
    error "gcloud.json mount missing from rendered analytics config."
    exit 1
  fi
  endgroup
fi

[ ! "$GITHUB_ACTIONS" = "true" ] && log "Build logflare image..." && compose build analytics

log "Pulling docker images..."
compose pull
endgroup

log "Starting Supabase stack (inside docker containers)..."
if ! compose up -d --wait --wait-timeout 180; then
  if [ "$GITHUB_ACTIONS" = "true" ]; then
    endgroup
    echo -n "::group::"
  fi

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

  # No container exited, so something is up but unhealthy. `depends_on:
  # service_healthy` means an unhealthy analytics container silently prevents
  # kong from ever starting, and nothing then listens on the public port — the
  # E2E suite fails with a wall of ERR_CONNECTION_REFUSED that says nothing
  # about the real cause. Name the unhealthy services and dump their logs.
  unhealthy=$(compose ps --all --format '{{.Service}} {{.Health}}' | awk '$2 == "unhealthy" || $2 == "starting" {print $1}')

  if [ -n "$unhealthy" ]; then
    error "Services failed their healthcheck: $unhealthy"
    for svc in $unhealthy; do
      warn "Logs for $svc:"
      compose logs --no-log-prefix --tail 100 "$svc"
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

log "Waiting for the API gateway to answer on the host..."

# The probe above runs `curl` *inside* the analytics container, so it stays
# green even when kong never started and nothing is published to the host.
# Playwright talks to kong from the host, so check that path explicitly rather
# than letting every test fail with ERR_CONNECTION_REFUSED.
KONG_HTTP_PORT=$(grep -E '^KONG_HTTP_PORT=' "$ENV_FILE" | head -1 | cut -d= -f2-)
KONG_HTTP_PORT="${KONG_HTTP_PORT:-8000}"
GATEWAY_URL="http://127.0.0.1:${KONG_HTTP_PORT}/"

DEADLINE=$((SECONDS + 60))
while true; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$GATEWAY_URL" 2>/dev/null) || code="000"
  # Any HTTP response means kong is listening; 401 is expected (dashboard auth).
  [ "$code" != "000" ] && break

  if [ "$SECONDS" -gt "$DEADLINE" ]; then
    error "API gateway not reachable at $GATEWAY_URL after 60s."
    warn  "kong is likely not running — check its depends_on targets above."
    compose ps --all
    endgroup
    exit 1
  fi
  sleep 1
done

log "API gateway is reachable at ${CYAN}${GATEWAY_URL}${RESET} (HTTP $code)."
endgroup

log "Supabase stack is up! Access Supabase studio via ${CYAN}http://localhost:8000${RESET}"
log "Run E2E tests with '${GREEN}npm run playwright:test${RESET}'"
