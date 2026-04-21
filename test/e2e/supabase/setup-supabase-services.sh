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

cd ../..

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

  warn "compose up --wait reported failure but no containers exited; continuing."
  compose logs --no-log-prefix analytics
fi
endgroup

log "Supabase stack is up! Access Supabase studio via ${CYAN}http://localhost:8000${RESET}"
log "Run E2E tests with '${GREEN}npm run playwright:test${RESET}'"
