#!/usr/bin/env bash

set -euo pipefail

RED='\033[31m'
GREEN='\033[32m'
YELLOW='\033[33m'
PURPLE='\033[35m'
PINK='\033[95m'
GREY='\033[90m'
CYAN='\033[36m'
BLUE='\033[34m'
RESET='\033[0m'

SUPABASE_REPO="https://github.com/supabase/supabase"
BRANCH="master"
SPARSE_PATH="docker"
SUPABASE_DIR="supabase"

log() { echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${PINK}[+]${RESET} $1"; }
warn() { echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${YELLOW}[!]${RESET} $1"; }
error() { echo -e "${GREY}[$(date +"%Y-%m-%d %H:%M:%S")]${RESET} ${RED}[✗]${RESET} $1" >&2; }

log "Cloning Supabase repository..."
if [ -d "$SUPABASE_DIR" ]; then
  warn "Directory '$SUPABASE_DIR' exists. Removing..."
  cd "$SUPABASE_DIR/$SPARSE_PATH"
  docker compose down -v
  cd ../..
  sudo rm -rf "$SUPABASE_DIR"
fi

git clone --filter=blob:none --no-checkout "$SUPABASE_REPO" "$SUPABASE_DIR"

cd "$SUPABASE_DIR"

log "Enabling sparse-checkout (cone mode)..."
git sparse-checkout set --cone "$SPARSE_PATH"

log "Checking out $BRANCH..."
git checkout "$BRANCH"

cd "$SPARSE_PATH"

log "Copying .env.example → .env..."
if [ ! -f ".env.example" ]; then
  error ".env.example not found!"
  exit 1
fi
cp .env.example .env

log "Build logflare image..."
docker compose -f docker-compose.yml -f ../../docker-compose.e2e.yml build analytics

log "Starting Supabase stack (inside docker containers)..."
if ! docker compose -f docker-compose.yml -f ../../docker-compose.e2e.yml up -d; then
  error "Failed to start containers!"
  docker compose -f docker-compose.yml -f ../../docker-compose.e2e.yml logs
  exit 1
fi

log "Supabase stack is up! Access Supabase studio via ${CYAN}http://localhost:8000${RESET}"
log "Run E2E tests with '${GREEN}npm run playwright:test${RESET}'"
