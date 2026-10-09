#!/usr/bin/env bash
# Starts the dev server in single-tenant Postgres mode as a named node, in the foreground.
# Run it in the background and redirect output to a log file. Used by the ingest-qa and
# ui-qa skills; scripts/qa/remsh.sh connects to the node it starts.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$REPO_ROOT"

backend_db=${QA_BACKEND_URL##*/}
if command -v psql >/dev/null 2>&1; then
  psql "${QA_BACKEND_URL%/*}/postgres" -Atc "select 1 from pg_database where datname = '$backend_db'" | grep -q 1 ||
    psql "${QA_BACKEND_URL%/*}/postgres" -qc "create database \"$backend_db\""
fi

MIX_ENV=dev mix do ecto.create --quiet + ecto.migrate --quiet

LOGFLARE_SINGLE_TENANT=true \
  POSTGRES_BACKEND_URL="$QA_BACKEND_URL" \
  LOGFLARE_PUBLIC_ACCESS_TOKEN="$QA_PUBLIC_TOKEN" \
  GOOGLE_PROJECT_ID=logflare-qa \
  PHX_HTTP_PORT="$QA_PORT" \
  LOGFLARE_GRPC_PORT="$QA_GRPC_PORT" \
  MIX_ENV=dev \
  exec elixir --sname "$QA_NODE" --cookie "$QA_COOKIE" -S mix phx.server
