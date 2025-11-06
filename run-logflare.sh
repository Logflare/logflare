set -o allexport
source "supabase-project/.env"
set +o allexport

export POSTGRES_HOST=localhost
export POSTGRES_PORT=7654

export LOGFLARE_NODE_HOST=127.0.0.1
export DB_USERNAME=supabase_admin
export DB_DATABASE=_supabase
export DB_HOSTNAME=${POSTGRES_HOST}
export DB_PORT=${POSTGRES_PORT}
export DB_PASSWORD=${POSTGRES_PASSWORD}
export DB_SCHEMA=_analytics
export LOGFLARE_PUBLIC_ACCESS_TOKEN=${LOGFLARE_PUBLIC_ACCESS_TOKEN}
export LOGFLARE_PRIVATE_ACCESS_TOKEN=${LOGFLARE_PRIVATE_ACCESS_TOKEN}
export LOGFLARE_SINGLE_TENANT=true
export LOGFLARE_SUPABASE_MODE=true
export POSTGRES_BACKEND_URL="postgresql://supabase_admin:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/_supabase"
export POSTGRES_BACKEND_SCHEMA=_analytics
export LOGFLARE_FEATURE_FLAG_OVERRIDE="multibackend=true"

# curl -X GET http://localhost:8000/api/cli-release-version
mix do ecto.create, ecto.migrate
mix phx.server
