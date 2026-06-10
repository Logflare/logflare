#!/usr/bin/env bash

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
GITHUB_ACTIONS="${GITHUB_ACTIONS:-false}"

# Logflare backend for the analytics service: "postgres" (default) or
# "bigquery". BigQuery mode layers docker-compose.e2e.bigquery.yml and requires
# GOOGLE_CREDENTIALS_JSON (service account key contents) at setup time.
# GOOGLE_PROJECT_ID, GOOGLE_PROJECT_NUMBER and GOOGLE_DATASET_ID_APPEND default
# to the CI project/dataset in that overlay and can be overridden via the
# environment.
LOGFLARE_BACKEND="${LOGFLARE_BACKEND:-postgres}"
