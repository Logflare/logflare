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
