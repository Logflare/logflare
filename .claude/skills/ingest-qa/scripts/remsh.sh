#!/usr/bin/env bash
# Usage: remsh.sh <script.exs> [KEY=VALUE ...]
# Pipes an .exs file into `iex --remsh` on the QA node, replacing {{KEY}} placeholders.
# The session ends by halting the local probe node: EOF on a piped --remsh session
# stops the remote node too.
set -euo pipefail
source "$(dirname "$0")/env.sh"

probe="ingest_qa_probe_$$"
script=$(cat "$1")
shift

for kv in "SKILL_DIR=$(cd "$(dirname "$0")/.." && pwd)" "$@"; do
  script=${script//"{{${kv%%=*}}}"/${kv#*=}}
done

{
  printf '%s\n' "$script"
  printf ':rpc.cast(:"%s@%s", :erlang, :halt, [])\nProcess.sleep(5_000)\n' "$probe" "$QA_HOST"
} | (cd /tmp && timeout 300 iex --sname "$probe" --cookie "$QA_COOKIE" --remsh "$QA_NODE@$QA_HOST") 2>&1
