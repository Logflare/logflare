#!/usr/bin/env bash
# Usage: remsh.sh <script.exs> [KEY=VALUE ...]
# Pipes an .exs file into `iex --remsh` on the node that server.sh started, replacing
# {{KEY}} placeholders. {{SCRIPT_DIR}} is always set to the directory of the .exs file.
# The session ends by halting the local probe node: EOF on a piped --remsh session
# stops the remote node too.
set -euo pipefail
source "$(dirname "$0")/env.sh"

probe="qa_probe_$$"
script=$(cat "$1")
script_dir=$(cd "$(dirname "$1")" && pwd)
shift

for kv in "SCRIPT_DIR=$script_dir" "$@"; do
  script=${script//"{{${kv%%=*}}}"/${kv#*=}}
done

{
  printf '%s\n' "$script"
  printf ':rpc.cast(:"%s@%s", :erlang, :halt, [])\nProcess.sleep(5_000)\n' "$probe" "$QA_HOST"
} | (cd /tmp && timeout 300 iex --sname "$probe" --cookie "$QA_COOKIE" --remsh "$QA_NODE@$QA_HOST") 2>&1
