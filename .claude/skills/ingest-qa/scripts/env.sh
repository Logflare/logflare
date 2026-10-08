# Shared settings for the ingest-qa scripts. Source this file; override any value via env.
export LC_ALL=C.UTF-8
REPO_ROOT=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
export REPO_ROOT
export QA_NODE=${QA_NODE:-ingest_qa}
export QA_COOKIE=${QA_COOKIE:-ingest_qa}
export QA_HOST=${QA_HOST:-$(hostname -s)}
export QA_PORT=${QA_PORT:-4000}
export QA_GRPC_PORT=${QA_GRPC_PORT:-50051}
export QA_PUBLIC_TOKEN=${QA_PUBLIC_TOKEN:-ingest-qa-public-token}
export QA_BACKEND_URL=${QA_BACKEND_URL:-postgresql://postgres:postgres@localhost:5432/logflare_ingest_qa}

if command -v mise >/dev/null 2>&1; then
  for tool in erlang elixir node; do
    tool_dir=$(cd "$REPO_ROOT" && mise where "$tool" 2>/dev/null) && PATH="$tool_dir/bin:$PATH"
  done
  export PATH
fi
