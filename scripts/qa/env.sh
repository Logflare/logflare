# Shared settings for the QA scripts used by the ingest-qa and ui-qa skills.
# Source this file; override any value through the environment.
export LC_ALL=C.UTF-8
REPO_ROOT=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
export REPO_ROOT
export QA_NODE=${QA_NODE:-logflare_qa}
export QA_COOKIE=${QA_COOKIE:-logflare_qa}
export QA_HOST=${QA_HOST:-$(hostname -s)}
export QA_PORT=${QA_PORT:-4000}
export QA_GRPC_PORT=${QA_GRPC_PORT:-50051}
export QA_PUBLIC_TOKEN=${QA_PUBLIC_TOKEN:-logflare-qa-public-token}
export QA_BACKEND_URL=${QA_BACKEND_URL:-postgresql://postgres:postgres@localhost:5432/logflare_qa_backend}
export LOGFLARE_URL=${LOGFLARE_URL:-http://localhost:$QA_PORT}

if command -v mise >/dev/null 2>&1; then
  for tool in erlang elixir node; do
    tool_dir=$(cd "$REPO_ROOT" && mise where "$tool" 2>/dev/null) && PATH="$tool_dir/bin:$PATH"
  done
  export PATH
fi
