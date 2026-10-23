#!/bin/bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPO_ROOT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local expected="$2"

  grep -Fq -- "${expected}" "${file}" || fail "${file} does not contain: ${expected}"
}

wait_for_file() {
  local file="$1"

  for _ in $(seq 1 50); do
    [ -s "${file}" ] && return 0
    sleep 0.1
  done

  return 1
}

# Runs the real run.sh against a stub release that records its PID and whether
# it got SIGTERM. Kubernetes and Docker only signal the container's PID 1, which
# is the shell running run.sh, so the release has to replace that shell.
test_run_sh_hands_sigterm_to_the_release() {
  local dir
  local run_sh_pid
  local release_pid

  dir="$(mktemp -d)"
  trap 'kill "${run_sh_pid:-}" "${release_pid:-}" 2>/dev/null || true; rm -rf "${dir}"' RETURN

  cp "${REPO_ROOT}/run.sh" "${dir}/run.sh"
  cat > "${dir}/logflare" <<'STUB'
#!/bin/sh
case "$1" in
  eval) exit 0 ;;
  start)
    echo $$ > release.pid
    trap 'echo sigterm > release.sigterm; exit 0' TERM
    while :; do sleep 0.1; done
    ;;
esac
STUB
  chmod +x "${dir}/logflare"

  (cd "${dir}" && exec sh run.sh > run.log 2>&1) &
  run_sh_pid=$!

  wait_for_file "${dir}/release.pid" || fail "run.sh never started the release"
  release_pid="$(cat "${dir}/release.pid")"

  [ "${release_pid}" = "${run_sh_pid}" ] ||
    fail "release runs as PID ${release_pid}, not as run.sh's PID ${run_sh_pid}: run.sh must exec it"

  kill -TERM "${run_sh_pid}"

  wait_for_file "${dir}/release.sigterm" || fail "SIGTERM sent to run.sh never reached the release"
}

test_images_run_under_tini() {
  local dockerfile

  for dockerfile in "${REPO_ROOT}/Dockerfile" "${REPO_ROOT}/Dockerfile.runner"; do
    assert_contains "${dockerfile}" " tini"
    assert_contains "${dockerfile}" 'ENTRYPOINT ["/usr/bin/tini", "--"]'
  done
}

test_run_sh_hands_sigterm_to_the_release
test_images_run_under_tini

echo "sigterm tests passed"
