#!/bin/bash
# Only the container's PID 1, the shell running run.sh, gets SIGTERM, so run.sh must exec the release.
set -eu

dir="$(mktemp -d)"
cp "$(dirname "$0")/../../run.sh" "${dir}"
cat > "${dir}/logflare" <<'STUB'
#!/bin/sh
[ "$1" = start ] || exit 0
trap 'touch sigterm; exit 0' TERM
touch started
sleep 5 & wait
STUB
chmod +x "${dir}/logflare"

wait_for() {
  for _ in $(seq 50); do
    [ -f "${dir}/$1" ] && return
    sleep 0.1
  done
  echo "FAIL: timed out waiting for the release to create $1" >&2
  exit 1
}

(cd "${dir}" && exec sh run.sh) &
wait_for started
kill -TERM $!
wait_for sigterm
echo "sigterm test passed"
