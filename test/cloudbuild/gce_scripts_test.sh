#!/bin/bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPO_ROOT
readonly STARTUP_SCRIPT="${REPO_ROOT}/cloudbuild/gce-startup.sh"
readonly CREATE_SCRIPT="${REPO_ROOT}/cloudbuild/create-instance-template.sh"
readonly TEMPLATE_SCRIPT="${REPO_ROOT}/scripts/create-instance-templates.sh"
TEST_DIR="$(mktemp -d)"
readonly TEST_DIR

trap 'rm -rf "${TEST_DIR}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local expected="$2"

  grep -Fq -- "${expected}" "${file}" || fail "${file} does not contain: ${expected}"
}

assert_not_contains() {
  local file="$1"
  local unexpected="$2"

  if grep -Fq -- "${unexpected}" "${file}"; then
    fail "${file} unexpectedly contains: ${unexpected}"
  fi
}

install_startup_stubs() {
  local bin_dir="$1"

  mkdir -p "${bin_dir}"

  cat >"${bin_dir}/curl" <<'STUB'
#!/bin/bash
url="${!#}"
printf 'curl %s\n' "${url}" >>"${CALL_LOG}"
case "${url}" in
  */logflare-container-image)
    printf '%s' 'gcr.io/test-project/logflare_app:test-tag'
    ;;
  */logflare-container-env)
    printf '%s\n' \
      'LOGFLARE_GRPC_PORT=50051' \
      'RELEASE_COOKIE=test-cookie' \
      'LOGFLARE_METADATA_CLUSTER=test-cluster'
    ;;
  *)
    exit 22
    ;;
esac
STUB

  cat >"${bin_dir}/docker-credential-gcr" <<'STUB'
#!/bin/bash
printf 'credential HOME=%s %s\n' "${HOME}" "$*" >>"${CALL_LOG}"
STUB

  cat >"${bin_dir}/iptables" <<'STUB'
#!/bin/bash
printf 'iptables %s\n' "$*" >>"${CALL_LOG}"
rule="$2 $4"
case "$1" in
  -C)
    grep -Fxq -- "${rule}" "${IPTABLES_STATE}" 2>/dev/null
    ;;
  -A)
    printf '%s\n' "${rule}" >>"${IPTABLES_STATE}"
    ;;
  *)
    exit 2
    ;;
esac
STUB

  cat >"${bin_dir}/docker" <<'STUB'
#!/bin/bash
printf 'docker %s\n' "$*" >>"${CALL_LOG}"
if [[ "$1" == "pull" && "${DOCKER_PULL_FAIL:-false}" == "true" ]]; then
  exit 1
fi
STUB

  chmod +x "${bin_dir}"/*
}

run_startup_script() {
  local call_log="$1"
  local iptables_state="$2"
  local docker_home="$3"
  local env_file="$4"
  local pull_fail="${5:-false}"

  CALL_LOG="${call_log}" \
    IPTABLES_STATE="${iptables_state}" \
    DOCKER_PULL_FAIL="${pull_fail}" \
    LOGFLARE_DOCKER_HOME="${docker_home}" \
    LOGFLARE_CONTAINER_ENV_FILE="${env_file}" \
    PATH="${TEST_DIR}/startup-bin:${PATH}" \
    bash "${STARTUP_SCRIPT}"
}

test_startup_script() {
  local call_log="${TEST_DIR}/startup-calls"
  local iptables_state="${TEST_DIR}/iptables-state"
  local docker_home="${TEST_DIR}/docker-home"
  local env_file="${TEST_DIR}/container.env"
  local first_add_count
  local pull_line
  local remove_line

  : >"${call_log}"
  : >"${iptables_state}"
  install_startup_stubs "${TEST_DIR}/startup-bin"

  run_startup_script "${call_log}" "${iptables_state}" "${docker_home}" "${env_file}"

  [[ "$(stat -c '%a' "${env_file}")" == "600" ]] || fail "container env file is not mode 600"
  assert_contains "${env_file}" "RELEASE_COOKIE=test-cookie"
  assert_contains "${call_log}" "credential HOME=${docker_home} configure-docker --registries=gcr.io"
  assert_contains "${call_log}" "docker run --name=logflare --privileged --restart=always --network=host --detach --log-driver=json-file --log-opt max-size=500m --log-opt max-file=3 --env-file ${env_file} gcr.io/test-project/logflare_app:test-tag"

  first_add_count="$(grep -c '^iptables -A ' "${call_log}")"
  [[ "${first_add_count}" == "6" ]] || fail "expected six firewall rules, got ${first_add_count}"

  pull_line="$(grep -n '^docker pull ' "${call_log}" | head -1 | cut -d: -f1)"
  remove_line="$(grep -n '^docker rm ' "${call_log}" | head -1 | cut -d: -f1)"
  ((pull_line < remove_line)) || fail "container was removed before the image pull completed"

  run_startup_script "${call_log}" "${iptables_state}" "${docker_home}" "${env_file}"
  [[ "$(grep -c '^iptables -A ' "${call_log}")" == "6" ]] || fail "firewall rules were duplicated on rerun"

  : >"${call_log}"
  if run_startup_script "${call_log}" "${iptables_state}" "${docker_home}" "${env_file}" true; then
    fail "startup script succeeded when docker pull failed"
  fi
  assert_not_contains "${call_log}" "docker rm -f logflare"
}

install_gcloud_stub() {
  local path="$1"

  cat >"${path}" <<'STUB'
#!/bin/bash
printf 'gcloud %s\n' "$*" >>"${CALL_LOG}"
if [[ "$3" == "describe" ]]; then
  case "${GCLOUD_DESCRIBE_MODE}" in
    missing)
      exit 1
      ;;
    good)
      printf '%s\n' '{"properties":{"metadata":{"items":[{"key":"startup-script"},{"key":"shutdown-script"},{"key":"logflare-container-env"},{"key":"logflare-container-image"}]}}}'
      ;;
    deprecated)
      printf '%s\n' '{"properties":{"metadata":{"items":[{"key":"gce-container-declaration"}]}}}'
      ;;
    incomplete)
      printf '%s\n' '{"properties":{"metadata":{"items":[{"key":"startup-script"}]}}}'
      ;;
  esac
elif [[ "$3" == "create" && -n "${ENV_CAPTURE_DIR:-}" ]]; then
  template="$4"
  for arg in "$@"; do
    case "${arg}" in
      --metadata-from-file=*logflare-container-env=*)
        env_file="${arg##*logflare-container-env=}"
        cp "${env_file}" "${ENV_CAPTURE_DIR}/${template}.env"
        ;;
    esac
  done
fi
STUB
  chmod +x "${path}"
}

run_create_script() {
  local mode="$1"
  local call_log="$2"

  CALL_LOG="${call_log}" \
    GCLOUD_DESCRIBE_MODE="${mode}" \
    GCLOUD_BIN="${TEST_DIR}/gcloud" \
    bash "${CREATE_SCRIPT}" test-project test-template --machine-type=test-machine
}

test_create_script() {
  local call_log="${TEST_DIR}/gcloud-calls"

  install_gcloud_stub "${TEST_DIR}/gcloud"

  : >"${call_log}"
  run_create_script missing "${call_log}"
  assert_contains "${call_log}" "gcloud compute instance-templates create test-template --project=test-project --machine-type=test-machine"

  : >"${call_log}"
  run_create_script good "${call_log}"
  assert_not_contains "${call_log}" "instance-templates create"

  : >"${call_log}"
  if run_create_script deprecated "${call_log}"; then
    fail "deprecated existing template was accepted"
  fi

  : >"${call_log}"
  if run_create_script incomplete "${call_log}"; then
    fail "incomplete existing template was accepted"
  fi
}

test_manual_template_script() {
  local call_log="${TEST_DIR}/manual-gcloud-calls"
  local capture_dir="${TEST_DIR}/captured-env"
  local output="${TEST_DIR}/manual-output"

  mkdir -p "${capture_dir}"
  : >"${call_log}"

  CALL_LOG="${call_log}" \
    ENV_CAPTURE_DIR="${capture_dir}" \
    GCLOUD_DESCRIBE_MODE=missing \
    GCLOUD_BIN="${TEST_DIR}/gcloud" \
    bash "${TEMPLATE_SCRIPT}" 1.2.3 staging >"${output}"

  assert_contains "${output}" "Created        : 2"
  assert_contains "${capture_dir}/logflare-staging-main-cluster-1-2-3.env" "RELEASE_COOKIE=default-main"
  assert_contains "${capture_dir}/logflare-staging-versioned-cluster-1-2-3.env" "LOGFLARE_METADATA_CLUSTER=versioned"
  assert_contains "${call_log}" "logflare-container-image=gcr.io/logflare-staging/logflare_app:1.2.3"
  assert_not_contains "${call_log}" "create-with-container"

  rm -rf "${capture_dir}"
  mkdir -p "${capture_dir}"
  : >"${call_log}"

  CALL_LOG="${call_log}" \
    ENV_CAPTURE_DIR="${capture_dir}" \
    GCLOUD_DESCRIBE_MODE=missing \
    GCLOUD_BIN="${TEST_DIR}/gcloud" \
    bash "${TEMPLATE_SCRIPT}" 1.2.3 >"${output}"

  assert_contains "${output}" "Created        : 8"
  assert_contains "${capture_dir}/logflare-prod-1-2-3-prod-a.env" "LOGFLARE_ALERTS_ENABLED=true"
  assert_contains "${capture_dir}/logflare-prod-1-2-3-prod-b.env" "LOGFLARE_ALERTS_ENABLED=false"
}

test_startup_script
test_create_script
test_manual_template_script

echo "gce script tests passed"
