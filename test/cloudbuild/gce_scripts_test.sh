#!/bin/bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPO_ROOT
readonly STARTUP_SCRIPT="${REPO_ROOT}/cloudbuild/gce-startup.sh"
readonly CREATE_SCRIPT="${REPO_ROOT}/cloudbuild/create-instance-template.sh"
readonly PROD_CONFIG="${REPO_ROOT}/cloudbuild/prod/pre-deploy.yaml"
readonly STAGING_CONFIG="${REPO_ROOT}/cloudbuild/staging/deploy.yaml"
readonly TEMPLATE_SCRIPT="${REPO_ROOT}/scripts/create-instance-templates.sh"

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

test_startup_script_contract() {
  local required

  for required in \
    "logflare-container-image" \
    "logflare-container-env" \
    "docker-credential-gcr configure-docker" \
    "iptables -C" \
    "iptables -A" \
    "docker run" \
    "--privileged" \
    "--restart=always" \
    "--network=host" \
    "--log-driver=json-file" \
    "--log-opt max-size=500m" \
    "--log-opt max-file=3" \
    "--env-file"; do
    assert_contains "${STARTUP_SCRIPT}" "${required}"
  done
}

test_instance_template_contract() {
  local config
  local metadata_key

  assert_contains "${CREATE_SCRIPT}" "compute instance-templates create"

  for config in "${PROD_CONFIG}" "${STAGING_CONFIG}" "${TEMPLATE_SCRIPT}"; do
    assert_contains "${config}" "create-instance-template.sh"
    assert_contains "${config}" "--metadata-from-file="
    assert_contains "${config}" "--metadata="
    assert_not_contains "${config}" "create-with-container"

    for metadata_key in \
      "startup-script" \
      "shutdown-script" \
      "logflare-container-env" \
      "logflare-container-image"; do
      assert_contains "${config}" "${metadata_key}"
    done
  done
}

test_startup_script_contract
test_instance_template_contract

echo "gce script contract tests passed"
