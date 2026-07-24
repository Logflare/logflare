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

# Expected snippets intentionally include literal shell substitutions.
# shellcheck disable=SC2016
test_startup_script_config() {
  local option

  assert_contains "${STARTUP_SCRIPT}" 'image="$(metadata logflare-container-image)"'
  assert_contains "${STARTUP_SCRIPT}" 'metadata logflare-container-env >"${CONTAINER_ENV_FILE}"'
  assert_contains "${STARTUP_SCRIPT}" "docker-credential-gcr configure-docker --registries=gcr.io"
  assert_contains "${STARTUP_SCRIPT}" 'iptables -C "${chain}" -p "${protocol}" -j ACCEPT'
  assert_contains "${STARTUP_SCRIPT}" 'iptables -A "${chain}" -p "${protocol}" -j ACCEPT'

  for option in \
    '--privileged' \
    '--restart=always' \
    '--network=host' \
    '--log-driver=json-file' \
    '--log-opt max-size=500m' \
    '--log-opt max-file=3' \
    '--env-file "${CONTAINER_ENV_FILE}"'; do
    assert_contains "${STARTUP_SCRIPT}" "${option}"
  done
}

# shellcheck disable=SC2016
test_instance_template_config() {
  local config

  assert_contains "${CREATE_SCRIPT}" '"${GCLOUD_BIN}" compute instance-templates create "${TEMPLATE}"'
  assert_contains "${CREATE_SCRIPT}" 'metadata_has_key "${description}" "gce-container-declaration"'

  for config in "${PROD_CONFIG}" "${STAGING_CONFIG}"; do
    assert_contains "${config}" "./cloudbuild/create-instance-template.sh"
    assert_contains "${config}" "> gce-container.env"
    assert_contains "${config}" "--metadata-from-file=startup-script=./cloudbuild/gce-startup.sh,shutdown-script=./cloudbuild/shutdown.sh,logflare-container-env=./gce-container.env"
    assert_contains "${config}" '--metadata=google-monitoring-enabled=true,google-logging-enabled=true,logflare-container-image=${_CONTAINER_IMAGE}'
  done

  assert_contains "${TEMPLATE_SCRIPT}" '"${REPO_ROOT}/cloudbuild/create-instance-template.sh"'
  assert_contains "${TEMPLATE_SCRIPT}" '--metadata-from-file="startup-script=${REPO_ROOT}/cloudbuild/gce-startup.sh,shutdown-script=${REPO_ROOT}/cloudbuild/shutdown.sh,logflare-container-env=${container_env_file}"'
  assert_contains "${TEMPLATE_SCRIPT}" '--metadata="google-monitoring-enabled=true,google-logging-enabled=true,logflare-container-image=${CONTAINER_IMAGE}"'

  for config in "${PROD_CONFIG}" "${STAGING_CONFIG}" "${TEMPLATE_SCRIPT}"; do
    assert_not_contains "${config}" "create-with-container"
  done
}

test_startup_script_config
test_instance_template_config

echo "gce script config tests passed"
