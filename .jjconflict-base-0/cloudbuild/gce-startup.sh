#!/bin/bash

set -Eeuo pipefail

readonly METADATA_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
readonly CONTAINER_NAME="logflare"
readonly CONTAINER_ENV_FILE="${LOGFLARE_CONTAINER_ENV_FILE:-/run/logflare-container.env}"
readonly DOCKER_HOME="${LOGFLARE_DOCKER_HOME:-/home/logflare}"

# Tracks the current step so an unexpected failure reports where it happened.
# Every phase is logged with elapsed seconds, which is what distinguishes a
# script that never ran from one that stalled (e.g. on a metadata retry).
CURRENT_PHASE="startup"

log() {
  echo "gce-startup[+${SECONDS}s] $*"
}

phase() {
  CURRENT_PHASE="$1"

  log "phase: ${CURRENT_PHASE}"
}

report_failure() {
  local status="$1"
  local line="$2"
  local command="$3"

  log "FAILED during phase: ${CURRENT_PHASE} (line ${line}, exit ${status}): ${command}" >&2
}

trap 'report_failure "$?" "$LINENO" "$BASH_COMMAND"' ERR

metadata() {
  local key="$1"

  curl \
    --fail \
    --silent \
    --show-error \
    --connect-timeout 5 \
    --max-time 30 \
    --retry 10 \
    --retry-connrefused \
    --retry-delay 2 \
    --header "Metadata-Flavor: Google" \
    "${METADATA_URL}/${key}"
}

wait_for_docker() {
  local attempt

  for attempt in {1..30}; do
    if docker info >/dev/null 2>&1; then
      log "docker ready after ${attempt} attempt(s)"
      return 0
    fi

    sleep 2
  done

  echo "Docker did not become ready after 60 seconds" >&2
  return 1
}

configure_firewall() {
  local chain
  local protocol

  # Konlet opened these host firewall paths before starting the container.
  # Keep the rules idempotent because startup scripts can be rerun.
  for protocol in tcp udp icmp; do
    for chain in INPUT FORWARD; do
      if ! iptables -C "${chain}" -p "${protocol}" -j ACCEPT 2>/dev/null; then
        iptables -A "${chain}" -p "${protocol}" -j ACCEPT
      fi
    done
  done
}

main() {
  local image

  umask 077

  log "begin"

  phase "read container image metadata"
  image="$(metadata logflare-container-image)"
  if [[ -z "${image}" ]]; then
    echo "logflare-container-image metadata is empty" >&2
    return 1
  fi
  log "container image: ${image}"

  phase "write container env file"
  metadata logflare-container-env >"${CONTAINER_ENV_FILE}"
  if [[ ! -s "${CONTAINER_ENV_FILE}" ]]; then
    echo "logflare-container-env metadata is empty" >&2
    return 1
  fi

  phase "configure docker credentials"
  export HOME="${DOCKER_HOME}"
  mkdir -p "${HOME}"
  chmod 0700 "${HOME}"
  docker-credential-gcr configure-docker --registries=gcr.io

  phase "configure host firewall"
  configure_firewall

  phase "wait for docker"
  wait_for_docker

  # Keep the existing container available if the registry is temporarily
  # unavailable during a manual startup-script rerun.
  phase "pull container image"
  docker pull "${image}"

  phase "start container"
  docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

  docker run \
    --name="${CONTAINER_NAME}" \
    --privileged \
    --restart=always \
    --network=host \
    --detach \
    --log-driver=json-file \
    --log-opt max-size=500m \
    --log-opt max-file=3 \
    --env-file "${CONTAINER_ENV_FILE}" \
    "${image}"

  phase "done"
}

main "$@"
