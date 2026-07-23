#!/bin/bash

set -euo pipefail

readonly METADATA_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
readonly CONTAINER_NAME="logflare"
readonly CONTAINER_ENV_FILE="${LOGFLARE_CONTAINER_ENV_FILE:-/run/logflare-container.env}"
readonly DOCKER_HOME="${LOGFLARE_DOCKER_HOME:-/home/logflare}"

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

  image="$(metadata logflare-container-image)"
  if [[ -z "${image}" ]]; then
    echo "logflare-container-image metadata is empty" >&2
    return 1
  fi

  metadata logflare-container-env >"${CONTAINER_ENV_FILE}"
  if [[ ! -s "${CONTAINER_ENV_FILE}" ]]; then
    echo "logflare-container-env metadata is empty" >&2
    return 1
  fi

  export HOME="${DOCKER_HOME}"
  install -d -m 0700 "${HOME}"
  docker-credential-gcr configure-docker --registries=gcr.io

  configure_firewall
  wait_for_docker

  # Keep the existing container available if the registry is temporarily
  # unavailable during a manual startup-script rerun.
  docker pull "${image}"
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
}

main "$@"
