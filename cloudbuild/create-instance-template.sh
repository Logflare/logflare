#!/bin/bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 PROJECT TEMPLATE [GCLOUD_CREATE_ARGS...]" >&2
  exit 2
fi

readonly PROJECT="$1"
readonly TEMPLATE="$2"
readonly GCLOUD_BIN="${GCLOUD_BIN:-gcloud}"
shift 2

metadata_has_key() {
  local description="$1"
  local key="$2"

  grep -Eq "\"key\"[[:space:]]*:[[:space:]]*\"${key}\"" <<<"${description}"
}

if description="$("${GCLOUD_BIN}" compute instance-templates describe "${TEMPLATE}" \
  --project="${PROJECT}" \
  --format=json 2>/dev/null)"; then
  if metadata_has_key "${description}" "gce-container-declaration"; then
    echo "Existing template ${TEMPLATE} still uses gce-container-declaration" >&2
    exit 1
  fi

  for key in startup-script shutdown-script logflare-container-env logflare-container-image; do
    if ! metadata_has_key "${description}" "${key}"; then
      echo "Existing template ${TEMPLATE} is missing ${key} metadata" >&2
      exit 1
    fi
  done

  echo "Instance template already exists with startup-script metadata: ${TEMPLATE}"
  exit 0
fi

"${GCLOUD_BIN}" compute instance-templates create "${TEMPLATE}" \
  --project="${PROJECT}" \
  "$@"
