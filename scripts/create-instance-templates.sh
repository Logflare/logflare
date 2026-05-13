#!/usr/bin/env bash
# Create GCP instance templates for a given Logflare version.
#
# Usage: ./scripts/create-instance-templates.sh <version> [prod|staging]
# Example: ./scripts/create-instance-templates.sh 1.2.3
#          ./scripts/create-instance-templates.sh 1.2.3 staging
#
# ENV defaults to "prod". The release cookie is derived per cluster as
# "default-<cluster>", matching the _COOKIE substitution in the cloudbuild yamls.
#
# Idempotent: templates that already exist are reported but do not cause failure.

set -euo pipefail

# ── Argument validation ───────────────────────────────────────────────────────

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <version> [prod|staging]" >&2
  echo "Example: $0 1.2.3" >&2
  echo "         $0 1.2.3 staging" >&2
  exit 1
fi

VERSION="$1"
ENV="${2:-prod}"

if [[ "${ENV}" != "prod" && "${ENV}" != "staging" ]]; then
  echo "Error: ENV must be 'prod' or 'staging', got '${ENV}'" >&2
  exit 1
fi

NORMALIZED_VERSION="${VERSION//./-}"

# ── Environment-specific config ───────────────────────────────────────────────

if [[ "${ENV}" == "prod" ]]; then
  PROJECT="logflare-232118"
  MACHINE_TYPE="c2d-highcpu-32"
  SERVICE_ACCOUNT="compute-engine-2022@logflare-232118.iam.gserviceaccount.com"
  NETWORK_INTERFACE="network=global,network-tier=PREMIUM,no-address"
  CLUSTERS=(canary prod-a prod-b prod-c prod-d prod-e prod-f prod-g)
else
  PROJECT="logflare-staging"
  MACHINE_TYPE="c2d-highcpu-4"
  SERVICE_ACCOUNT="compute-engine-2022@logflare-staging.iam.gserviceaccount.com"
  NETWORK_INTERFACE="network=default,network-tier=PREMIUM,no-address"
  CLUSTERS=(main versioned)
fi

CONTAINER_IMAGE="gcr.io/${PROJECT}/logflare_app:${VERSION}"

echo "Creating instance templates for version ${VERSION} (normalized: ${NORMALIZED_VERSION})"
echo "Env     : ${ENV}"
echo "Project : ${PROJECT}"
echo "Image   : ${CONTAINER_IMAGE}"
echo ""

# ── Create loop ───────────────────────────────────────────────────────────────

created=()
already_exists=()
errored=()

for cluster in "${CLUSTERS[@]}"; do
  # Template name pattern differs per env (matches cloudbuild substitutions)
  if [[ "${ENV}" == "prod" ]]; then
    template="logflare-prod-${NORMALIZED_VERSION}-${cluster}"
  else
    template="logflare-staging-${cluster}-cluster-${NORMALIZED_VERSION}"
  fi

  # Cookie derived per cluster: "default-<cluster>" (matches _COOKIE in cloudbuild yamls)
  cluster_cookie="default-${cluster}"

  # Container env differs per env
  if [[ "${ENV}" == "prod" ]]; then
    # Only prod-a has alerting enabled (matches deploy.prod.versioned in Makefile)
    if [[ "${cluster}" == "prod-a" ]]; then
      alerts_enabled="true"
    else
      alerts_enabled="false"
    fi
    container_env="LOGFLARE_GRPC_PORT=50051,LOGFLARE_MIN_CLUSTER_SIZE=2,RELEASE_COOKIE=${cluster_cookie},LOGFLARE_PUBSUB_POOL_SIZE=32,LOGFLARE_METADATA_CLUSTER=${cluster},LOGFLARE_ALERTS_ENABLED=${alerts_enabled}"
  else
    container_env="LOGFLARE_GRPC_PORT=50051,RELEASE_COOKIE=${cluster_cookie},LOGFLARE_METADATA_CLUSTER=${cluster}"
  fi

  printf "  %-55s " "${template}"

  err_output=$(gcloud compute instance-templates create-with-container "${template}" \
    --project="${PROJECT}" \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-balanced \
    --machine-type="${MACHINE_TYPE}" \
    --network-interface="${NETWORK_INTERFACE}" \
    --maintenance-policy=TERMINATE \
    --service-account="${SERVICE_ACCOUNT}" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=phoenix-http,https-server \
    --metadata-from-file=shutdown-script=./cloudbuild/shutdown.sh \
    --metadata=google-monitoring-enabled=true,google-logging-enabled=true \
    --container-image="${CONTAINER_IMAGE}" \
    --container-privileged \
    --container-restart-policy=always \
    --container-env="${container_env}" \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --image=cos-stable-109-17800-147-54 \
    --image-project=cos-cloud \
    2>&1) && rc=0 || rc=$?

  if [[ $rc -eq 0 ]]; then
    echo "created"
    created+=("${template}")
  elif echo "${err_output}" | grep -qiE "already exists|alreadyExists"; then
    echo "already exists (skipped)"
    already_exists+=("${template}")
  else
    echo "ERROR"
    echo "    ${err_output}" >&2
    errored+=("${template}")
  fi
done

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "Summary"
echo "───────────────────────────────────────────────────"
echo "  Created        : ${#created[@]}"
echo "  Already existed: ${#already_exists[@]}"
echo "  Errors         : ${#errored[@]}"

if [[ ${#errored[@]} -gt 0 ]]; then
  echo ""
  echo "The following templates encountered errors:" >&2
  for t in "${errored[@]}"; do
    echo "  - ${t}" >&2
  done
  exit 1
fi
