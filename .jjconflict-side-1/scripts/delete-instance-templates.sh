#!/usr/bin/env bash
# Delete all GCP instance templates for a given Logflare version.
#
# Usage: ./scripts/delete_instance_templates.sh <version>
# Example: ./scripts/delete_instance_templates.sh 1.2.3
#
# Targets prod clusters (prod-a through prod-g) and canary.
# Idempotent: templates that don't exist are reported but do not cause failure.

set -euo pipefail

PROJECT="logflare-232118"
CLUSTERS=(canary prod-a prod-b prod-c prod-d prod-e prod-f prod-g)

# ── Argument validation ───────────────────────────────────────────────────────

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  echo "Example: $0 1.2.3" >&2
  exit 1
fi

VERSION="$1"
NORMALIZED_VERSION="${VERSION//./-}"

echo "Deleting instance templates for version ${VERSION} (normalized: ${NORMALIZED_VERSION})"
echo "Project: ${PROJECT}"
echo ""

# ── Delete loop ───────────────────────────────────────────────────────────────

deleted=()
not_found=()
errored=()

for cluster in "${CLUSTERS[@]}"; do
  template="logflare-prod-${NORMALIZED_VERSION}-${cluster}"
  printf "  %-50s " "${template}"

  # Capture stderr; gcloud exits 1 for "not found", other codes for real errors.
  err_output=$(gcloud compute instance-templates delete "${template}" \
    --project="${PROJECT}" \
    --quiet 2>&1) && rc=0 || rc=$?

  if [[ $rc -eq 0 ]]; then
    echo "deleted"
    deleted+=("${template}")
  elif echo "${err_output}" | grep -qiE "was not found|does not exist|resourceNotFound"; then
    echo "not found (skipped)"
    not_found+=("${template}")
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
echo "  Deleted   : ${#deleted[@]}"
echo "  Not found : ${#not_found[@]}"
echo "  Errors    : ${#errored[@]}"

if [[ ${#errored[@]} -gt 0 ]]; then
  echo ""
  echo "The following templates encountered errors:" >&2
  for t in "${errored[@]}"; do
    echo "  - ${t}" >&2
  done
  exit 1
fi
