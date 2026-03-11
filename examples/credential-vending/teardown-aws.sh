#!/usr/bin/env bash
# Deletes all AWS resources created by setup-aws.sh.
# Resources are removed in reverse dependency order.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: ${ENV_FILE} not found. Nothing to tear down." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

echo "This will DELETE the following AWS resources:"
echo "  Trust Anchor : ${TRUST_ANCHOR_ARN}"
echo "  Profile      : ${PROFILE_ARN}"
echo "  Master Role  : ${MASTER_ROLE_NAME}"
echo "  Data Role    : ${DATA_ROLE_NAME}"
echo "  S3 Bucket    : ${BUCKET_NAME}"
echo ""
read -rp "Continue? [y/N] " confirm
if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
  echo "Aborted."
  exit 0
fi

# ---------------------------------------------------------------------------
# IAM Roles Anywhere
# ---------------------------------------------------------------------------
echo "==> Deleting IAM Roles Anywhere Profile …"
PROFILE_ID="$(echo "${PROFILE_ARN}" | awk -F/ '{print $NF}')"
aws rolesanywhere delete-profile --profile-id "${PROFILE_ID}" --region "${AWS_REGION}" 2>/dev/null || true

echo "==> Deleting IAM Roles Anywhere Trust Anchor …"
TRUST_ANCHOR_ID="$(echo "${TRUST_ANCHOR_ARN}" | awk -F/ '{print $NF}')"
aws rolesanywhere delete-trust-anchor --trust-anchor-id "${TRUST_ANCHOR_ID}" --region "${AWS_REGION}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# IAM Roles (inline policies first, then roles)
# ---------------------------------------------------------------------------
echo "==> Deleting Data Access Role …"
aws iam delete-role-policy --role-name "${DATA_ROLE_NAME}" --policy-name "${RESOURCE_PREFIX}-data-s3-policy" 2>/dev/null || true
aws iam delete-role --role-name "${DATA_ROLE_NAME}" 2>/dev/null || true

echo "==> Deleting Master Role …"
aws iam delete-role-policy --role-name "${MASTER_ROLE_NAME}" --policy-name "${RESOURCE_PREFIX}-master-assume-policy" 2>/dev/null || true
aws iam delete-role --role-name "${MASTER_ROLE_NAME}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# S3 Bucket (must be emptied first)
# ---------------------------------------------------------------------------
echo "==> Emptying and deleting S3 bucket: ${BUCKET_NAME} …"
aws s3 rm "s3://${BUCKET_NAME}" --recursive 2>/dev/null || true
aws s3api delete-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Clean up local state
# ---------------------------------------------------------------------------
rm -f "${ENV_FILE}"
echo ""
echo "Teardown complete."
