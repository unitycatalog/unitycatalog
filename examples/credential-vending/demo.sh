#!/usr/bin/env bash
# End-to-end demonstration of Unity Catalog credential vending using
# external locations and storage credentials.
#
# This script:
#   1. Creates a catalog + schema (if they don't already exist)
#   2. Creates a storage credential pointing to the Data Access IAM Role
#   3. Reads back the server-generated external_id and updates the
#      Data Access Role trust policy so that STS AssumeRole succeeds
#   4. Creates an external location mapping the S3 bucket to the credential
#   5. Requests temporary path credentials via the UC API
#   6. Verifies access by listing S3 objects with the vended credentials
#
# Prerequisites:
#   - UC server running with the server.properties from configure-uc.sh
#   - .env populated by setup-aws.sh
#   - AWS CLI, curl, jq
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: ${ENV_FILE} not found. Run setup-aws.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

UC_BASE="${UC_BASE_URL:-http://localhost:8080}"
API="${UC_BASE}/api/2.1/unity-catalog"

CATALOG_NAME="demo"
SCHEMA_NAME="credential_vending"
CREDENTIAL_NAME="s3-data-access"
LOCATION_NAME="demo-external-location"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
uc_post() {
  local path="$1" body="$2"
  curl -sf -X POST "${API}${path}" \
    -H "Content-Type: application/json" \
    -d "${body}"
}

uc_get() {
  local path="$1"
  curl -sf -X GET "${API}${path}" -H "Accept: application/json"
}

# ---------------------------------------------------------------------------
# 1. Ensure catalog + schema exist
# ---------------------------------------------------------------------------
echo "==> Ensuring catalog '${CATALOG_NAME}' exists …"
uc_post "/catalogs" "{\"name\": \"${CATALOG_NAME}\", \"comment\": \"Demo catalog\"}" 2>/dev/null || true

echo "==> Ensuring schema '${CATALOG_NAME}.${SCHEMA_NAME}' exists …"
uc_post "/schemas" "{\"name\": \"${SCHEMA_NAME}\", \"catalog_name\": \"${CATALOG_NAME}\", \"comment\": \"Credential vending demo\"}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Create storage credential
# ---------------------------------------------------------------------------
echo ""
echo "==> Creating storage credential '${CREDENTIAL_NAME}' …"
CRED_RESPONSE="$(
  uc_post "/credentials" "$(cat <<JSON
{
  "name": "${CREDENTIAL_NAME}",
  "purpose": "STORAGE",
  "aws_iam_role": {
    "role_arn": "${DATA_ROLE_ARN}"
  },
  "comment": "Data access role for credential-vending demo"
}
JSON
)"
)"

echo "${CRED_RESPONSE}" | jq .

EXTERNAL_ID="$(echo "${CRED_RESPONSE}" | jq -r '.aws_iam_role.external_id')"
UC_IAM_ARN="$(echo "${CRED_RESPONSE}" | jq -r '.aws_iam_role.unity_catalog_iam_arn')"

echo ""
echo "  external_id          = ${EXTERNAL_ID}"
echo "  unity_catalog_iam_arn = ${UC_IAM_ARN}"

# ---------------------------------------------------------------------------
# 3. Update the Data Access Role trust policy with the real external_id
# ---------------------------------------------------------------------------
echo ""
echo "==> Updating trust policy on ${DATA_ROLE_NAME} with external_id …"

TRUST_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "${UC_IAM_ARN}"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "${EXTERNAL_ID}"
        }
      }
    }
  ]
}
POLICY
)

aws iam update-assume-role-policy \
  --role-name "${DATA_ROLE_NAME}" \
  --policy-document "${TRUST_POLICY}"

echo "  Trust policy updated."

# ---------------------------------------------------------------------------
# 4. Create external location
# ---------------------------------------------------------------------------
echo ""
echo "==> Creating external location '${LOCATION_NAME}' …"
LOC_RESPONSE="$(
  uc_post "/external-locations" "$(cat <<JSON
{
  "name": "${LOCATION_NAME}",
  "url": "s3://${BUCKET_NAME}/demo",
  "credential_name": "${CREDENTIAL_NAME}",
  "comment": "Points to the demo prefix in the S3 bucket"
}
JSON
)"
)"

echo "${LOC_RESPONSE}" | jq .

# ---------------------------------------------------------------------------
# 5. Request temporary path credentials
# ---------------------------------------------------------------------------
echo ""
echo "==> Requesting temporary path credentials …"
TEMP_CREDS="$(
  uc_post "/temporary-path-credentials" "$(cat <<JSON
{
  "url": "s3://${BUCKET_NAME}/demo",
  "operation": "PATH_READ"
}
JSON
)"
)"

echo "${TEMP_CREDS}" | jq .

AWS_ACCESS_KEY="$(echo "${TEMP_CREDS}" | jq -r '.aws_temp_credentials.access_key_id')"
AWS_SECRET_KEY="$(echo "${TEMP_CREDS}" | jq -r '.aws_temp_credentials.secret_access_key')"
AWS_SESSION_TOKEN="$(echo "${TEMP_CREDS}" | jq -r '.aws_temp_credentials.session_token')"

if [[ "${AWS_ACCESS_KEY}" == "null" || -z "${AWS_ACCESS_KEY}" ]]; then
  echo "ERROR: No AWS credentials returned. Check server logs." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 6. Verify: use the vended credentials to access S3
# ---------------------------------------------------------------------------
echo ""
echo "==> Verifying access: listing objects at s3://${BUCKET_NAME}/demo/ …"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY}" \
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_KEY}" \
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
  aws s3 ls "s3://${BUCKET_NAME}/demo/" --region "${AWS_REGION}"

echo ""
echo "==> Reading s3://${BUCKET_NAME}/demo/hello.txt …"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY}" \
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_KEY}" \
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
  aws s3 cp "s3://${BUCKET_NAME}/demo/hello.txt" - --region "${AWS_REGION}"

echo ""
echo ""
echo "Credential vending demo completed successfully."
