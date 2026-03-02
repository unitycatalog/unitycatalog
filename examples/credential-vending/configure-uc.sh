#!/usr/bin/env bash
# Generates a server.properties file for the UC server with the AWS
# credential-vending settings populated from the .env file written
# by setup-aws.sh.
#
# It also prints the ~/.aws/config snippet needed to use
# aws_signing_helper as a credential_process so that the Java SDK's
# DefaultCredentialsProvider can obtain temporary credentials via
# IAM Roles Anywhere.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
CERT_DIR="${SCRIPT_DIR}/.certs"
OUT="${SCRIPT_DIR}/server.properties"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: ${ENV_FILE} not found. Run setup-aws.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

cat > "${OUT}" <<EOF
server.env=dev
server.authorization=disable
server.auth-type=token
server.cookie-timeout=P5D

#### AWS credential-vending configuration ####

# Master role ARN -- advertised as unity_catalog_iam_arn in credential
# securables.  The UC server assumes storage roles on behalf of this
# identity.
aws.masterRoleArn=${MASTER_ROLE_ARN}

# Leave access keys blank so the Java SDK uses DefaultCredentialsProvider.
# When using IAM Roles Anywhere, credentials are supplied via the
# credential_process in ~/.aws/config (see below).
aws.accessKey=
aws.secretKey=

aws.region=${AWS_REGION}
EOF

echo "Wrote ${OUT}"
echo ""

# -------------------------------------------------------------------------
# Print the aws_signing_helper credential_process config
# -------------------------------------------------------------------------
TRUST_ANCHOR_ID="$(echo "${TRUST_ANCHOR_ARN}" | awk -F/ '{print $NF}')"
PROFILE_ID="$(echo "${PROFILE_ARN}" | awk -F/ '{print $NF}')"

cat <<INSTRUCTIONS
===========================================================================
 Next: configure the AWS SDK credential chain
===========================================================================

Add the following profile to ~/.aws/config so that the AWS Java SDK
(DefaultCredentialsProvider) obtains temporary credentials through
IAM Roles Anywhere.

  [profile uc-credential-vending]
  credential_process = aws_signing_helper credential-process \\
    --certificate       ${CERT_DIR}/server.crt \\
    --private-key       ${CERT_DIR}/server.key \\
    --trust-anchor-arn  ${TRUST_ANCHOR_ARN} \\
    --profile-arn       ${PROFILE_ARN} \\
    --role-arn          ${MASTER_ROLE_ARN} \\
    --region            ${AWS_REGION}

Then export the profile before starting the UC server:

  export AWS_PROFILE=uc-credential-vending

Alternatively, set the credential_process in the [default] profile
if you prefer not to use a named profile.

Install aws_signing_helper:
  https://docs.aws.amazon.com/rolesanywhere/latest/userguide/credential-helper.html
===========================================================================

You can now:
  1. Copy or symlink ${OUT} to etc/conf/server.properties
  2. export AWS_PROFILE=uc-credential-vending
  3. Start the UC server:  bin/start-uc-server
  4. Run ./demo.sh to exercise credential vending
INSTRUCTIONS
