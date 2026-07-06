#!/usr/bin/env bash
# Resolve OAuth tenant settings from environment (source, do not execute).
# Hostname pattern: {UC_OAUTH_TEAM}.{UC_OAUTH_REALM}
set -euo pipefail

: "${UC_OAUTH_TEAM:=dev}"
: "${UC_OAUTH_REALM:=dev.celonis.cloud}"

if [[ -z "${UC_OAUTH_HOST:-}" ]]; then
  UC_OAUTH_HOST="${UC_OAUTH_TEAM}.${UC_OAUTH_REALM}"
fi

if [[ -z "${UC_OAUTH_ISSUER:-}" ]]; then
  UC_OAUTH_ISSUER="https://${UC_OAUTH_HOST}"
fi

if [[ -z "${UC_OAUTH_BASE_URL:-}" ]]; then
  UC_OAUTH_BASE_URL="http://${UC_OAUTH_HOST}:9010"
fi

if [[ -z "${UC_OAUTH_ALLOWED_ISSUERS:-}" ]]; then
  UC_OAUTH_ALLOWED_ISSUERS="https://*.${UC_OAUTH_REALM}"
fi

export UC_OAUTH_TEAM UC_OAUTH_REALM UC_OAUTH_HOST UC_OAUTH_ISSUER UC_OAUTH_BASE_URL UC_OAUTH_ALLOWED_ISSUERS
