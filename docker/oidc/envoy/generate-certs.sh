#!/usr/bin/env bash
# Generate self-signed TLS certs for Envoy HTTPS (:443) used by UC JWKS discovery.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/certs"
REALM="${UC_OAUTH_REALM:-dev.celonis.cloud}"
mkdir -p "$CERT_DIR"
openssl req -x509 -newkey rsa:2048 \
  -keyout "$CERT_DIR/key.pem" \
  -out "$CERT_DIR/cert.pem" \
  -days 365 -nodes \
  -subj "/CN=*.${REALM}" \
  -addext "subjectAltName=DNS:*.${REALM},DNS:${REALM}"
echo "Wrote $CERT_DIR/cert.pem and $CERT_DIR/key.pem (wildcard *.${REALM})"
