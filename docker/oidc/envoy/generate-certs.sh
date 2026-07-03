#!/usr/bin/env bash
# Generate self-signed TLS certs for Envoy HTTPS (:443) used by UC JWKS discovery.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/certs"
mkdir -p "$CERT_DIR"
openssl req -x509 -newkey rsa:2048 \
  -keyout "$CERT_DIR/key.pem" \
  -out "$CERT_DIR/cert.pem" \
  -days 365 -nodes \
  -subj "/CN=dev.dev.celonis.cloud"
echo "Wrote $CERT_DIR/cert.pem and $CERT_DIR/key.pem"
