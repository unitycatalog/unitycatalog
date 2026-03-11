#!/usr/bin/env bash
# Generates a self-signed CA and a server certificate for use with
# AWS IAM Roles Anywhere.  The CA certificate is uploaded as a Trust
# Anchor and the server certificate is used by aws_signing_helper to
# obtain temporary AWS credentials.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/.certs"
CA_DAYS="${CA_DAYS:-3650}"
CERT_DAYS="${CERT_DAYS:-365}"
CN="${CERT_CN:-unity-catalog-server}"

mkdir -p "${CERT_DIR}"

# --- CA key + self-signed certificate ---
if [[ -f "${CERT_DIR}/ca.key" ]]; then
  echo "CA key already exists at ${CERT_DIR}/ca.key -- skipping CA generation."
else
  echo "Generating CA private key …"
  openssl genrsa -out "${CERT_DIR}/ca.key" 4096

  echo "Generating self-signed CA certificate (valid ${CA_DAYS} days) …"
  openssl req -new -x509 \
    -key "${CERT_DIR}/ca.key" \
    -out "${CERT_DIR}/ca.crt" \
    -days "${CA_DAYS}" \
    -subj "/C=US/ST=Local/L=Local/O=UnityCatalog/OU=Demo/CN=UnityCatalogDemoCA"
fi

# --- Server key + certificate signed by the CA ---
if [[ -f "${CERT_DIR}/server.key" ]]; then
  echo "Server key already exists at ${CERT_DIR}/server.key -- skipping."
else
  echo "Generating server private key …"
  openssl genrsa -out "${CERT_DIR}/server.key" 2048

  echo "Creating certificate signing request (CN=${CN}) …"
  openssl req -new \
    -key "${CERT_DIR}/server.key" \
    -out "${CERT_DIR}/server.csr" \
    -subj "/C=US/ST=Local/L=Local/O=UnityCatalog/OU=Demo/CN=${CN}"

  echo "Signing server certificate with CA (valid ${CERT_DAYS} days) …"
  openssl x509 -req \
    -in "${CERT_DIR}/server.csr" \
    -CA "${CERT_DIR}/ca.crt" \
    -CAkey "${CERT_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/server.crt" \
    -days "${CERT_DAYS}" \
    -sha256

  rm -f "${CERT_DIR}/server.csr"
fi

echo ""
echo "Certificates generated in ${CERT_DIR}/"
echo "  CA certificate:     ${CERT_DIR}/ca.crt"
echo "  Server certificate: ${CERT_DIR}/server.crt"
echo "  Server private key: ${CERT_DIR}/server.key"
