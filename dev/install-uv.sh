#!/bin/sh
# Install uv — SHA256-verified, statically-linked musl build.
# Single source of truth for uv version + checksum used across CI workflows.
#
# Usage (from repo root):
#   bash dev/install-uv.sh
set -eu

UV_VERSION="0.13.5"
# SHA256 from official release asset:
# https://github.com/astral-sh/uv/releases/download/0.13.5/uv-x86_64-unknown-linux-musl.tar.gz
UV_SHA256="57ef9d9ccb01fb1bd998df995b0dd221b52f7d7841fbea93f2d4463bf61fb7a9"

TARBALL="uv-x86_64-unknown-linux-musl.tar.gz"
EXTRACT_DIR="uv-x86_64-unknown-linux-musl"

curl -fsSL -o "$TARBALL" \
  "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${TARBALL}"

echo "${UV_SHA256}  ${TARBALL}" | sha256sum -c -

tar -xzf "$TARBALL"
install -m 755 "${EXTRACT_DIR}/uv" /usr/local/bin/uv
install -m 755 "${EXTRACT_DIR}/uvx" /usr/local/bin/uvx
rm -rf "${EXTRACT_DIR}" "${TARBALL}"

uv --version
