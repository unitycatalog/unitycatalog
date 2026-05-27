#!/bin/sh
# Install uv — SHA256-verified, statically-linked musl build.
# Single source of truth for version + checksum across CI workflows.
#
# Usage (from any working directory in the repo):
#   bash "$GITHUB_WORKSPACE/.github/scripts/install-uv.sh"
set -eu

UV_VERSION="0.10.12"
# SHA256 from official release asset:
# https://github.com/astral-sh/uv/releases/download/0.10.12/uv-x86_64-unknown-linux-musl.tar.gz
UV_SHA256="adccf40b5d1939a5e0093081ec2307ea24235adf7c2d96b122c561fa37711c46"

TARBALL="uv-x86_64-unknown-linux-musl.tar.gz"
EXTRACT_DIR="uv-x86_64-unknown-linux-musl"

curl -fsSL -o "$TARBALL" \
  "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${TARBALL}"

echo "${UV_SHA256}  ${TARBALL}" | sha256sum -c -

tar -xzf "$TARBALL"
install -m 755 "${EXTRACT_DIR}/uv" /usr/local/bin/uv
install -m 755 "${EXTRACT_DIR}/uvx" /usr/local/bin/uvx
rm -rf "$EXTRACT_DIR" "$TARBALL"

uv --version
