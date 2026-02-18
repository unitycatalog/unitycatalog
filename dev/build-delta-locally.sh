#!/usr/bin/env bash
set -euo pipefail

# Script to clone the latest Delta Lake master branch and publish artifacts
# to the local Maven repository (~/.m2).
#
# This is required because unitycatalog depends on delta 4.1.0-SNAPSHOT,
# which is not published to Maven Central.
#
# Usage:
#   ./dev/build-delta-locally.sh
#
# Environment variables:
#   DELTA_DIR      - Directory to clone delta into (default: /tmp/delta)
#   DELTA_CLEANUP  - Set to "true" to remove the delta clone after build (default: false)

DELTA_DIR="${DELTA_DIR:-/tmp/delta}"
DELTA_REPO="https://github.com/delta-io/delta.git"

echo "=== Building Delta Lake from source and publishing to local Maven repository ==="

# Clone or update the delta repo (shallow clone for speed)
if [ -d "$DELTA_DIR/.git" ]; then
  echo "Delta directory already exists at $DELTA_DIR, pulling latest master..."
  cd "$DELTA_DIR"
  git fetch --depth 1 origin master
  git checkout master
  git reset --hard origin/master
else
  echo "Cloning Delta Lake repository (shallow)..."
  git clone --depth 1 --branch master "$DELTA_REPO" "$DELTA_DIR"
  cd "$DELTA_DIR"
fi

echo "Building Delta Lake with Spark 4.1 and publishing to local Maven repository..."
./build/sbt -DsparkVersion=4.1 clean package publishM2

echo "=== Delta Lake build and publish complete ==="

# Optionally clean up to save disk space (useful in CI/Docker environments)
if [ "${DELTA_CLEANUP:-false}" = "true" ]; then
  echo "Cleaning up Delta Lake source directory..."
  rm -rf "$DELTA_DIR"
fi
