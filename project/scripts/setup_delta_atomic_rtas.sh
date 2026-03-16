#!/usr/bin/env bash

set -euo pipefail

DELTA_DIR="${DELTA_DIR:-/tmp/delta}"
DELTA_REPO="${DELTA_REPO:-https://github.com/TimothyW553/delta.git}"
DELTA_REF="${DELTA_REF:-b190c2c1a59a6b3d5c74b6e1c1b4e27959f990ef}"
DELTA_SPARK_VERSION="${DELTA_SPARK_VERSION:-4.0.1}"

case "$DELTA_SPARK_VERSION" in
  4.0.0)
    # UC's matrix uses 4.0.0, but the pinned Delta branch models that line as 4.0.1.
    DELTA_SPARK_VERSION="4.0.1"
    ;;
esac

rm -rf "$DELTA_DIR"
git clone "$DELTA_REPO" "$DELTA_DIR"
cd "$DELTA_DIR"
git checkout "$DELTA_REF"

# Override version so UC tests can gate features behind isDeltaAtLeast("4.2.0").
echo 'ThisBuild / version := "4.2.0-SNAPSHOT"' > version.sbt

for attempt in 1 2 3; do
  if ./build/sbt -DsparkVersion="$DELTA_SPARK_VERSION" clean publishM2; then
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    exit 1
  fi

  echo "Delta publishM2 failed on attempt $attempt; retrying after a short backoff"
  sleep 5
done
