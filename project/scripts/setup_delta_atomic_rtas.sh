#!/usr/bin/env bash

set -euo pipefail

DELTA_DIR="${DELTA_DIR:-/tmp/delta}"
DELTA_REPO="${DELTA_REPO:-https://github.com/delta-io/delta.git}"
DELTA_REF="${DELTA_REF:-a08dcaf59081342b18f5daca0a0e79122099773c}"
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
