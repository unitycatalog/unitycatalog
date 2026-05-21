#!/usr/bin/env bash
# Clone Delta from source (delta-io/delta master) and optionally build + publish
# to local Maven (~/.m2). Designed for CI but works locally too.
set -euo pipefail

DELTA_DIR="${DELTA_DIR:-/tmp/delta}"
SKIP_UC_AUTO_BUILD="${SKIP_UC_AUTO_BUILD:-false}"
META_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --meta-only) META_ONLY=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# ── Clone ────────────────────────────────────────────────────────────────────
if [ -d "$DELTA_DIR/.git" ]; then
  echo "Delta already cloned at $DELTA_DIR -- skipping clone."
else
  echo "Cloning delta-io/delta into $DELTA_DIR ..."
  git clone --depth=1 --branch=master "https://github.com/delta-io/delta.git" "$DELTA_DIR" || \
    git clone --depth=1 "https://github.com/delta-io/delta.git" "$DELTA_DIR"
fi

# ── Resolve metadata ────────────────────────────────────────────────────────
DELTA_SHA=$(git -C "$DELTA_DIR" rev-parse HEAD)
DELTA_VER=$(grep -oP '(?<=")[^"]*-SNAPSHOT(?=")' "$DELTA_DIR/version.sbt")
if [ -z "$DELTA_VER" ]; then
  echo "::error::Failed to resolve Delta SNAPSHOT version from $DELTA_DIR/version.sbt"
  exit 1
fi

emit_output() {
  local key="$1" value="$2"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "$key=$value" >> "$GITHUB_OUTPUT"
  fi
  echo "$key=$value"
}

emit_output "sha" "$DELTA_SHA"
emit_output "version" "$DELTA_VER"

if $META_ONLY; then
  exit 0
fi

# ── Build & publish to local Maven ───────────────────────────────────────────
SPARK_MAJOR_MINOR="${SPARK_MAJOR_MINOR:?SPARK_MAJOR_MINOR is required for build (e.g. 4.1)}"
cd "$DELTA_DIR"

SKIP_UC_FLAG=""
if [ "$SKIP_UC_AUTO_BUILD" = "true" ]; then
  SKIP_UC_FLAG="-Ddelta.autoBuildPinnedUnityCatalog=false"
  SPARK_MAJOR_MINOR=$SPARK_MAJOR_MINOR bash project/scripts/setup_unitycatalog_main.sh
fi

build/sbt -DsparkVersion="$SPARK_MAJOR_MINOR" \
  $SKIP_UC_FLAG \
  storage/publishM2 \
  kernelApi/publishM2 \
  kernelDefaults/publishM2 \
  kernelUnityCatalog/publishM2 \
  spark/publishM2
