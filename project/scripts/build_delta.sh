#!/usr/bin/env bash
# Clone Delta from source (delta-io/delta master) and optionally build + publish
# to local Maven (~/.m2). Designed for CI but works locally too.
set -euo pipefail

DELTA_REPO="${DELTA_REPO:-https://github.com/delta-io/delta.git}"
DELTA_REF="${DELTA_REF:-master}"
DELTA_DIR="${DELTA_DIR:-/tmp/delta}"
META_ONLY=false

# Read UC version from the checkout directory (before cd-ing to Delta).
UC_VERSION=$(sed -n 's/.*version := "\([^"]*\)".*/\1/p' version.sbt)
if [ -z "$UC_VERSION" ]; then
  echo "::error::Failed to resolve UC version from version.sbt"
  exit 1
fi

for arg in "$@"; do
  case "$arg" in
    --meta-only) META_ONLY=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# Clone
if [ -d "$DELTA_DIR/.git" ]; then
  echo "Delta already cloned at $DELTA_DIR -- fetching $DELTA_REF."
  git -C "$DELTA_DIR" fetch "$DELTA_REPO" "$DELTA_REF"
else
  echo "Cloning Delta from $DELTA_REPO into $DELTA_DIR ..."
  git clone "$DELTA_REPO" "$DELTA_DIR"
  git -C "$DELTA_DIR" fetch "$DELTA_REPO" "$DELTA_REF"
fi
git -C "$DELTA_DIR" checkout --detach FETCH_HEAD

# Resolve metadata
DELTA_SHA=$(git -C "$DELTA_DIR" rev-parse HEAD)
DELTA_VER=$(sed -n 's/.*version := "\([^"]*-SNAPSHOT\)".*/\1/p' "$DELTA_DIR/version.sbt")
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

emit_output "DELTA_SHA" "$DELTA_SHA"
emit_output "DELTA_VERSION" "$DELTA_VER"

if $META_ONLY; then
  exit 0
fi

# Build and publish to local Maven
SPARK_VERSION="${SPARK_VERSION:-${SPARK_MAJOR_MINOR:-}}"
SPARK_VERSION="${SPARK_VERSION:?SPARK_VERSION or SPARK_MAJOR_MINOR is required for build (e.g. 4.1)}"
SPARK_COMMIT_ARGS=()
if [[ -n "${SPARK_COMMIT:-}" ]]; then
  SPARK_COMMIT_ARGS+=("-DsparkCommit=$SPARK_COMMIT")
fi
if [[ -n "${SPARK_ARTIFACT_VERSION:-}" ]]; then
  SPARK_COMMIT_ARGS+=("-DsparkArtifactVersion=$SPARK_ARTIFACT_VERSION")
fi
cd "$DELTA_DIR"

# UC jars are already in ~/.m2 from the workflow's pre-Delta publishM2 step.
# -DunityCatalogVersion makes Delta resolve them directly; the autoBuild flag
# is a safety net (redundant when -DunityCatalogVersion is set, but explicit).
# publishM2 only (not publishLocal): Delta's ivy.xml lists internal modules
# (delta-spark-v1, delta-spark-v2) that aren't published separately. The M2 POM
# filters them via pomPostProcess. UC's build/sbt forces maven-local in the
# resolver chain, so ~/.m2 artifacts are found.
build/sbt -DsparkVersion="$SPARK_VERSION" \
  "${SPARK_COMMIT_ARGS[@]}" \
  -Ddelta.autoBuildPinnedUnityCatalog=false \
  -DunityCatalogVersion="$UC_VERSION" \
  storage/publishM2 \
  kernelApi/publishM2 \
  kernelDefaults/publishM2 \
  kernelUnityCatalog/publishM2 \
  spark/publishM2
