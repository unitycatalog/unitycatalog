#!/usr/bin/env bash
#
# Builds the server image from a git ref and tags it unitycatalog-build:<commit sha>. The upgrade
# tests (UpgradePath.BASE) run it to get a server built from the code a PR starts from.
#
# Usage: dev/build-image.sh <git ref>
#
# The ref's tree is exported with `git archive` (no worktree, nothing untracked), then built with
# BuildKit: the Dockerfile builds from source and needs `COPY --parents`. Optional environment:
#   UC_BUILDX_BUILDER  a buildx builder to use instead of the daemon's own BuildKit, for a Docker
#                      daemon too old for `COPY --parents`, for example one made with
#                      `docker buildx create --name uc-build --driver docker-container`
#   MAVEN_PROXY_URL    passed to the build as the Maven mirror (see build/sbt)
#
# Does nothing when the image already exists. Prints the image name last.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <git ref>" >&2
  exit 2
fi

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
sha=$(git -C "$repo" rev-parse --verify "$1^{commit}")
image="unitycatalog-build:$sha"

if docker image inspect "$image" > /dev/null 2>&1; then
  echo "$image"
  exit 0
fi

context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
git -C "$repo" archive "$sha" | tar -x -C "$context"

args=(--tag "$image")
if [[ -n "${MAVEN_PROXY_URL:-}" ]]; then
  args+=(--build-arg "MAVEN_PROXY_URL=$MAVEN_PROXY_URL")
  # build/sbt fetches its launcher from the mirror's root, which a Maven mirror may not serve.
  # Reuse the launcher this checkout already downloaded (git-ignored, so not in the archive).
  cp "$repo"/build/sbt-launch-*.jar "$context/build/" 2> /dev/null || true
fi

echo "Building $image from $1" >&2
if [[ -n "${UC_BUILDX_BUILDER:-}" ]]; then
  docker buildx build --builder "$UC_BUILDX_BUILDER" --load "${args[@]}" "$context" >&2
else
  DOCKER_BUILDKIT=1 docker build "${args[@]}" "$context" >&2
fi
echo "$image"
