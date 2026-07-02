#!/usr/bin/env bash
# Ensure Unity Catalog server is running for docker/spark and docker/tests.
#
# UC_SERVER_MODE:
#   binary   (default) bin/start-uc-server from this repo
#   docker   prebuilt image via UC_DOCKER_IMAGE
#   external already-running server at UC_SERVER_URL / localhost:8080
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
exec "$ROOT/docker/start-uc-for-tests.sh" "$ROOT"
