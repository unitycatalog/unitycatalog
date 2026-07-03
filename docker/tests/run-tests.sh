#!/usr/bin/env bash
# Run docker/tests JUnit integration suite against a UC server.
#
# UC_SERVER_MODE:
#   binary   (default) start bin/start-uc-server from this repo
#   docker   start a container from UC_DOCKER_IMAGE
#   external use an already-running server (set UC_SERVER_URL if not localhost:8080)
#
# Start container from a prebuilt image (Celonis OAuth required for auth tests):
#   docker compose -f docker/oidc/compose.yaml up -d
#   UC_SERVER_MODE=docker \
#   UC_DOCKER_IMAGE=652406827250.dkr.ecr.eu-central-1.amazonaws.com/celospark/unitycatalog-server:dev_20260701-144822 \
#   ./docker/tests/run-tests.sh
#
# Run against a server you started yourself:
#   docker run -d --name uc-test-server -p 8080:8080 -p 8081:8081 <image>
#   UC_SERVER_MODE=external ./docker/tests/run-tests.sh
#
# Optional:
#   DOCKER_TESTS_ENSURE_UC=0   skip UC start/health check (tests assume server is up)
#   DOCKER_TESTS_STOP_UC=1     stop UC after tests (binary or docker only)
#   UC_ENABLE_OIDC=0           skip merging Celonis OAuth OIDC settings (docker mode)
#   DOCKER_TESTS_KEEP=1        keep tenant resources after tests
#   DOCKER_TESTS_TIMEOUT_SECS  hard timeout for mvn test (default 480)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ucJava.sh
source "$SCRIPT_DIR/ucJava.sh"

cleanup() {
  if [[ "${DOCKER_TESTS_STOP_UC:-0}" == "1" ]]; then
    stop_uc_server
  fi
}
trap cleanup EXIT

if [[ $# -gt 0 ]]; then
  run_docker_tests "$@"
else
  run_all_docker_tests
fi
