#!/usr/bin/env bash
# Start Unity Catalog for integration tests.
#
# Modes (UC_SERVER_MODE):
#   binary   - run bin/start-uc-server from the repo (default)
#   docker   - run a prebuilt UC Docker image (UC_DOCKER_IMAGE required)
#   external - UC server already running; only verify reachability
#
# Examples:
#   ./docker/start-uc-for-tests.sh
#   UC_SERVER_MODE=docker UC_DOCKER_IMAGE=my-registry/unitycatalog-server:tag ./docker/start-uc-for-tests.sh
#   UC_SERVER_MODE=external ./docker/start-uc-for-tests.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${1:-${GITHUB_WORKSPACE:-$(cd "$SCRIPT_DIR/.." && pwd)}}"

UC_SERVER_MODE="${UC_SERVER_MODE:-binary}"
UC_DOCKER_IMAGE="${UC_DOCKER_IMAGE:-}"
UC_DOCKER_CONTAINER_NAME="${UC_DOCKER_CONTAINER_NAME:-uc-test-server}"
UC_DOCKER_HOME="${UC_DOCKER_HOME:-/home/unitycatalog}"
UC_DOCKER_MINIO_ENDPOINT="${UC_DOCKER_MINIO_ENDPOINT:-http://host.docker.internal:9000}"
UC_DOCKER_KEYCLOAK_HOST="${UC_DOCKER_KEYCLOAK_HOST:-host.docker.internal:9090}"
UC_ENABLE_OIDC="${UC_ENABLE_OIDC:-}"
UC_API_PORT="${UC_API_PORT:-8080}"
UC_BACKEND_PORT="${UC_BACKEND_PORT:-8081}"
LOOP_COUNT="${LOOP_COUNT:-48}"
SLEEP_DURATION="${SLEEP_DURATION:-5}"
HEALTH_URL="${HEALTH_URL:-http://localhost:${UC_API_PORT}/api/2.1/unity-catalog/catalogs}"
LOG_FILE="${LOG_FILE:-uc_server.log}"

MODE_FILE="$ROOT_DIR/uc_server.mode"
PID_FILE="$ROOT_DIR/uc_server.pid"
CONTAINER_FILE="$ROOT_DIR/uc_server.container"
DOCKER_CONF_DIR=""

cd "$ROOT_DIR"

cleanup_on_failure() {
  case "$UC_SERVER_MODE" in
    docker)
      if [[ -f "$CONTAINER_FILE" ]]; then
        docker rm -f "$(cat "$CONTAINER_FILE")" 2>/dev/null || true
      fi
      rm -f "$MODE_FILE" "$CONTAINER_FILE"
      ;;
    binary)
      if [[ -n "${UC_SERVER_PID:-}" ]] && kill -0 "$UC_SERVER_PID" 2>/dev/null; then
        kill "$UC_SERVER_PID" 2>/dev/null || true
        sleep 2
        kill -9 "$UC_SERVER_PID" 2>/dev/null || true
      fi
      rm -f "$PID_FILE" "$MODE_FILE"
      ;;
  esac
  if [[ -n "$DOCKER_CONF_DIR" && -d "$DOCKER_CONF_DIR" ]]; then
    rm -rf "$DOCKER_CONF_DIR"
    DOCKER_CONF_DIR=""
  fi
}

auth_header() {
  if [[ -f "$ROOT_DIR/etc/conf/token.txt" ]]; then
    echo "Authorization: Bearer $(tr -d '[:space:]' < "$ROOT_DIR/etc/conf/token.txt")"
  fi
}

check_server() {
  local auth
  auth="$(auth_header)"
  if [[ -n "$auth" ]]; then
    curl --silent --fail --max-time 3 -H "$auth" "$HEALTH_URL" >/dev/null 2>&1
  else
    curl --silent --fail --max-time 3 "$HEALTH_URL" >/dev/null 2>&1
  fi
}

wait_for_server() {
  local label="$1"
  local check_alive="${2:-}"

  echo "Waiting for UC server to be ready at $HEALTH_URL ..."
  for (( i=1; i<=LOOP_COUNT; i++ )); do
    if [[ -n "$check_alive" ]] && ! "$check_alive"; then
      cleanup_on_failure
      exit 1
    fi
    if check_server; then
      echo "UC server is ready ($label)"
      return 0
    fi
    echo "Waiting for UC server... ($i/$LOOP_COUNT)"
    sleep "$SLEEP_DURATION"
  done

  echo "ERROR: UC server failed to become ready ($label)"
  cleanup_on_failure
  exit 1
}

prepare_docker_conf() {
  local conf_dir props minio_snippet oidc_snippet enable_oidc
  conf_dir="$(mktemp -d "${TMPDIR:-/tmp}/uc-docker-conf.XXXXXX")"
  cp -a "$ROOT_DIR/etc/conf/." "$conf_dir/"
  props="$conf_dir/server.properties"
  minio_snippet="$ROOT_DIR/docker/minio/server.properties.snippet"
  oidc_snippet="$ROOT_DIR/docker/oidc/server.properties.docker.snippet"
  enable_oidc="${UC_ENABLE_OIDC:-1}"
  local keycloak_issuer="http://${UC_DOCKER_KEYCLOAK_HOST}/realms/unity-catalog"

  if [[ -f "$props" ]]; then
    sed -i.bak \
      -e "s|http://\\[::1\\]:9000|${UC_DOCKER_MINIO_ENDPOINT}|g" \
      -e "s|http://127\\.0\\.0\\.1:9000|${UC_DOCKER_MINIO_ENDPOINT}|g" \
      -e "s|http://localhost:9000|${UC_DOCKER_MINIO_ENDPOINT}|g" \
      "$props"
    rm -f "${props}.bak"
  fi

  if [[ -f "$minio_snippet" && -f "$props" ]] \
      && ! grep -q '^s3\.bucketPath\.0=s3://' "$props"; then
    {
      echo ""
      echo "# Appended for docker/tests (MinIO on host)"
      sed "s|http://\\[::1\\]:9000|${UC_DOCKER_MINIO_ENDPOINT}|g" "$minio_snippet"
    } >>"$props"
  fi

  if [[ "$enable_oidc" == "1" && -f "$oidc_snippet" && -f "$props" ]]; then
    if command -v curl >/dev/null 2>&1; then
      local actual_issuer
      actual_issuer="$(curl -sf "http://${UC_DOCKER_KEYCLOAK_HOST}/realms/unity-catalog/.well-known/openid-configuration" 2>/dev/null | sed -n 's/.*"issuer":"\([^"]*\)".*/\1/p' | head -1 || true)"
      if [[ -n "$actual_issuer" && "$actual_issuer" != "$keycloak_issuer" ]]; then
        echo "ERROR: Keycloak issuer is '$actual_issuer' but docker tests expect '$keycloak_issuer'" >&2
        echo "Restart Keycloak with:" >&2
        echo "  docker compose -f docker/oidc/compose.yaml -f docker/oidc/compose.docker-tests.yaml up -d --force-recreate keycloak" >&2
        exit 1
      fi
    fi
    {
      echo ""
      echo "# Appended for docker/tests (Keycloak at ${UC_DOCKER_KEYCLOAK_HOST})"
      grep -v '^#' "$oidc_snippet" | grep -v '^[[:space:]]*$'
    } >>"$props"
    echo "OIDC auth enabled for docker tests (issuer ${keycloak_issuer})" >&2
  fi

  DOCKER_CONF_DIR="$conf_dir"
  echo "$conf_dir"
}

start_binary_server() {
  echo "Starting UC server in binary mode"
  : > "$LOG_FILE"

  env "storage-root.models=file:///tmp/ucroot" bin/start-uc-server >"$LOG_FILE" 2>&1 &
  UC_SERVER_PID=$!
  echo "$UC_SERVER_PID" > "$PID_FILE"
  echo "binary" > "$MODE_FILE"
  echo "UC server started with PID $UC_SERVER_PID"

  check_process() {
    if ! kill -0 "$UC_SERVER_PID" 2>/dev/null; then
      echo "ERROR: UC server process (PID $UC_SERVER_PID) died unexpectedly!"
      echo "=== Server Logs (tail) ==="
      tail -n 200 "$LOG_FILE" || true
      echo "=========================="
      return 1
    fi
    return 0
  }

  wait_for_server "binary" check_process
  echo "UC server up and running"
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$UC_DOCKER_CONTAINER_NAME"
}

check_container() {
  if ! container_running; then
    echo "ERROR: UC server container '$UC_DOCKER_CONTAINER_NAME' is not running!"
    echo "=== Container Logs (tail) ==="
    docker logs --tail 200 "$UC_DOCKER_CONTAINER_NAME" 2>&1 || true
    echo "============================="
    return 1
  fi
  return 0
}

start_docker_server() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is required when UC_SERVER_MODE=docker"
    exit 1
  fi
  if [[ -z "$UC_DOCKER_IMAGE" ]]; then
    echo "ERROR: UC_DOCKER_IMAGE is required when UC_SERVER_MODE=docker" >&2
    exit 1
  fi

  if container_running; then
    echo "Reusing running UC server container: $UC_DOCKER_CONTAINER_NAME"
    echo "docker" > "$MODE_FILE"
    echo "$UC_DOCKER_CONTAINER_NAME" > "$CONTAINER_FILE"
    wait_for_server "docker (existing container)" check_container
    echo "UC server container up and running"
    return 0
  fi

  local conf_mount
  conf_mount="$(prepare_docker_conf)"

  echo "Starting UC server in docker mode"
  echo "Image: $UC_DOCKER_IMAGE"
  echo "Container: $UC_DOCKER_CONTAINER_NAME"
  echo "Ports: ${UC_API_PORT}->8080, ${UC_BACKEND_PORT}->8081"
  echo "Config: $conf_mount -> ${UC_DOCKER_HOME}/etc/conf"

  docker rm -f "$UC_DOCKER_CONTAINER_NAME" 2>/dev/null || true

  docker run -d \
    --name "$UC_DOCKER_CONTAINER_NAME" \
    -p "${UC_API_PORT}:8080" \
    -p "${UC_BACKEND_PORT}:8081" \
    --add-host=host.docker.internal:host-gateway \
    -v "${conf_mount}:${UC_DOCKER_HOME}/etc/conf" \
    -e "storage-root.models=file:///tmp/ucroot" \
    "$UC_DOCKER_IMAGE"

  echo "docker" > "$MODE_FILE"
  echo "$UC_DOCKER_CONTAINER_NAME" > "$CONTAINER_FILE"

  wait_for_server "docker" check_container
  echo "UC server container up and running"
}

verify_external_server() {
  echo "Using external UC server at $HEALTH_URL"
  if check_server; then
    echo "external" > "$MODE_FILE"
    echo "UC server is reachable"
    return 0
  fi

  echo "ERROR: UC server not reachable at $HEALTH_URL" >&2
  echo "Start the container first, or set UC_SERVER_URL / UC_API_PORT to match." >&2
  exit 1
}

if check_server; then
  echo "UC server already running at $HEALTH_URL"
  exit 0
fi

case "$UC_SERVER_MODE" in
  binary)
    start_binary_server
    ;;
  docker)
    start_docker_server
    ;;
  external)
    verify_external_server
    ;;
  *)
    echo "ERROR: unsupported UC_SERVER_MODE='$UC_SERVER_MODE' (expected 'binary', 'docker', or 'external')"
    exit 1
    ;;
esac
