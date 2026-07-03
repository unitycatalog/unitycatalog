#!/usr/bin/env bash
# Start Unity Catalog for integration tests.
#
# Modes (UC_SERVER_MODE):
#   binary   - run bin/start-uc-server from the repo (default; merges Celonis OIDC when UC_ENABLE_OIDC=1)
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
UC_DOCKER_MINIO_ENDPOINT="${UC_DOCKER_MINIO_ENDPOINT:-}"
UC_DOCKER_MINIO_NETWORK="${UC_DOCKER_MINIO_NETWORK:-}"
UC_DOCKER_OAUTH_HOST="${UC_DOCKER_OAUTH_HOST:-dev.dev.celonis.cloud}"
UC_OAUTH_ISSUER="${UC_OAUTH_ISSUER:-https://dev.dev.celonis.cloud}"
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
CONF_BACKUP_MARKER="$ROOT_DIR/uc_server.conf_backup"
TEST_CONF_DIR=""
ETC_CONF_LINK="$ROOT_DIR/etc/conf"

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
  restore_test_conf_overlay
  if [[ -n "$TEST_CONF_DIR" && -d "$TEST_CONF_DIR" ]]; then
    rm -rf "$TEST_CONF_DIR"
    TEST_CONF_DIR=""
  fi
}

restore_test_conf_overlay() {
  if [[ -L "$ETC_CONF_LINK" ]]; then
    rm -f "$ETC_CONF_LINK"
  fi
  if [[ -f "$CONF_BACKUP_MARKER" ]]; then
    local kind
    kind="$(cat "$CONF_BACKUP_MARKER")"
    if [[ "$kind" == "dir" || "$kind" == "file" ]] \
        && [[ -e "${ETC_CONF_LINK}.uc-test-bak" ]]; then
      mv "${ETC_CONF_LINK}.uc-test-bak" "$ETC_CONF_LINK"
    fi
    rm -f "$CONF_BACKUP_MARKER"
  fi
}

install_test_conf_overlay() {
  local conf_dir="$1"
  if [[ -L "$ETC_CONF_LINK" ]] && [[ "$(readlink "$ETC_CONF_LINK")" == "$conf_dir" ]]; then
    return 0
  fi
  restore_test_conf_overlay
  if [[ -d "$ETC_CONF_LINK" && ! -L "$ETC_CONF_LINK" ]]; then
    mv "$ETC_CONF_LINK" "${ETC_CONF_LINK}.uc-test-bak"
    echo "dir" >"$CONF_BACKUP_MARKER"
  elif [[ -e "$ETC_CONF_LINK" ]]; then
    mv "$ETC_CONF_LINK" "${ETC_CONF_LINK}.uc-test-bak"
    echo "file" >"$CONF_BACKUP_MARKER"
  else
    mkdir -p "$ROOT_DIR/etc"
    echo "missing" >"$CONF_BACKUP_MARKER"
  fi
  ln -sfn "$conf_dir" "$ETC_CONF_LINK"
}

prepare_oauth_truststore() {
  local cert_dir="$ROOT_DIR/docker/oidc/caddy"
  local root_cert="$cert_dir/root.crt"
  local intermediate_cert="$cert_dir/intermediate.crt"
  local truststore="${TMPDIR:-/tmp}/uc-oauth-truststore.jks"
  local gateway_container="${UC_OAUTH_GATEWAY_CONTAINER:-unitycatalog-oidc-oauth-gateway-1}"

  if command -v docker >/dev/null 2>&1 \
      && docker ps --format '{{.Names}}' | grep -qx "$gateway_container"; then
    mkdir -p "$cert_dir"
    docker cp "${gateway_container}:/data/caddy/pki/authorities/local/root.crt" "$root_cert" >/dev/null 2>&1 || true
    docker cp "${gateway_container}:/data/caddy/pki/authorities/local/intermediate.crt" \
      "$intermediate_cert" >/dev/null 2>&1 || true
  fi

  if [[ ! -f "$root_cert" ]]; then
    echo "WARN: OAuth CA cert not found at $root_cert; HTTPS discovery may fail" >&2
    return 1
  fi
  rm -f "$truststore"
  keytool -importcert -noprompt \
    -alias celonis-oauth-root \
    -file "$root_cert" \
    -keystore "$truststore" \
    -storepass changeit >/dev/null 2>&1
  if [[ -f "$intermediate_cert" ]]; then
    keytool -importcert -noprompt \
      -alias celonis-oauth-intermediate \
      -file "$intermediate_cert" \
      -keystore "$truststore" \
      -storepass changeit >/dev/null 2>&1
  fi
  echo "$truststore"
}

resolve_docker_minio_settings() {
  if [[ -z "$UC_DOCKER_MINIO_ENDPOINT" ]] \
      && docker ps --format '{{.Names}}' | grep -qx 'unitycatalog-local-minio-1'; then
    UC_DOCKER_MINIO_ENDPOINT="http://minio:9000"
    UC_DOCKER_MINIO_NETWORK="${UC_DOCKER_MINIO_NETWORK:-unitycatalog-local_default}"
    echo "MinIO reachable via docker network ${UC_DOCKER_MINIO_NETWORK} at ${UC_DOCKER_MINIO_ENDPOINT}" >&2
  fi
  UC_DOCKER_MINIO_ENDPOINT="${UC_DOCKER_MINIO_ENDPOINT:-http://host.docker.internal:9000}"
}

oauth_java_opts() {
  local truststore
  if truststore="$(prepare_oauth_truststore)"; then
    echo "-Djavax.net.ssl.trustStore=$truststore -Djavax.net.ssl.trustStorePassword=changeit"
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

# Build a temp etc/conf tree for integration tests (binary or docker).
# Optional minio_endpoint rewrites host MinIO URLs for UC running inside Docker.
prepare_test_conf() {
  local minio_endpoint="${1:-}"
  local conf_dir props minio_snippet oidc_snippet enable_oidc
  conf_dir="$(mktemp -d "${TMPDIR:-/tmp}/uc-test-conf.XXXXXX")"
  cp -a "$ROOT_DIR/etc/conf/." "$conf_dir/"
  props="$conf_dir/server.properties"
  minio_snippet="$ROOT_DIR/docker/minio/server.properties.snippet"
  oidc_snippet="$ROOT_DIR/docker/oidc/server.properties.docker.snippet"
  enable_oidc="${UC_ENABLE_OIDC:-1}"
  local oauth_issuer="$UC_OAUTH_ISSUER"

  if [[ -n "$minio_endpoint" && -f "$props" ]]; then
    sed -i.bak \
      -e "s|http://\\[::1\\]:9000|${minio_endpoint}|g" \
      -e "s|http://127\\.0\\.0\\.1:9000|${minio_endpoint}|g" \
      -e "s|http://localhost:9000|${minio_endpoint}|g" \
      -e "s|http://host\\.docker\\.internal:9000|${minio_endpoint}|g" \
      "$props"
    rm -f "${props}.bak"
  fi

  if [[ -f "$minio_snippet" && -f "$props" ]] \
      && ! grep -q '^s3\.bucketPath\.0=s3://' "$props"; then
    {
      echo ""
      echo "# Appended for docker/tests (MinIO on host)"
      if [[ -n "$minio_endpoint" ]]; then
        sed -e "s|http://\\[::1\\]:9000|${minio_endpoint}|g" \
            -e "s|http://host\\.docker\\.internal:9000|${minio_endpoint}|g" \
            "$minio_snippet"
      else
        cat "$minio_snippet"
      fi
    } >>"$props"
  fi

  if [[ "$enable_oidc" == "1" && -f "$oidc_snippet" && -f "$props" ]]; then
    if command -v curl >/dev/null 2>&1; then
      local actual_issuer
      actual_issuer="$(curl -skf --resolve "${UC_DOCKER_OAUTH_HOST}:443:127.0.0.1" \
        -H 'X-Celonis-Team-Domain: dev' \
        -H 'X-Celonis-Team-Id: 79257834-828d-48cb-951d-75294d6e1cce' \
        "https://${UC_DOCKER_OAUTH_HOST}/.well-known/openid-configuration" 2>/dev/null \
        | sed -n 's/.*"issuer":"\([^"]*\)".*/\1/p' | head -1 || true)"
      if [[ -n "$actual_issuer" && "$actual_issuer" != "$oauth_issuer" ]]; then
        echo "ERROR: OAuth issuer is '$actual_issuer' but docker tests expect '$oauth_issuer'" >&2
        echo "Start the OAuth stack with:" >&2
        echo "  docker compose -f docker/oidc/compose.yaml up -d" >&2
        exit 1
      fi
    fi
    {
      echo ""
      echo "# Appended for docker/tests (Celonis OAuth at ${UC_DOCKER_OAUTH_HOST})"
      grep -v '^#' "$oidc_snippet" | grep -v '^[[:space:]]*$'
    } >>"$props"
    echo "OIDC auth enabled for integration tests (issuer ${oauth_issuer})" >&2
  fi

  TEST_CONF_DIR="$conf_dir"
  echo "$conf_dir"
}

start_binary_server() {
  echo "Starting UC server in binary mode"
  : > "$LOG_FILE"

  local conf_dir truststore java_opts
  conf_dir="$(prepare_test_conf)"
  install_test_conf_overlay "$conf_dir"
  truststore="$(prepare_oauth_truststore || true)"
  if [[ -n "$truststore" ]]; then
    cp "$truststore" "$conf_dir/oauth-truststore.jks"
  fi
  java_opts="$(oauth_java_opts || true)"

  echo "Config overlay: $conf_dir -> etc/conf"

  env ${java_opts:+JAVA_TOOL_OPTIONS="$java_opts"} "storage-root.models=file:///tmp/ucroot" bin/start-uc-server >"$LOG_FILE" 2>&1 &
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
    if [[ -n "$UC_DOCKER_MINIO_NETWORK" ]]; then
      echo "Recreating UC container to join MinIO network ${UC_DOCKER_MINIO_NETWORK}" >&2
      docker rm -f "$UC_DOCKER_CONTAINER_NAME" 2>/dev/null || true
    else
      echo "Reusing running UC server container: $UC_DOCKER_CONTAINER_NAME"
      echo "docker" > "$MODE_FILE"
      echo "$UC_DOCKER_CONTAINER_NAME" > "$CONTAINER_FILE"
      wait_for_server "docker (existing container)" check_container
      echo "UC server container up and running"
      return 0
    fi
  fi

  resolve_docker_minio_settings
  local conf_mount truststore java_opts docker_network_arg=()
  if [[ -n "$UC_DOCKER_MINIO_NETWORK" ]]; then
    docker_network_arg=(--network "$UC_DOCKER_MINIO_NETWORK")
  fi
  conf_mount="$(prepare_test_conf "$UC_DOCKER_MINIO_ENDPOINT")"
  truststore="$(prepare_oauth_truststore || true)"
  java_opts=""
  if [[ -n "$truststore" ]]; then
    java_opts="-Djavax.net.ssl.trustStore=${UC_DOCKER_HOME}/etc/conf/oauth-truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"
    cp "$truststore" "$conf_mount/oauth-truststore.jks"
  fi

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
    "${docker_network_arg[@]}" \
    --add-host=host.docker.internal:host-gateway \
    --add-host="${UC_DOCKER_OAUTH_HOST}:host-gateway" \
    -v "${conf_mount}:${UC_DOCKER_HOME}/etc/conf" \
    -e "storage-root.models=file:///tmp/ucroot" \
    ${java_opts:+-e "JAVA_TOOL_OPTIONS=$java_opts"} \
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
  if [[ "$UC_SERVER_MODE" == "binary" ]]; then
    echo "WARN: stop any docker UC container on this port before expecting the local binary server" >&2
  fi
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
