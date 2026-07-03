#!/usr/bin/env bash
# Shared helpers for docker/tests JUnit integration suite.
set -euo pipefail

_uc_lib_dir() {
  if [[ -n "${BASH_SOURCE:-}" ]]; then
    cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
  else
    cd "$(dirname "$0")" && pwd
  fi
}

UC_JAVA_ROOT="${UC_JAVA_ROOT:-$(cd "$(_uc_lib_dir)/../.." && pwd)}"
UC_TESTS_POM="$UC_JAVA_ROOT/docker/tests/pom.xml"
UC_VERSION="0.5.0-SNAPSHOT"
UC_CLIENT_JAR="$UC_JAVA_ROOT/clients/java/target/unitycatalog-client-${UC_VERSION}.jar"
UC_CONTROL_API_JAR="$UC_JAVA_ROOT/target/control/java/target/unitycatalog-controlapi-${UC_VERSION}.jar"
UC_CONTROL_MODELS_JAR="$UC_JAVA_ROOT/server/target/controlmodels/target/unitycatalog-controlmodels-${UC_VERSION}.jar"

# Portable timeout (macOS has no GNU timeout; brew coreutils installs gtimeout).
# Kills the full process tree so Maven/Surefire cannot outlive the limit.
run_with_timeout() {
  local secs="$1"
  shift
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout --signal=TERM --kill-after=10 "$secs" "$@"
    return $?
  fi
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=10 "$secs" "$@"
    return $?
  fi

  "$@" &
  local pid=$!
  local watcher
  (
    sleep "$secs"
    if kill -0 "$pid" 2>/dev/null; then
      echo "==> TIMEOUT after ${secs}s — stopping test run (pid $pid)" >&2
      pkill -TERM -P "$pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true
      sleep 3
      pkill -KILL -P "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
    fi
  ) &
  watcher=$!
  set +e
  wait "$pid"
  local rc=$?
  set -e
  kill "$watcher" 2>/dev/null || true
  wait "$watcher" 2>/dev/null || true
  if kill -0 "$pid" 2>/dev/null; then
    return 124
  fi
  return "$rc"
}

_uc_java_install_local_jar() {
  local file="$1" artifact="$2"
  mvn -q install:install-file \
    -Dfile="$file" \
    -DgroupId=io.unitycatalog \
    -DartifactId="$artifact" \
    -Dversion="$UC_VERSION" \
    -Dpackaging=jar
}

ensure_uc_server() {
  if [[ "${DOCKER_TESTS_ENSURE_UC:-1}" == "0" ]]; then
    return
  fi

  local mode="${UC_SERVER_MODE:-binary}"
  echo "==> Ensuring UC server (mode: ${mode})" >&2
  if [[ "$mode" == "docker" && -z "${UC_DOCKER_IMAGE:-}" ]]; then
    echo "ERROR: UC_DOCKER_IMAGE is required when UC_SERVER_MODE=docker" >&2
    exit 1
  fi
  UC_OAUTH_BASE_URL="$(resolve_oauth_base_url)"
  UC_SERVER_MODE="$mode" UC_DOCKER_IMAGE="${UC_DOCKER_IMAGE:-}" UC_ENABLE_OIDC="${UC_ENABLE_OIDC:-1}" \
    UC_OAUTH_BASE_URL="$UC_OAUTH_BASE_URL" \
    "$UC_JAVA_ROOT/docker/start-uc-for-tests.sh" "$UC_JAVA_ROOT"
}

resolve_oauth_base_url() {
  if [[ -n "${UC_OAUTH_BASE_URL:-}" ]]; then
    echo "$UC_OAUTH_BASE_URL"
    return
  fi
  echo "http://dev.dev.celonis.cloud:9010"
}

resolve_oauth_team_id() {
  if [[ -n "${UC_OAUTH_TEAM_ID:-}" ]]; then
    echo "$UC_OAUTH_TEAM_ID"
    return
  fi
  if command -v docker >/dev/null 2>&1; then
    local team_id
    team_id="$(docker compose -f "$UC_JAVA_ROOT/docker/oidc/compose.yaml" exec -T postgres \
      psql -U celonis -d team -tAc "SELECT cpm_id FROM cpm_team_team WHERE cpm_domain='dev' LIMIT 1" 2>/dev/null \
      | tr -d '[:space:]')"
    if [[ -n "$team_id" ]]; then
      echo "$team_id"
      return
    fi
  fi
  echo ""
}

docker_test_env() {
  local oauth_url oauth_team_id
  oauth_url="$(resolve_oauth_base_url)"
  oauth_team_id="$(resolve_oauth_team_id)"
  if [[ -n "$oauth_url" ]]; then
    echo "UC_OAUTH_BASE_URL=$oauth_url"
  fi
  if [[ -n "$oauth_team_id" ]]; then
    echo "UC_OAUTH_TEAM_ID=$oauth_team_id"
  fi
}

stop_uc_server() {
  if [[ -x "$UC_JAVA_ROOT/docker/stop-uc-for-tests.sh" ]]; then
    "$UC_JAVA_ROOT/docker/stop-uc-for-tests.sh" "$UC_JAVA_ROOT"
  fi
}

ensure_uc_client_jars() {
  if [[ ! -f "$UC_CLIENT_JAR" || ! -f "$UC_CONTROL_API_JAR" || ! -f "$UC_CONTROL_MODELS_JAR" ]]; then
    echo "==> Building Unity Catalog Java client + control API (first run, ~1-2 min)" >&2
    build/sbt -batch "controlModels/compile; controlApi/compile; client/package" >/dev/null
  fi
  local stamp="$UC_JAVA_ROOT/docker/tests/target/.uc-client-jars-installed"
  local client_mtime
  if [[ "$(uname -s)" == "Darwin" ]]; then
    client_mtime="$(stat -f '%m' "$UC_CLIENT_JAR")"
  else
    client_mtime="$(stat -c '%Y' "$UC_CLIENT_JAR")"
  fi
  if [[ -f "$stamp" && "$(cat "$stamp")" == "$client_mtime" ]]; then
    return
  fi
  mkdir -p "$(dirname "$stamp")"
  _uc_java_install_local_jar "$UC_CLIENT_JAR" unitycatalog-client
  _uc_java_install_local_jar "$UC_CONTROL_API_JAR" unitycatalog-controlapi
  _uc_java_install_local_jar "$UC_CONTROL_MODELS_JAR" unitycatalog-controlmodels
  echo "$client_mtime" >"$stamp"
}

run_docker_tests() {
  local test_class="$1"
  shift
  local timeout_secs="${DOCKER_TESTS_TIMEOUT_SECS:-600}"
  local log="${DOCKER_TESTS_LOG:-$UC_JAVA_ROOT/docker/tests/target/last-test-run.log}"
  ensure_uc_server
  ensure_uc_client_jars
  mkdir -p "$(dirname "$log")"
  echo "==> Running $test_class (timeout ${timeout_secs}s, ETA ~2-4 min)" >&2
  echo "==> Live log: $log" >&2
  set -o pipefail
  run_with_timeout "$timeout_secs" env UC_REPO_ROOT="$UC_JAVA_ROOT" $(docker_test_env) \
    mvn -f "$UC_TESTS_POM" test -Dtest="$test_class" "$@" 2>&1 | tee "$log"
  return "${PIPESTATUS[0]}"
}

run_all_docker_tests() {
  local timeout_secs="${DOCKER_TESTS_TIMEOUT_SECS:-480}"
  local log="${DOCKER_TESTS_LOG:-$UC_JAVA_ROOT/docker/tests/target/last-test-run.log}"
  ensure_uc_server
  ensure_uc_client_jars
  mkdir -p "$(dirname "$log")"
  echo "==> Running all docker tests (hard timeout ${timeout_secs}s, ETA ~4-6 min)" >&2
  echo "==> Started at $(date '+%H:%M:%S'); will abort after ${timeout_secs}s" >&2
  echo "==> Live log: $log  (tail -f \"$log\" in another terminal)" >&2
  set -o pipefail
  run_with_timeout "$timeout_secs" env UC_REPO_ROOT="$UC_JAVA_ROOT" $(docker_test_env) \
    mvn -f "$UC_TESTS_POM" test "$@" 2>&1 | tee "$log"
  return "${PIPESTATUS[0]}"
}

bootstrap_tenant_java() {
  ensure_uc_server
  ensure_uc_client_jars
  local args=""
  for arg in "$@"; do
    args="$args $(printf '%q' "$arg")"
  done
  UC_REPO_ROOT="$UC_JAVA_ROOT" mvn -q -f "$UC_TESTS_POM" \
    org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
    -Dexec.mainClass=io.unitycatalog.docker.tests.support.BootstrapCli \
    -Dexec.args="$args"
}
