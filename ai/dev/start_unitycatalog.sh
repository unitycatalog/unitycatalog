#!/usr/bin/env bash
set -euo pipefail

LOOP_COUNT=${LOOP_COUNT:-48}
SLEEP_DURATION=${SLEEP_DURATION:-5}
HEALTH_URL=${HEALTH_URL:-http://localhost:8080/api/2.1/unity-catalog/catalogs}
ROOT_DIR="${1:-${GITHUB_WORKSPACE:-$(pwd)}}"

echo "Building and starting the Unity Catalog Server"
cd "$ROOT_DIR"

# -------- Build hardening (avoid Coursier lock races) --------
# Give this run an isolated cache to avoid OverlappingFileLockException
export COURSIER_CACHE="${COURSIER_CACHE:-${RUNNER_TEMP:-/tmp}/coursier-${GITHUB_RUN_ID:-$$}}"
mkdir -p "$COURSIER_CACHE" || true

# Keep temp local
export SBT_OPTS="${SBT_OPTS:-} -Djava.io.tmpdir=${RUNNER_TEMP:-/tmp}"
export SBT_LOG_NOFORMAT=true

# Remove any stale coursier lock files (safe)
find "$COURSIER_CACHE" -type f -name '*.lock' -delete 2>/dev/null || true

SBT_CMD="${SBT_CMD:-./build/sbt -info}"

run_sbt_with_retries() {
  local cmd="$1"
  local max=3
  local n=1
  while true; do
    echo "Running: $cmd (attempt $n/$max)"
    if eval "$cmd"; then
      return 0
    fi
    if [[ $n -ge $max ]]; then
      echo "SBT command failed after $max attempts: $cmd"
      return 1
    fi
    echo "Retrying in $((n*5))s..."
    sleep $((n*5))
    # Clean any locks that might have been left by a crash
    find "$COURSIER_CACHE" -type f -name '*.lock' -delete 2>/dev/null || true
    ((n++))
  done
}

# Ensure the server jar exists before starting
if ! ls server/target/unitycatalog-server*.jar >/dev/null 2>&1; then
  echo "Server JAR not found. Building with SBT..."
  run_sbt_with_retries "$SBT_CMD clean update"
  run_sbt_with_retries "$SBT_CMD package"
else
  echo "Found existing server JAR."
fi

# -------- Start server --------
LOG_FILE="${LOG_FILE:-uc_server.log}"
: > "$LOG_FILE"

# Cleanup function for when server fails to start (only called on error paths)
cleanup_on_failure() {
  if [[ -n "${UC_SERVER_PID:-}" ]] && kill -0 "$UC_SERVER_PID" 2>/dev/null; then
    echo "Cleaning up failed UC server (PID $UC_SERVER_PID)..."
    kill "$UC_SERVER_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$UC_SERVER_PID" 2>/dev/null || true
  fi
  rm -f uc_server.pid
}

echo "Starting UC server..."
bin/start-uc-server >"$LOG_FILE" 2>&1 &
UC_SERVER_PID=$!
echo "$UC_SERVER_PID" > uc_server.pid
echo "UC server started with PID $UC_SERVER_PID"

check_process() {
  if ! kill -0 "$UC_SERVER_PID" 2>/dev/null; then
    echo "ERROR: UC server process (PID $UC_SERVER_PID) died unexpectedly!"
    echo "=== Server Logs (tail) ==="
    tail -n 200 "$LOG_FILE" || true
    echo "=========================="
    cleanup_on_failure
    return 1
  fi
  return 0
}

check_server() {
  # Fail on non-2xx and cap each probe to 3s
  curl --silent --show-error --fail --max-time 3 "$HEALTH_URL" >/dev/null
}

echo "Waiting for UC server to be ready..."
for (( i=1; i<=LOOP_COUNT; i++ )); do
  if ! check_process; then
    exit 1
  fi

  if check_server; then
    echo "UC server is ready"
    echo "UC server up and running"
    exit 0
  fi

  echo "Waiting for UC server... ($i/$LOOP_COUNT)"
  sleep "$SLEEP_DURATION"
done

echo "UC server failed to start within $((LOOP_COUNT * SLEEP_DURATION)) seconds"
echo "=== Server Logs (tail) ==="
tail -n 400 "$LOG_FILE" || true
echo "=========================="
cleanup_on_failure
exit 1
