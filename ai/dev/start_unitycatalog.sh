#!/bin/bash
set -e

LOOP_COUNT=48
SLEEP_DURATION=5

ROOT_DIR="${1:-$GITHUB_WORKSPACE}"

echo "Building and starting the Unity Catalog Server"

cd "$ROOT_DIR"

# Start the Unity Catalog server in the background
bin/start-uc-server > uc_server.log 2>&1 &

UC_SERVER_PID=$!

echo "UC server started with PID $UC_SERVER_PID"

# Function to check if the server process is still running
check_process() {
  if ! kill -0 "$UC_SERVER_PID" 2>/dev/null; then
    echo "ERROR: UC server process (PID $UC_SERVER_PID) has died unexpectedly!"
    echo "=== Server Logs ==="
    cat uc_server.log
    echo "=================="
    return 1
  fi
  return 0
}

check_server() {
  curl -s http://localhost:8080/api/2.1/unity-catalog/catalogs > /dev/null
}

echo "Waiting for UC server to be ready..."
for (( i=1; i<=LOOP_COUNT; i++ )); do
  # First check if the process is still alive
  if ! check_process; then
    exit 1
  fi
  
  if check_server; then
    echo "UC server is ready"
    break
  else
    echo "Waiting for UC server... ($i/$LOOP_COUNT)"
    sleep "$SLEEP_DURATION"
  fi

  if [ "$i" -eq "$LOOP_COUNT" ]; then
    echo "UC server failed to start within $((LOOP_COUNT * SLEEP_DURATION)) seconds"
    echo "=== Server Logs ==="
    cat uc_server.log
    echo "=================="
    kill "$UC_SERVER_PID" 2>/dev/null || true
    exit 1
  fi
done

echo "UC server up and running"
