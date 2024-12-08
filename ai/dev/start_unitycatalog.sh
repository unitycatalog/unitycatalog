#!/bin/bash
set -e

LOOP_COUNT=36
SLEEP_DURATION=5

ROOT_DIR="${1:-$GITHUB_WORKSPACE}"

echo "Building and starting the Unity Catalog Server"

cd "$ROOT_DIR"

# Start the Unity Catalog server in the background
bin/start-uc-server > uc_server.log 2>&1 &

UC_SERVER_PID=$!

echo "UC server started with PID $UC_SERVER_PID"

check_server() {
  curl -s http://localhost:8080/api/2.1/unity-catalog/catalogs > /dev/null
}

echo "Waiting for UC server to be ready..."
for (( i=1; i<=LOOP_COUNT; i++ )); do
  if check_server; then
    echo "UC server is ready"
    break
  else
    echo "Waiting for UC server... ($i/$LOOP_COUNT)"
    sleep "$SLEEP_DURATION"
  fi

  if [ "$i" -eq "$LOOP_COUNT" ]; then
    echo "UC server failed to start within $((LOOP_COUNT * SLEEP_DURATION)) seconds"
    kill "$UC_SERVER_PID"
    exit 1
  fi
done

echo "UC server up and running"
