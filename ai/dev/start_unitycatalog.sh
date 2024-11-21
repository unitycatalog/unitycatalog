#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

ROOT_DIR="${1:-$GITHUB_WORKSPACE}"
MAX_RETRY="36"
CHECK_INTERVAL="5"

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
for i in {1..MAX_RETRY}; do
  if check_server; then
    echo "UC server is ready"
    break
  else
    echo "Waiting for UC server... ($i/${MAX_RETRY})"
    sleep $CHECK_INTERVAL
  fi

  if [ $i -eq 36 ]; then
    echo "UC server failed to start within 3 minutes"
    kill $UC_SERVER_PID
    exit 1
  fi
done

echo "UC server up and running"
