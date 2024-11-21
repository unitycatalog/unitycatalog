#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

ROOT_DIR="${1:-$GITHUB_WORKSPACE}"

echo "Building and starting the Unity Catalog Server"

cd "$ROOT_DIR"

# Start the Unity Catalog server in the background
# Redirect output to a log file for debugging purposes
bin/start-uc-server > uc_server.log 2>&1 &

# Capture the PID of the background process
UC_SERVER_PID=$!

echo "UC server started with PID $UC_SERVER_PID"

# Function to check if the server is up
check_server() {
  curl -s http://localhost:8080/api/2.1/unity-catalog/catalogs > /dev/null
}

# Wait until the server is up or timeout after 60 seconds
echo "Waiting for UC server to be ready..."
for i in {1..30}; do
  if check_server; then
    echo "UC server is ready"
    break
  else
    echo "Waiting for UC server... ($i/30)"
    sleep 2
  fi

  if [ $i -eq 30 ]; then
    echo "UC server failed to start within 60 seconds"
    # Optionally, kill the server process if it's still running
    kill $UC_SERVER_PID
    exit 1
  fi
done

echo "UC server up and running"
