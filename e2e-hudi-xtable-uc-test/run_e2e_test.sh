#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

UC_CONTAINER="uc-e2e-test"
UC_PORT=8090

if [ ! -f "$SCRIPT_DIR/.env" ]; then
  echo "ERROR: .env file not found. Copy .env.template to .env and fill in your ADLS credentials."
  exit 1
fi
set -a; source "$SCRIPT_DIR/.env"; set +a

for var in TENANT_ID CLIENT_ID CLIENT_SECRET STORAGE_ACCOUNT_NAME CONTAINER_NAME TABLE_PATH; do
  if [ -z "${!var:-}" ] || [[ "${!var}" == your-* ]]; then
    echo "ERROR: $var is not set or still has placeholder value in .env"; exit 1
  fi
done

cleanup() {
  echo ""; echo "Cleanup"
  docker rm -f "$UC_CONTAINER" 2>/dev/null || true
  rm -f /tmp/uc-e2e-server.properties
}
trap cleanup EXIT

echo "Ensuring UC Docker image"
if [ "${REBUILD_IMAGE:-false}" = "true" ] || ! docker image inspect unitycatalog-test:local >/dev/null 2>&1; then
  echo "  Building UC image (--no-cache)..."
  docker build --no-cache -t unitycatalog-test:local "$REPO_ROOT"
else
  echo "  Using existing image. Set REBUILD_IMAGE=true to force rebuild."
fi
echo "  UC image ready."

envsubst < "$SCRIPT_DIR/uc/server.properties.tpl" > /tmp/uc-e2e-server.properties

echo ""
echo "Starting Unity Catalog server (port $UC_PORT)"
docker rm -f "$UC_CONTAINER" 2>/dev/null || true
docker run -d --name "$UC_CONTAINER" \
  -p "${UC_PORT}:8080" \
  -v "/tmp/uc-e2e-server.properties:/home/unitycatalog/etc/conf/server.properties" \
  unitycatalog-test:local

echo -n "  Waiting for server"
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${UC_PORT}/api/2.1/unity-catalog/catalogs" >/dev/null 2>&1; then
    echo " ready."; break
  fi
  echo -n "."; sleep 2
  if [ "$i" -eq 30 ]; then echo " TIMEOUT"; docker logs "$UC_CONTAINER"; exit 1; fi
done

echo ""
echo "Building and running Java E2E test"
cd "$SCRIPT_DIR"
export UC_URL="http://localhost:${UC_PORT}"
mvn -q compile exec:exec 2>&1

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "Java test failed. UC server logs:"
  docker logs "$UC_CONTAINER" 2>&1 | tail -30
fi
exit $EXIT_CODE
