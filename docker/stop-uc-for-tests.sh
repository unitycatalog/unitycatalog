#!/usr/bin/env bash
# Stop Unity Catalog started by docker/start-uc-for-tests.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${1:-${GITHUB_WORKSPACE:-$(cd "$SCRIPT_DIR/.." && pwd)}}"

MODE_FILE="$ROOT_DIR/uc_server.mode"
PID_FILE="$ROOT_DIR/uc_server.pid"
CONTAINER_FILE="$ROOT_DIR/uc_server.container"

CONF_BACKUP_MARKER="$ROOT_DIR/uc_server.conf_backup"
ETC_CONF_LINK="$ROOT_DIR/etc/conf"

restore_binary_test_conf() {
  if [[ ! -L "$ETC_CONF_LINK" && ! -f "$CONF_BACKUP_MARKER" ]]; then
    return 0
  fi
  local conf_dir=""
  if [[ -L "$ETC_CONF_LINK" ]]; then
    conf_dir="$(readlink "$ETC_CONF_LINK")"
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
  if [[ -n "$conf_dir" && "$conf_dir" == *uc-test-conf* && -d "$conf_dir" ]]; then
    rm -rf "$conf_dir"
  fi
}

cd "$ROOT_DIR"

echo "Stopping Unity Catalog Server..."

if [[ -f "$MODE_FILE" ]] && [[ "$(cat "$MODE_FILE")" == "external" ]]; then
  echo "UC_SERVER_MODE=external — leaving running server untouched"
  rm -f "$MODE_FILE" "$CONTAINER_FILE" "$PID_FILE"
  exit 0
fi

if [[ -f "$MODE_FILE" ]] && [[ "$(cat "$MODE_FILE")" == "docker" ]]; then
  CONTAINER_NAME="${UC_DOCKER_CONTAINER_NAME:-}"
  if [[ -z "$CONTAINER_NAME" ]] && [[ -f "$CONTAINER_FILE" ]]; then
    CONTAINER_NAME="$(cat "$CONTAINER_FILE")"
  fi
  CONTAINER_NAME="${CONTAINER_NAME:-uc-test-server}"

  if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "Removing UC server container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" || true
    echo "UC server container stopped successfully"
  else
    echo "No UC server container named '$CONTAINER_NAME' found (may already be stopped)"
  fi

  rm -f "$MODE_FILE" "$CONTAINER_FILE" "$PID_FILE"
  echo "Teardown complete"
  exit 0
fi

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  echo "Found PID file with PID: $PID"
  if kill -0 "$PID" 2>/dev/null; then
    echo "Killing UC server (PID $PID)..."
    kill "$PID" || echo "Failed to kill PID $PID"
    sleep 2
    if kill -0 "$PID" 2>/dev/null; then
      echo "Force killing UC server (PID $PID)..."
      kill -9 "$PID" 2>/dev/null || true
    fi
    echo "UC server stopped successfully"
  else
    echo "Process with PID $PID is not running"
  fi
  rm -f "$PID_FILE" "$MODE_FILE" "$CONTAINER_FILE"
  restore_binary_test_conf
  exit 0
fi

echo "No PID file found. Attempting to find UC server process..."
UC_PIDS=$(pgrep -f 'io.unitycatalog.server.UnityCatalogServer' || true)
if [[ -n "$UC_PIDS" ]]; then
  echo "Found UC server process(es): $UC_PIDS"
  echo "$UC_PIDS" | xargs kill || echo "Failed to kill some processes"
  sleep 2
  UC_PIDS=$(pgrep -f 'io.unitycatalog.server.UnityCatalogServer' || true)
  if [[ -n "$UC_PIDS" ]]; then
    echo "Force killing remaining process(es): $UC_PIDS"
    echo "$UC_PIDS" | xargs kill -9 2>/dev/null || true
  fi
  echo "UC server stopped successfully"
else
  echo "No UC server process found (may already be stopped)"
fi

rm -f "$MODE_FILE" "$CONTAINER_FILE" "$PID_FILE"
restore_binary_test_conf
echo "Teardown complete"
