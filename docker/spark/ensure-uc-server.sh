#!/usr/bin/env bash
# Ensure Unity Catalog server is running from this repo (bin/start-uc-server / Maven build).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

UC_PORT="${UC_PORT:-8080}"
UC_LOG="${UC_LOG:-/tmp/unitycatalog-server.log}"
AUTO_START="${AUTO_START_UC_SERVER:-1}"

auth_header() {
  if [[ -f etc/conf/token.txt ]]; then
    echo "Authorization: Bearer $(tr -d '[:space:]' < etc/conf/token.txt)"
  fi
}

uc_ready() {
  local auth
  auth="$(auth_header)"
  if [[ -n "$auth" ]]; then
    curl -sf "http://localhost:${UC_PORT}/api/2.1/unity-catalog/catalogs" \
      -H "$auth" >/dev/null 2>&1
  else
    curl -sf "http://localhost:${UC_PORT}/api/2.1/unity-catalog/catalogs" >/dev/null 2>&1
  fi
}

if uc_ready; then
  echo "==> UC server already running at http://localhost:${UC_PORT}"
  exit 0
fi

if [[ "$AUTO_START" == "0" ]]; then
  echo "FATAL: UC server not reachable at http://localhost:${UC_PORT}" >&2
  echo "Start it from the repo root: bin/start-uc-server" >&2
  echo "Or set AUTO_START_UC_SERVER=1 (default) to start it automatically." >&2
  exit 1
fi

echo "==> Starting UC server from repo (bin/start-uc-server, log: ${UC_LOG})"
nohup bin/start-uc-server >>"$UC_LOG" 2>&1 &
UC_PID=$!
echo "    PID ${UC_PID}"

echo "==> Waiting for UC server"
for _ in $(seq 1 90); do
  if uc_ready; then
    echo "==> UC server ready at http://localhost:${UC_PORT}"
    if [[ -f etc/conf/token.txt ]]; then
      echo "==> Admin token: etc/conf/token.txt"
    fi
    exit 0
  fi
  if ! kill -0 "$UC_PID" 2>/dev/null; then
    echo "FATAL: UC server process exited; see ${UC_LOG}" >&2
    tail -30 "$UC_LOG" >&2 || true
    exit 1
  fi
  sleep 2
done

echo "FATAL: UC server did not become ready within 180s; see ${UC_LOG}" >&2
tail -30 "$UC_LOG" >&2 || true
exit 1
