#!/usr/bin/env bash
# Long-running Spark Thrift JDBC/ODBC server wired to Unity Catalog (UCSingleCatalog → UC REST).
# SPARK_CONF_DIR must already contain UC catalog settings (see entrypoint.sh).
set -euo pipefail

THRIFT_BIND_HOST="${THRIFT_BIND_HOST:-0.0.0.0}"
THRIFT_PORT="${THRIFT_PORT:-10000}"

echo "==> Starting Spark Thrift Server (UC catalog via JDBC session conf)"
echo "    JDBC: jdbc:hive2://localhost:${THRIFT_PORT} (bind ${THRIFT_BIND_HOST}:${THRIFT_PORT})"

/opt/spark/sbin/start-thriftserver.sh \
  --hiveconf "hive.server2.thrift.bind.host=${THRIFT_BIND_HOST}" \
  --hiveconf "hive.server2.thrift.port=${THRIFT_PORT}"

echo "==> Waiting for Thrift listener on port ${THRIFT_PORT}"
for _ in $(seq 1 90); do
  if timeout 1 bash -c "echo > /dev/tcp/127.0.0.1/${THRIFT_PORT}" 2>/dev/null; then
    echo "==> Spark Thrift Server ready"
    # start-thriftserver.sh daemonizes; keep the container alive.
    exec tail -f /dev/null
  fi
  sleep 1
done

echo "FATAL: Spark Thrift Server did not open port ${THRIFT_PORT}" >&2
exit 1
