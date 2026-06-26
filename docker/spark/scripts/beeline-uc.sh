#!/usr/bin/env bash
# Run SQL against the Spark Thrift Server via JDBC (beeline).
set -euo pipefail

if [[ -z "${HOME:-}" || "${HOME}" == /nonexistent ]]; then
  export HOME=/tmp
fi
mkdir -p "${HOME}/.beeline" 2>/dev/null || true

SPARK_THRIFT_HOST="${SPARK_THRIFT_HOST:-spark-thrift}"
SPARK_THRIFT_PORT="${SPARK_THRIFT_PORT:-10000}"
UC_SCHEMA="${UC_SCHEMA:-default}"
JDBC_URL="jdbc:hive2://${SPARK_THRIFT_HOST}:${SPARK_THRIFT_PORT}/${UC_SCHEMA}"

if [[ $# -eq 0 ]]; then
  exec /opt/spark/bin/beeline -u "${JDBC_URL}"
fi

if [[ $# -eq 1 && -f "$1" ]]; then
  exec /opt/spark/bin/beeline -u "${JDBC_URL}" -f "$1"
fi

sql=""
for stmt in "$@"; do
  if [[ -n "$sql" ]]; then
    sql="${sql}; "
  fi
  sql="${sql}${stmt}"
done
if [[ "${sql}" != *";" ]]; then
  sql="${sql};"
fi

exec /opt/spark/bin/beeline --silent=true --showHeader=false -u "${JDBC_URL}" -e "${sql}"
