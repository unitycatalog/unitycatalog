#!/usr/bin/env bash
# Run spark-sql against Unity Catalog with docker/spark image defaults.
set -euo pipefail

if [[ $# -eq 0 ]]; then
  exec /opt/spark/bin/spark-sql
fi

if [[ $# -eq 1 && -f "$1" ]]; then
  exec /opt/spark/bin/spark-sql -f "$1"
fi

# Pass SQL statements as arguments (joined with semicolons).
sql=""
for stmt in "$@"; do
  if [[ -n "$sql" ]]; then
    sql="${sql}; "
  fi
  sql="${sql}${stmt}"
done
exec /opt/spark/bin/spark-sql -e "${sql}"
