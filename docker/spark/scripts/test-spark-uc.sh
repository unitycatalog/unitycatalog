#!/usr/bin/env bash
# Basic Spark SQL smoke test against Unity Catalog.
set -euo pipefail

UC_CATALOG="${UC_CATALOG:-catA}"
TEST_SCHEMA="spark_test"

log() { echo "==> $*"; }
run_sql() {
  log "SQL: $1"
  /opt/spark/bin/spark-sql --conf "spark.ui.showConsoleProgress=false" -e "$1"
  echo ""
}

log "Spark version: $(/opt/spark/bin/spark-submit --version 2>&1 | head -1)"
log "UC server: ${UC_SERVER_URI}"
log "UC catalog: ${UC_CATALOG}"
log "MinIO endpoint: ${MINIO_ENDPOINT}"
echo ""

run_sql "SHOW SCHEMAS IN ${UC_CATALOG}"
run_sql "CREATE SCHEMA IF NOT EXISTS ${UC_CATALOG}.${TEST_SCHEMA}"
run_sql "SHOW TABLES IN ${UC_CATALOG}.${TEST_SCHEMA}"
run_sql "SHOW SCHEMAS IN ${UC_CATALOG}"

log "Spark + Unity Catalog smoke test passed (metastore SQL)."
