#!/usr/bin/env bash
# Build the Spark image and run basic SQL against Unity Catalog on the host.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

COMPOSE="docker compose --profile spark -f docker/compose.yaml"
UC_CATALOG="${UC_CATALOG:-catA}"

echo "==> Start MinIO"
$COMPOSE up -d --remove-orphans minio minio-init

echo "==> Ensure UC server from repo (bin/start-uc-server)"
"$(dirname "$0")/ensure-uc-server.sh"

echo "==> Build Spark 4.1 image (Unity Catalog + Delta + hadoop-aws)"
$COMPOSE build spark

SPARK_RUN_ARGS=()
if [[ -f etc/conf/token.txt ]]; then
  SPARK_RUN_ARGS+=(-e "UC_AUTH_TOKEN=$(tr -d '[:space:]' < etc/conf/token.txt)")
fi

echo "==> Run Spark SQL smoke test (catalog: ${UC_CATALOG})"
UC_CATALOG="$UC_CATALOG" $COMPOSE run --rm "${SPARK_RUN_ARGS[@]}" spark

echo ""
echo "Done. Interactive shell:"
echo "  UC_CATALOG=${UC_CATALOG} $COMPOSE run --rm spark /opt/spark/uc-scripts/spark-sql-uc.sh"
