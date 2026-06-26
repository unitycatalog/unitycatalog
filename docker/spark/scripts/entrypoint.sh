#!/usr/bin/env bash
set -euo pipefail

UC_SERVER_URI="${UC_SERVER_URI:-http://host.docker.internal:8080}"
UC_CATALOG="${UC_CATALOG:-catA}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

if [[ -z "${UC_AUTH_TOKEN:-}" && -f "${UC_AUTH_TOKEN_FILE:-}" ]]; then
  UC_AUTH_TOKEN="$(tr -d '[:space:]' < "${UC_AUTH_TOKEN_FILE}")"
fi

if [[ -z "${UC_AUTH_TOKEN:-}" ]]; then
  UC_AUTH_TOKEN="not-used"
fi

export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/tmp/spark-uc-conf}"
mkdir -p "${SPARK_CONF_DIR}"
cp /opt/spark/conf/spark-defaults.conf "${SPARK_CONF_DIR}/spark-defaults.conf"

# Runtime catalog wiring (spark-defaults.conf holds shared Delta/S3A settings).
cat >>"${SPARK_CONF_DIR}/spark-defaults.conf" <<EOF

# --- Unity Catalog (generated at container start) ---
spark.master=local[*]
spark.sql.catalog.${UC_CATALOG}=io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.${UC_CATALOG}.uri=${UC_SERVER_URI}
spark.sql.catalog.${UC_CATALOG}.token=${UC_AUTH_TOKEN}
spark.sql.catalog.${UC_CATALOG}.warehouse=${UC_CATALOG}
spark.sql.defaultCatalog=${UC_CATALOG}
spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}
EOF

if [[ $# -eq 0 ]]; then
  set -- /opt/spark/uc-scripts/test-spark-uc.sh
fi

exec "$@"
