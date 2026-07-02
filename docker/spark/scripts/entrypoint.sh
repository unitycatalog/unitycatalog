#!/usr/bin/env bash
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/tmp/spark-uc-conf}"
mkdir -p "${SPARK_CONF_DIR}"
cp /opt/spark/conf/spark-defaults.conf "${SPARK_CONF_DIR}/spark-defaults.conf"

# Shared Spark/MinIO defaults. Per-tenant UC catalog settings are passed via JDBC session conf.
cat >>"${SPARK_CONF_DIR}/spark-defaults.conf" <<EOF

# --- docker/spark shared runtime ---
spark.master=local[*]
spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}
EOF

if [[ $# -eq 0 ]]; then
  set -- /opt/spark/uc-scripts/test-spark-uc.sh
fi

exec "$@"
