#!/usr/bin/env bash
# Write random Parquet files to an S3A path using MinIO admin credentials only (no UC).
# Usage: seed-parquet-minio.sh s3a://data/tenant/MYID/landing/batch1 [row_count]
set -euo pipefail

DEST="${1:?destination s3a path required}"
ROW_COUNT="${2:-100}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/tmp/spark-seed-conf}"
mkdir -p "${SPARK_CONF_DIR}"
cat >"${SPARK_CONF_DIR}/spark-defaults.conf" <<EOF
spark.master=local[*]
spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}
spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}
spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
EOF

echo "==> Seeding Parquet at ${DEST} (${ROW_COUNT} rows, no UC)"
/opt/spark/bin/spark-sql -e "
INSERT OVERWRITE DIRECTORY '${DEST}'
USING parquet
SELECT
  cast(id AS int) AS id,
  concat('row-', cast(id AS string)) AS label,
  rand() AS value
FROM range(${ROW_COUNT});
"
