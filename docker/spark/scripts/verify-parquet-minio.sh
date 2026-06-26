#!/usr/bin/env bash
# Count rows in Parquet at an S3 path using MinIO admin credentials only (no UC).
# Usage: verify-parquet-minio.sh s3://data/tenant/.../path expected_count
set -euo pipefail

S3_PATH="${1:?s3 path required}"
EXPECTED="${2:?expected row count required}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/tmp/spark-verify-conf}"
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

echo "==> Verifying Parquet row count at ${S3_PATH} (expect ${EXPECTED})"
out=$(/opt/spark/bin/spark-sql -e "SELECT count(*) FROM parquet.\`${S3_PATH}\`" 2>&1)
echo "$out"
printf '%s\n' "$out" | grep -qx "${EXPECTED}"
