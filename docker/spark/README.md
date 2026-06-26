# Spark 4.1 in Docker (Unity Catalog + MinIO)

Apache Spark **4.1.2** container with pre-downloaded jars:

| Artifact | Version |
|----------|---------|
| `unitycatalog-spark_4.1_2.13` | 0.5.0-SNAPSHOT (built from repo source in image) |
| `delta-spark_2.13` | 4.2.0 |
| `hadoop-aws` | 3.4.2 |

The UC server always runs **on the host** from this repository (`bin/start-uc-server`, built via Maven/sbt). Docker runs MinIO, a long-running **Spark Thrift Server**, and one-shot `spark` containers for JDBC clients.

## Architecture

```
catalogTest.sh §5          docker compose
     │                          │
     ▼                          ▼
 beeline-uc.sh  ──JDBC──►  spark-thrift:10000
                               │
                               ▼
                    UCSingleCatalog (REST) ──► UC server (host:8080)
                               │
                               ▼
                         MinIO (S3A)
```

- **REST** (`curl`, `bin/uc`): bootstrap, schema/tables/views, drops (sections 1–4, 6 in `docker/test/catalogTest.sh`).
- **JDBC** (`beeline-uc.sh`): data movement — INSERT, MERGE, EXPORT, DELETE (section 5).

## What the smoke test covers

The default test (`test-spark-uc.sh`) exercises **metastore SQL**:

- `SHOW SCHEMAS IN <catalog>`
- `CREATE SCHEMA IF NOT EXISTS <catalog>.spark_test`
- `SHOW TABLES IN <catalog>.spark_test`

`SELECT` / `DESCRIBE` on sample tables requires Delta files on storage reachable from Spark (see MinIO setup below).

## Prerequisites

1. **MinIO** — `docker compose -f docker/compose.yaml up -d`
2. **UC server** — from the repo root:
   ```sh
   build/sbt server/package   # first time, or when server code changes
   bin/start-uc-server
   ```
   `docker/spark/run-test.sh` starts the server automatically if it is not already running.
3. **Auth token** — with `server.authorization=enable`, the server writes `etc/conf/token.txt` on start. `run-test.sh` passes it to Spark via `UC_AUTH_TOKEN`.

### S3 / MinIO configuration

`etc/conf/server.properties` includes settings from `docker/minio/server.properties.snippet`
(`storage-root.*`, MinIO endpoints on `[::1]:9000` for the host UC server).

Spark in Docker does **not** need `docker/spark/server.properties.snippet` in `server.properties`:
the Spark entrypoint sets `spark.hadoop.fs.s3a.endpoint=http://minio:9000` so containers reach
MinIO on the compose network directly.

If you run Spark on the host (not Docker), merge `docker/spark/server.properties.snippet` or point
S3A at `http://localhost:9000` yourself.

## Quick test

From the repository root:

```sh
docker/spark/run-test.sh
```

Override catalog:

```sh
UC_CATALOG=unity docker/spark/run-test.sh   # sample DB catalog after populateTestDB
```

Skip auto-starting UC (fail if not already running):

```sh
AUTO_START_UC_SERVER=0 docker/spark/run-test.sh
```

## Manual usage

```sh
# Build
docker compose -f docker/compose.yaml --profile spark build spark

# Start Thrift Server (long-running; used by docker/test/catalogTest.sh and beeline)
UC_CATALOG=cattest UC_AUTH_TOKEN=$(tr -d '[:space:]' < etc/conf/token.txt) \
  docker compose -f docker/compose.yaml --profile spark up -d --wait spark-thrift

# JDBC via beeline (schema s1 must exist in UC)
docker compose -f docker/compose.yaml --profile spark run --rm --no-deps \
  -e UC_SCHEMA=s1 spark /opt/spark/uc-scripts/beeline-uc.sh "SELECT 1"

# Smoke test (default entrypoint; UC must be running on host)
docker compose -f docker/compose.yaml --profile spark run --rm spark

# Interactive spark-sql (one-shot, no Thrift)
docker compose -f docker/compose.yaml --profile spark run --rm spark \
  /opt/spark/uc-scripts/spark-sql-uc.sh

# Single statement (one-shot spark-sql)
docker compose -f docker/compose.yaml --profile spark run --rm spark \
  /opt/spark/uc-scripts/spark-sql-uc.sh "SHOW SCHEMAS"

# Full catalog lifecycle test (REST + JDBC)
bash docker/test/catalogTest.sh
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `UC_SERVER_URI` | `http://host.docker.internal:8080` | UC server base URL (host) |
| `UC_CATALOG` | `catA` | Catalog name for `spark.sql.catalog.*` |
| `UC_AUTH_TOKEN` | _(from `etc/conf/token.txt` in run-test)_ | Bearer token when auth is enabled |
| `UC_AUTH_TOKEN_FILE` | `/etc/uc/token.txt` | Alternative token file path inside container |
| `UC_SCHEMA` | `default` | JDBC schema/database in beeline URL (`jdbc:hive2://…/UC_SCHEMA`) |
| `SPARK_THRIFT_HOST` | `spark-thrift` | Thrift Server hostname (compose service name) |
| `SPARK_THRIFT_PORT` | `10000` | Thrift Server port |
| `MINIO_ENDPOINT` | `http://minio:9000` | Fallback S3A endpoint inside compose network |
| `AUTO_START_UC_SERVER` | `1` | `run-test.sh` / `ensure-uc-server.sh` start UC if down |

## Spark configuration

Runtime catalog settings are written by `scripts/entrypoint.sh`. Shared defaults
(Delta extensions, S3A path-style access) live in `spark-defaults.conf`.

Equivalent host-side `spark-sql` (without Docker):

```sh
bin/spark-sql --master 'local[*]' \
  --packages 'org.apache.hadoop:hadoop-aws:3.4.2,io.delta:delta-spark_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0' \
  --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,io.unitycatalog.spark.UCSparkSessionExtensions' \
  --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' \
  --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
  --conf 'spark.sql.catalog.catA=io.unitycatalog.spark.UCSingleCatalog' \
  --conf 'spark.sql.catalog.catA.uri=http://localhost:8080' \
  --conf "spark.sql.catalog.catA.token=$(cat etc/conf/token.txt)" \
  --conf 'spark.sql.defaultCatalog=catA'
```
