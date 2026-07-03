# Local MinIO object storage

[S3-compatible](https://min.io/) object storage for Unity Catalog local development.

## 1. Start MinIO

From the repository root:

```sh
docker compose -f docker/compose.yaml up -d
```

Wait until healthy:

```sh
docker compose -f docker/compose.yaml ps
```

| Endpoint | URL | Credentials |
|---|---|---|
| S3 API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| Web console | http://localhost:9001 | `minioadmin` / `minioadmin` |

The `minio-init` job creates bucket **`unity-catalog`** on first start.

## 2. Configure Unity Catalog

Merge `docker/minio/server.properties.snippet` into `etc/conf/server.properties`.

This configures:

- **`s3.*`** — legacy per-bucket credentials for the `unity-catalog` bucket (with MinIO endpoint)
- **`storage-root.tables`** / **`storage-root.models`** — managed storage roots on MinIO

Restart the UC server after changing properties:

```sh
build/sbt server/package   # if not already built
bin/start-uc-server
```

## 3. Verify

Create a managed table (auth disabled):

```sh
bin/uc table create \
  --full_name unity.default.minio_test \
  --columns "id INT" \
  --table_type MANAGED
```

List the table and confirm the storage path is under `s3://unity-catalog/`:

```sh
bin/uc table get --full_name unity.default.minio_test
```

In the MinIO console (http://localhost:9001), browse bucket `unity-catalog` to see objects under `ucroot/`.

## Catalog storage model

The snippet uses **`storage-root.tables`** so managed entities land on MinIO without creating external locations or storage credentials. This matches the legacy per-bucket `s3.*` setup and is intended for local dev only.

For the full external-location workflow (storage credential + external location + catalog `storage_root`), see [docs/server/aws.md](../../docs/server/aws.md). MinIO STS is limited, so the snippet uses static access keys via `s3.sessionToken.0` (test-only path in the server).

Pre-seeded sample data under `etc/data/` remains on the local filesystem. Only **new** managed tables/volumes/models use MinIO. To start fresh on object storage only, remove `etc/db/` and run `build/sbt server/populateTestDB` after applying the snippet (you will need sample Delta files on MinIO for table reads).

## Combine with OIDC

```sh
docker compose -f docker/compose.yaml -f docker/oidc/compose.yaml up -d
```

Merge both `docker/minio/server.properties.snippet` and `docker/oidc/server.properties.docker.snippet` into `etc/conf/server.properties`.

## Stop

```sh
docker compose -f docker/compose.yaml down
```

Add `-v` to remove the `minio-data` volume and wipe bucket contents.
