# Unity Catalog OLake Go Integration

This page shows you how to use [OLake Go](https://olake.io/) to replicate data from databases into Apache Iceberg tables governed by Unity Catalog.

OLake Go is a high-performance, open-source EL (Extract–Load) platform that bridges operational databases and open lakehouse storage, writing Apache Iceberg tables or Parquet files on Amazon S3. It supports incremental sync, change data capture (CDC), and stateful, resumable syncs. OLake Go connects to Unity Catalog through the [Iceberg REST Catalog API](../usage/api/iceberg.md).

## Supported sources

| Source | Full Load | CDC | Incremental |
|---|---|---|---|
| PostgreSQL | Yes | Yes | Yes |
| MySQL | Yes | Yes | Yes |
| MongoDB | Yes | Yes | Yes |
| MSSQL | Yes | Yes | Yes |
| Oracle | Yes | — | Yes |
| DB2 | Yes | — | Yes |
| Amazon S3 | Yes | — | Yes |
| Kafka | — | — | Bounded |

See the [OLake connectors documentation](https://olake.io/docs/connectors/overview/) for details on each source.

## Prerequisites

- A running Unity Catalog server with Iceberg REST API write access enabled (on by default; see [Iceberg REST API](../usage/api/iceberg.md)).
- Object storage (such as AWS S3) configured as an [external location](../server/aws.md) in Unity Catalog.
- OLake Go installed via [Docker Compose](https://olake.io/docs/getting-started/quickstart/) (recommended) or [standalone Docker](https://olake.io/docs/install/docker-cli/).

## Destination configuration

OLake Go writes Iceberg tables through Unity Catalog's Iceberg REST endpoint at `/api/2.1/unity-catalog/iceberg`. Configure the destination as a REST catalog pointing at this endpoint.

Example `destination.json`:

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "catalog_name": "unity",
    "rest_catalog_url": "http://<UC_HOST>:8080/api/2.1/unity-catalog/iceberg",
    "iceberg_s3_path": "unity",
    "aws_region": "<AWS_REGION>",
    "aws_access_key": "<AWS_ACCESS_KEY>",
    "aws_secret_key": "<AWS_SECRET_KEY>",
    "arrow_writes": false
  }
}
```

!!! note
    When using Unity Catalog as the REST catalog, set `iceberg_s3_path` to the **Unity Catalog catalog name** (e.g. `unity`), not an S3 bucket path. OLake sends this value as the Iceberg `warehouse` parameter, and Unity Catalog uses it to identify the catalog.

| Parameter | Required | Description |
|---|---|---|
| `catalog_type` | Yes | Set to `rest` for the Iceberg REST Catalog API. |
| `catalog_name` | No | Name of the OLake internal catalog. Defaults to `olake_iceberg` if empty. |
| `rest_catalog_url` | Yes | Unity Catalog Iceberg REST endpoint. |
| `iceberg_s3_path` | Yes | The name of the catalog in Unity Catalog (e.g. `unity`). Despite the parameter name, this is used as the Iceberg warehouse identifier. |
| `aws_region` | Yes | AWS region of the S3 bucket backing the Unity Catalog tables. |
| `aws_access_key` | No | AWS access key. Optional with IAM roles or instance profiles. |
| `aws_secret_key` | No | AWS secret key. Optional with IAM roles or instance profiles. |
| `arrow_writes` | No | Enable the Arrow-based writer. Defaults to `false`. |

### Authentication

If Unity Catalog has [OAuth authentication](../server/auth.md) enabled, add a bearer token to the configuration:

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "catalog_name": "unity",
    "rest_catalog_url": "http://<UC_HOST>:8080/api/2.1/unity-catalog/iceberg",
    "iceberg_s3_path": "unity",
    "aws_region": "<AWS_REGION>",
    "aws_access_key": "<AWS_ACCESS_KEY>",
    "aws_secret_key": "<AWS_SECRET_KEY>",
    "token": "<BEARER_TOKEN>",
    "arrow_writes": false
  }
}
```

See the [OLake REST catalog configuration](https://olake.io/docs/writers/iceberg/catalog/rest/) for all available authentication options including OAuth2.

## Running a sync

OLake Go can be operated through the [web UI](https://olake.io/docs/getting-started/quickstart/) or the [Docker CLI](https://olake.io/docs/install/docker-cli/).

### Web UI

Start the OLake UI with Docker Compose:

```sh
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose-v1.yml | docker compose -f - up -d
```

Open `http://localhost:8000`, configure a source and the Unity Catalog destination, then run a sync from the UI.

### Docker CLI

First, discover the available streams from the source:

```sh
docker run --pull=always \
  -v "$(pwd)/config:/mnt/config" \
  olakego/source-postgres:latest \
  discover \
  --config /mnt/config/source.json
```

This writes `available_streams.json` and `selected_streams.json` to the config directory. Edit `selected_streams.json` to choose which tables to replicate, then run the sync:

```sh
docker run --pull=always \
  -v "$(pwd)/config:/mnt/config" \
  olakego/source-postgres:latest \
  sync \
  --config /mnt/config/source.json \
  --destination /mnt/config/destination.json \
  --selected-streams /mnt/config/selected_streams.json \
  --available-streams /mnt/config/available_streams.json
```

Replace `source-postgres` with the connector for your source (e.g. `source-mysql`, `source-mongodb`, `source-mssql`).

## More information

- [OLake Go GitHub repository](https://github.com/datazip-inc/olake)
- [OLake Go documentation](https://olake.io/docs)
- [OLake Iceberg REST Catalog configuration](https://olake.io/docs/writers/iceberg/catalog/rest/)
- [OLake Docker CLI reference](https://olake.io/docs/install/docker-cli/)
- [Unity Catalog Iceberg REST API](../usage/api/iceberg.md)
