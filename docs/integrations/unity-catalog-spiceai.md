# Unity Catalog Spice OSS Integration

Spice OSS is a unified SQL query interface and portable runtime to locally materialize, accelerate, and query datasets across databases, data warehouses, and data lakes.

Unity Catalog and Databricks Unity Catalog can be used with [Spice OSS](https://github.com/spiceai/spiceai) as [Catalog Connectors](https://docs.spiceai.org/components/catalogs) to make catalog tables available for query in Spice.

Follow the guide below to connect to open-source Unity Catalog. See the [Databricks Unity Catalog quickstart](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/databricks/README.md) if using Databricks Unity Catalog.

## Prerequisites

- **Spice OSS installed:** To install Spice, see [Spice OSS Installation](https://docs.spiceai.org/installation).
- Access to an open-source Unity Catalog server with 1+ tables.

## Step 1. Create a new directory and initialize a [Spicepod](https://docs.spiceai.org/reference/spicepod)

```sh
mkdir uc_quickstart
cd uc_quickstart
spice init
```

## Step 2. Add the Unity Catalog Connector

Configure the `spicepod.yaml` with:

```yaml
catalogs:
  - from: unity_catalog:https://<unity_catalog_host>/api/2.1/unity-catalog/catalogs/<catalog_name>
    name: uc_quickstart
    params:
      # Configure the object store credentials here
```

The Unity Catalog connector currently supports Delta Lake tables only and requires object store credentials.

## Step 3. Configure Delta Lake tables object store credentials

### AWS S3

```yaml
params:
  unity_catalog_aws_access_key_id: ${env:AWS_ACCESS_KEY_ID}
  unity_catalog_aws_secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
  unity_catalog_aws_region: <region> # E.g. us-east-1, us-west-2
  unity_catalog_aws_endpoint: <endpoint> # If using an S3-compatible service, like Minio
```

Set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to the AWS access key and secret key, respectively.

### Azure Storage

```yaml
params:
  unity_catalog_azure_storage_account_name: ${env:AZURE_ACCOUNT_NAME}
  unity_catalog_azure_account_key: ${env:AZURE_ACCOUNT_KEY}
```

Set the `AZURE_ACCOUNT_NAME` and `AZURE_ACCOUNT_KEY` environment variables to the Azure storage account name and account key, respectively.

### Google Cloud Storage

```yaml
params:
  unity_catalog_google_service_account: </path/to/service-account.json>
```

## Example Delta Lake Spicepod

```yaml
version: v1beta1
kind: Spicepod
name: uc_quickstart

catalogs:
  - from: unity_catalog:https://<unity_catalog_host>/api/2.1/unity-catalog/catalogs/<catalog_name>
    name: uc_quickstart
    params:
      # delta_lake S3 parameters
      unity_catalog_aws_region: us-west-2
      unity_catalog_aws_access_key_id: ${secrets:aws_access_key_id}
      unity_catalog_aws_secret_access_key: ${secrets:aws_secret_access_key}
      unity_catalog_aws_endpoint: s3.us-west-2.amazonaws.com
```

## Step 5. Start the Spice runtime and show the available tables

Once the `spicepod.yml` is configured, start the Spice runtime:

```sh
spice run
```

In a seperate terminal, run the Spice SQL REPL:

```sh
spice sql
```

In the REPL, show the available tables.

```sql
SHOW TABLES;
+---------------+--------------+---------------+------------+
| table_catalog | table_schema | table_name    | table_type |
+---------------+--------------+---------------+------------+
| spice         | runtime      | metrics       | BASE TABLE |
| spice         | runtime      | task_history  | BASE TABLE |
| uc_quickstart | default      | taxi_trips    | BASE TABLE |
+---------------+--------------+---------------+------------+
```

## Step 6. Query a dataset

In the SQL REPL execute query. For example:

```sql
-- SELECT * FROM uc_quickstart.<SCHEMA_NAME>.<TABLE_NAME> LIMIT 5;
sql> SELECT fare_amount FROM uc_quickstart.default.taxi_trips LIMIT 5;
+-------------+
| fare_amount |
+-------------+
| 11.4        |
| 13.5        |
| 11.4        |
| 27.5        |
| 18.4        |
+-------------+

Time: 1.4579897499999999 seconds. 5 rows.
```

## More information

- [Unity Catalog Connector Quickstart](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/unity_catalog/README.md)
- [Databricks Unity Catalog Connector Quickstart](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/databricks/README.md)
- Spice OSS [Catalogs Documentation](https://docs.spiceai.org/components/catalogs)
- Spice OSS [Unity Catalog Documentation](https://docs.spiceai.org/components/catalogs/unity-catalog)
- Get help on [Discord](https://discord.gg/kZnTfneP5u).
