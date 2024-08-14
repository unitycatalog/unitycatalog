# Unity Catalog Spice OSS Integration

Spice OSS is a unified SQL query interface and portable runtime to locally materialize, accelerate, and query datasets across databases, data warehouses, and data lakes.

Unity Catalog and Databricks Unity Catalog can be used with [Spice OSS](https://github.com/spiceai/spiceai) as [Catalog Connectors](https://docs.spiceai.org/components/catalogs) to make catalog tables available for query in Spice.

Follow the guide below to connect to Databricks Unity Catalog. See the [Unity Catalog quickstart](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/unity_catalog/README.md) to connect to UC directly.

## Prerequisite

- **Spice OSS installed:** To install Spice, see [Spice OSS Installation](https://docs.spiceai.org/installation).
- A Databricks account with a Unity Catalog configured with one or more tables. (see the [Databricks documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) for more information).

## Step 1. Create a Databricks API token

Create a Databricks personal access token by following the [Databricks documentation](https://docs.databricks.com/en/dev-tools/auth/index.html).

## Step 2. Create a new directory and initialize a Spicepod

```bash
mkdir uc_quickstart
cd uc_quickstart
spice init
```

## Step 3. Add the Databricks Unity Catalog Connector

Configure the `spicepod.yaml` with:

```yaml
catalogs:
  - from: databricks:<CATALOG_NAME>
    name: uc_quickstart
    params:
      mode: spark_connect # or delta_lake
      databricks_token: ${env:DATABRICKS_TOKEN}
      databricks_endpoint: <instance-id>.cloud.databricks.com
      databricks_cluster_id: <cluster-id>
```

Set `mode` to `spark_connect` or `delta_lake` appropriately.

The default mode is `spark_connect` and requires an [All-Purpose Compute Cluster](https://docs.databricks.com/en/compute/index.html). The `delta_lake` mode queries Delta Lake tables directly in object storage, and requires Spice to have the necessary object storage access.

Set the `DATABRICKS_TOKEN` environment variable to the Databricks personal access token created in Step 1. An `.env` file created in the same directory as `spicepod.yaml` can be used to set the variable, i.e.:

```bash
echo "DATABRICKS_TOKEN=<token>" > .env
```

See the [Databricks Unity Catalog Connector](https://docs.spiceai.org/components/catalogs/databricks) documentation for more information.

## Step 4. Set the object storage credentials for `delta_lake` mode

If using `delta_lake` mode, object storage credentials must be set for Spice to access the data.

### Using Delta Lake in AWS S3

```yaml
params:
  mode: delta_lake
  databricks_endpoint: <instance-id>.cloud.databricks.com
  databricks_token: ${env:DATABRICKS_TOKEN}
  databricks_aws_access_key_id: ${env:AWS_ACCESS_KEY_ID}
  databricks_aws_secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
  databricks_aws_region: <region> # E.g. us-east-1, us-west-2
  databricks_aws_endpoint: <endpoint> # If using an S3-compatible service, like Minio
```

Set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to the AWS access key and secret key, respectively.

### Using Delta Lake in Azure Storage

```yaml
params:
  mode: delta_lake
  databricks_token: ${env:DATABRICKS_TOKEN}
  databricks_azure_storage_account_name: ${env:AZURE_ACCOUNT_NAME}
  databricks_azure_account_key: ${env:AZURE_ACCOUNT_KEY}
```

Set the `AZURE_ACCOUNT_NAME` and `AZURE_ACCOUNT_KEY` environment variables to the Azure storage account name and account key, respectively.

### Using Delta Lake in Google Cloud Storage

```yaml
params:
  mode: delta_lake
  databricks_token: ${env:DATABRICKS_TOKEN}
  databricks_google_service_account: </path/to/service-account.json>
```

## Example Delta Lake Spicepod

```yaml
version: v1beta1
kind: Spicepod
name: uc_quickstart

catalogs:
  - from: databricks:<CATALOG_NAME>
    name: uc_quickstart
    params:
      mode: delta_lake
      databricks_token: ${env:DATABRICKS_TOKEN}
      databricks_endpoint: <instance-id>.cloud.databricks.com
      databricks_cluster_id: <cluster-id>
      databricks_aws_access_key_id: ${env:AWS_ACCESS_KEY_ID}
      databricks_aws_secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      databricks_aws_region: <region> # E.g. us-east-1, us-west-2
      databricks_aws_endpoint: <endpoint> # If using an S3-compatible service, like Minio
```

## Step 5. Start the Spice runtime and show the available tables

Once the `spicepod.yml` is configured, start the Spice runtime:

```shell
spice run
```

In a seperate terminal, run the Spice SQL REPL:

```shell
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
| uc            | default      | taxi_trips    | BASE TABLE |
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
