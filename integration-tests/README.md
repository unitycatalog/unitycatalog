# Integration tests

## Prerequisites

- Run `build/sbt clean package publishLocal` to publish spark connector to local maven cache
- These tests currently assume you have existing cloud resources set up (e.g. S3 bucket & IAM role for S3)
 
## Set up catalog

### Option 1: Use localhost unity catalog server

First, update integration test [server.properties](./etc/conf/server.properties)
  - For S3: Set `s3.bucketPath.0`, `s3.region.0`, `s3.awsRoleArn.0`, `s3.accessKey.0`, and `s3.secretKey.0`
  - For GCP: Set `gcs.bucketPath.0` and `gcs.jsonKeyFilePath.0`
  - For Azure: Set `adls.storageAccountName.0`, `adls.tenantId.0`, `adls.clientId.0`, and `adls.clientSecret.0`

Next, run the UC server to test against:
```sh
# run from the integration-tests dir to use the testing configurations
cd integration-tests
../bin/start-uc-server
```

In a separate shell, ensure a catalog is created for testing:
```
bin/uc catalog create --name unity
```

### Option 2: Use external catalog

This scenario assumes an existing unity catalog is already running externally.

```sh
export CATALOG_URI=https://<my-uc-instance/
export CATALOG_AUTH_TOKEN=<my-access-token>
export CATALOG_NAME=<my-catalog-name>
```

## Run the tests
By default, tests will run against the local filesystem. To run against cloud storage, set the following *optional* environment variables:

```sh
export S3_BASE_LOCATION=s3://<my-bucket>/<optional>/<path>/
export GS_BASE_LOCATION=gs://<my-bucket>/<optional>/<path>/
export ABFSS_BASE_LOCATION=abfss://<container>@<account_name>.dfs.core.windows.net/<optional>/<path>/
```

Finally, run the tests:
```sh
build/sbt integrationTests/test
```
 