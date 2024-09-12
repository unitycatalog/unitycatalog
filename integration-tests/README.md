# Integration tests

## Prerequisites

- Run `build/sbt clean package publishLocal` to publish spark connector to local maven cache
- These tests currently assume you have existing cloud set up:
  - S3S requires a testing S3 bucket and IAM role
      - IAM Role needs trust policy so that the identity running the UC server can assume
      - IAM role needs policy to read/write/list from s3 bucket (or at least a prefix)
      - Copy testing data to your S3 bucket [test numbers table](../etc/data/external/unity/default/tables/numbers/)
  - GCS requires
    - cloud storage bucket
    - service account which has access to that bucket
    - API key generated for that service account


## Set up catalog

### Option 1: Use Run localhost unity catalog server

First, update integration test [server.properties](./etc/conf/server.properties)
  - For S3: Set `s3.bucketPath.0`, `s3.region.0`, `s3.awsRoleArn.0`, `s3.accessKey.0`, and `s3.secretKey.0`
  - For GCP: Set `gcs.bucketPath.0` and `gcs.jsonKeyFilePath.0`

Next, run the UC server to test against:
```shell
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

```shell
export CATALOG_URI=https://<my-uc-instance/
export CATALOG_AUTH_TOKEN=<my-access-token>
export CATALOG_NAME=<my-catalog-name>
```

## Run the tests

Access to these locations should be configured in the UC server:
```shell
export S3_BASE_LOCATION=s3://<my-bucket>/<optional>/<path>/
export GS_BASE_LOCATION=gs://<my-bucket>/<optional>/<path>/
```

Finally, run the tests:
```shell
build/sbt integrationTests/test
```
 