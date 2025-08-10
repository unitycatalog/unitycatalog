# Unity Catalog Server Configuration

This is the  Unity Catalog server implementation that can be run in the cloud or started locally.

## Running the Server

To run against the latest main branch, start by cloning the open source Unity Catalog GitHub repository:

```sh
git clone git@github.com:unitycatalog/unitycatalog.git
```

To run Unity Catalog, you need **Java 17** installed on your machine. You can always run the `java --version` command
to verify that you have the right version of Java installed such as the following example output.

```sh
% java --version
openjdk 17.0.12 2024-07-16
OpenJDK Runtime Environment Homebrew (build 17.0.12+0)
OpenJDK 64-Bit Server VM Homebrew (build 17.0.12+0, mixed mode, sharing)
```

Change into the `unitycatalog` directory and run `bin/start-uc-server` to instantiate the server. Here is what you
should see:

```console
################################################################### 
#  _    _       _ _            _____      _        _              #
# | |  | |     (_) |          / ____|    | |      | |             #
# | |  | |_ __  _| |_ _   _  | |     __ _| |_ __ _| | ___   __ _  #
# | |  | | '_ \| | __| | | | | |    / _` | __/ _` | |/ _ \ / _` | #
# | |__| | | | | | |_| |_| | | |___| (_| | || (_| | | (_) | (_| | #
#  \____/|_| |_|_|\__|\__, |  \_____\__,_|\__\__,_|_|\___/ \__, | #
#                      __/ |                                __/ | #
#                     |___/               v0.2.0           |___/  #
###################################################################
```

The server can be started by issuing the below command from the project root directory:

```sh
bin/start-uc-server
```

!!! note "Running Unity Catalog Server on a specific port"
    To run the server on a specific port, use the `-p` or `--port` option followed by the port number:

    ```sh title="Use -p or --port to specify your port"
    bin/start-uc-server -p <port_number>
    bin/start-uc-server -port <port_number>
    ```

    If no port is specified, the server defaults to port **8080**.

## Configuration

The server config file is at the location `etc/conf/server.properties` (relative to the project root).

- `server.env`: The environment in which the server is running. This can be set to `dev` or `test`. When set to `test`
    the server will instantiate an empty in-memory h2 database for storing metadata. If set to `dev`, the server will
    use the file `etc/db/h2db.mv.db` as the metadata store. Any changes made to the metadata will be persisted in this
    file.

For enabling server to vend AWS temporary credentials to access S3 buckets (for accessing External tables/volumes),
the following parameters need to be set:

### Basic S3 Configuration

- `s3.bucketPath.i`: The S3 path of the bucket where the data is stored. Should be in the format `s3://<bucket-name>`.
- `s3.region.i`: The AWS region where the bucket is located.
- `s3.awsRoleArn.i`: The AWS IAM role ARN to assume for generating temporary credentials.
- `s3.accessKey.i`: (Optional) The AWS access key. If not provided, uses DefaultCredentialsProviderChain.
- `s3.secretKey.i`: (Optional) The AWS secret key used to sign API requests to AWS.
- `s3.sessionToken.i`: (Optional) The AWS session token. If provided, these credentials are used directly without downscoping.

### Advanced S3 Configuration (for S3-compatible services like MinIO)

- `s3.endpoint.i`: (Optional) Custom S3 endpoint URL for S3-compatible services (e.g., `http://minio:9000`).
- `s3.stsEndpoint.i`: (Optional) Custom STS endpoint URL for services with STS support (e.g., `http://minio:9000`).
- `s3.pathStyleAccess.i`: (Optional) Enable path-style access (`true`/`false`). Required for MinIO and some S3-compatible services.
- `s3.sslEnabled.i`: (Optional) Enable/disable SSL for S3 connections (`true`/`false`).

You can configure multiple buckets by incrementing the index *i* in the above parameters. The starting index should
be 0.

### Example Configurations

#### AWS S3 Configuration
```properties
s3.bucketPath.0=s3://my-bucket
s3.region.0=us-west-2
s3.awsRoleArn.0=arn:aws:iam::123456789012:role/MyRole
s3.accessKey.0=AKIAIOSFODNN7EXAMPLE
s3.secretKey.0=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

#### MinIO Configuration
```properties
s3.bucketPath.1=s3://minio-bucket
s3.region.1=us-east-1
s3.awsRoleArn.1=arn:minio:iam::account:role/your-role
s3.accessKey.1=minioadmin
s3.secretKey.1=minioadmin
s3.endpoint.1=http://localhost:9000
s3.stsEndpoint.1=http://localhost:9000
s3.pathStyleAccess.1=true
s3.sslEnabled.1=false
```

For vending temporary credentials, the server matches the bucket path in the table/volume storage_location with the 
bucket path in the configuration and returns the corresponding credentials based on the configuration.

Any params that are not required can be left empty.

## Logging

The server logs are located at `etc/logs/server.log`. The log level and log rolling policy can be set in log4j2 config
file: `etc/conf/server.log4j2.properties`.

## Authentication and Authorization

Please refer to [Authentication and Authorization](./auth.md) section for more information.
