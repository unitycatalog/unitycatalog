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

### AWS S3 Configuration

For enabling server to vend AWS temporary credentials to access S3 buckets (for accessing External tables/volumes),
the following parameters can be configured in the `server.properties` file:

- `s3.bucketPath.<i>`: (required) The S3 path of the bucket where the data is stored. Should be in the format `s3://<bucket-name>`.
- `s3.provider.<i>`: (optional if in-place auth configuration used) The credentials provider reference for vending. See below for more details for provider configuration.
- `s3.accessKey.<i>`: (optional if provider auth configuration used) The AWS access key, an identifier of temp credentials. See below for more details on in-place configuration.
- `s3.secretKey.<i>`: (optional if provider auth configuration used) The AWS secret key used to sign API requests to AWS. See below for more details on in-place configuration.
- `s3.sessionToken.<i>`: (optional if provider auth configuration used) The AWS session token, used to verify that the request is coming from a trusted source. See below for more details on in-place configuration.
- `s3.region.<i>`: (optional if provider auth configuration used) The AWS region for STS calls. See below for more details on in-place configuration.
- `s3.roleArn.<i>`: (optional if provider auth configuration used) The AWS role to be assumed by STS to vend credential. See below for more details on in-place configuration.
- `s3.endpoint.<i>`: (optional) Use non-standard endpoint for S3. This is useful for testing with localstack or other S3 compatible services.

You can configure multiple buckets by incrementing the index *i* in the above parameters. The starting index should
be 0.

Any params that are not required can be left empty.

#### AWS S3 Provider Configuration

The following parameters can be configured in the `server.properties` file for the provider configuration:
- `aws.credentials.provider.<provider_name>.class`: (required) The type of provider to be used. Currently, only `io.unitycatalog.server.service.credential.aws.provider.StsCredentialsProvider` and `io.unitycatalog.server.service.credential.aws.provider.StaticCredentialsProvider` are supported.
- `aws.credentials.provider.<provider_name>.accessKey`: (required) The AWS access key. Will be delivered as access key by `StaticCredentialsProvider` or used to authenticate call to STS by `StsCredentialsProvider`.
- `aws.credentials.provider.<provider_name>.secretKey`: (required) The AWS secret key. Will be delivered as secret key by `StaticCredentialsProvider` or used to authenticate call to STS by `StsCredentialsProvider`.

`StsCredentialsProvider` accepts following additional parameters:
- `aws.credentials.provider.<provider_name>.region`: (required) The AWS region for STS calls.
- `aws.credentials.provider.<provider_name>.roleArn`: (required) The AWS role to be assumed by STS to vend credential.

`StaticCredentialsProvider` accepts following additional parameters:
- `aws.credentials.provider.<provider_name>.sessionToken`: (optional) The AWS session token, used to verify that the request is coming from a trusted source.

You can configure multiple providers and reference them in storage configuration by supplying `s3.provider.<i>=<provider_name>`. In this case in-place authentication configuration is optional.

#### AWS S3 In-Place Configuration

If you want to use in-place configuration, you need to provide `aws.credentials.provider.<provider_name>.accessKey` and `aws.credentials.provider.<provider_name>.secretKey` plus additional provider-specific configs: `StsCredentialsProvider` requires `s3.region.<i>` and `s3.roleArn.<i>`, `StaticCredentialsProvider` requires `s3.sessionToken.<i>`. Use of in-place configuration takes precedence over provider configuration.

#### Example Configuration

```properties

# Using StsCredentialsProvider
s3.bucketPath.0=s3://my-bucket
s3.provider.0=my-provider
aws.credentials.provider.my-provider.class=io.unitycatalog.server.service.credential.aws.provider.StsCredentialsProvider
aws.credentials.provider.my-provider.accessKey=access-key
aws.credentials.provider.my-provider.secretKey=secret-key
aws.credentials.provider.my-provider.region=us-west-2

# Using StaticCredentialsProvider
s3.bucketPath.1=s3://my-other-bucket
s3.provider.1=my-other-provider
s3.endpoint.1=http://minio:9000
aws.credentials.provider.my-other-provider.class=io.unitycatalog.server.service.credential.aws.provider.StaticCredentialsProvider
aws.credentials.provider.my-other-provider.accessKey=access-key
aws.credentials.provider.my-other-provider.secretKey=secret-key

# Using in-place configuration for StsCredentialsProvider
s3.bucketPath.2=s3://my-third-bucket
s3.accessKey.2=access-key
s3.secretKey.2=secret-key
s3.region.2=us-west-2
s3.roleArn.2=arn:aws:iam::123456789012:role/role-name

# Using in-place configuration for StaticCredentialsProvider
s3.bucketPath.3=s3://my-fourth-bucket
s3.accessKey.3=access-key
s3.secretKey.3=secret-key
s3.sessionToken.3=session-token
```

#### Additional providers
It is possible to provide custom provider implementations by implementing `io.unitycatalog.server.service.credential.aws.provider.AwsCredentialsProvider` interface and providing the fully qualified class name in the `aws.credentials.provider.<provider_name>.class` property (implementation must be in the classpath of the server instance).

## Logging

The server logs are located at `etc/logs/server.log`. The log level and log rolling policy can be set in log4j2 config
file: `etc/conf/server.log4j2.properties`.

## Authentication and Authorization

Please refer to [Authentication and Authorization](./auth.md) section for more information.
