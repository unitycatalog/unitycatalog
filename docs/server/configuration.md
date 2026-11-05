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
#                     |___/           v<version>           |___/  #
###################################################################
```

!!! note "Server version string"
    `<version>` is the version you are running. Released builds display the release version (for
    example `v0.7.0`), while builds from the `main` branch append `-SNAPSHOT` (for example
    `v0.8.0-SNAPSHOT`).

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
- `server.external-local-roots`: A comma-separated list of local directories (plain paths or `file:` URLs) under
    which external Iceberg tables may be created through the Iceberg REST catalog without an external location. The
    server creates these tables' directories and writes their metadata itself
    (see [Local file system storage](#local-file-system-storage)), so it accepts a local location only when the
    location is under an external location or strictly under one of these roots (the root itself is not accepted).
    Unset by default: a local external Iceberg table then needs an external location. No securable governs a root, so
    any principal who can create a table can use it. Each entry must be a local path; an entry such as an `s3://` URL
    fails server startup. As with `storage-root.*`, a relative path containing `/` (for example `./uc-external`) is
    resolved against the server's working directory, and a directory that does not exist yet is accepted.
- `server.shutdown-timeout`: How long the server waits for in-flight requests when it stops, for example on SIGTERM.
    Defaults to `PT5S`. Stopping takes longer than this: up to 5 seconds more if a storage cleanup attempt is
    running, plus about 2 seconds to stop the HTTP server. With the default, give `docker stop` 15 seconds or more
    (`--time`, or `--stop-timeout` on `docker run`), and keep Kubernetes' `terminationGracePeriodSeconds` above it
    too; raise both when you raise this setting.

!!! note "Local `file:` locations"
    A local `file:` location must name one plain path. The server rejects with `400` a location that has an encoded
    `/` or NUL (`%2F`, `%00`), an encoded dot segment (`%2e`, `%2e%2e`, in either case), or a `..` above the root
    (`file:///../etc`), since the file system would read it as a different path, and a location with a query or
    fragment, which a local path does not have.
    A host is read as the first directory (`file://tmp/x` is `/tmp/x`) and is checked the same way. Other escapes
    are kept as sent. For an external Iceberg table, a local location must also use only the escapes a path needs
    (`my%20table`, `a%25b`, `caf%C3%A9`); a location with an escape it does not need (`table%41` for `tableA`,
    lowercase hex, raw non-ASCII) is rejected with `400`, and an external location whose URL has such an escape
    cannot hold one.

### Secrets

Any setting can also come from an environment variable named `UC_`, then the key in upper case, with `_` for each
character that is not a letter or digit. For example, `s3.region.0` is `UC_S3_REGION_0`. Set secrets this way, not in
`server.properties` or `hibernate.properties` (from `hibernate.properties`, only the password can come from the
environment):

| Key | Environment variable |
| --- | --- |
| `server.client-secret` | `UC_SERVER_CLIENT_SECRET` |
| `aws.secretKey` | `UC_AWS_SECRETKEY` |
| `s3.secretKey.<n>`, `s3.sessionToken.<n>` | `UC_S3_SECRETKEY_<n>`, `UC_S3_SESSIONTOKEN_<n>` |
| `adls.clientSecret.<n>` | `UC_ADLS_CLIENTSECRET_<n>` |
| `hibernate.connection.password` (in `hibernate.properties`) | `UC_HIBERNATE_CONNECTION_PASSWORD` |

```sh
export UC_S3_SECRETKEY_0='<secret key>'
bin/start-uc-server
```

A variable takes precedence over the same key in `server.properties`, and `UC_HIBERNATE_CONNECTION_PASSWORD` over
`hibernate.properties`. A variable named exactly as the key comes before the `UC_` one. An empty `UC_` variable is
ignored, and line breaks at the end of its value are dropped, as a Kubernetes Secret made from a file often ends with
one. The prefix keeps variables that runtimes set for their own use, such as `AWS_REGION`, from changing settings. The
server logs the name of each environment variable it uses for a setting, never its value. `server.client-secret` is
read by `bin/uc auth login`, not by the server. The Helm chart passes `UC_S3_SECRETKEY_<n>` and
`UC_ADLS_CLIENTSECRET_<n>` to the server container from Kubernetes Secrets. With `server.db.type: postgresql`, it also
passes `UC_HIBERNATE_CONNECTION_PASSWORD` if `server.db.postgresqlConfig.passwordSecretName` is set.

!!! warning "Deprecated: secrets in properties files"
    Secrets in `server.properties` and the password in `hibernate.properties` still work, but this is deprecated. The
    server logs a warning at startup for each one it reads from a file. Move them to environment variables. The server
    never reads `hibernate.connection.password` from `server.properties`, and logs a warning that it is ignored.

#### Upgrading

- A `UC_` variable that is already set in the server's environment now takes precedence over the same key in
  `server.properties`, and `UC_HIBERNATE_CONNECTION_PASSWORD` over `hibernate.properties`.
- `bin/uc auth login` reads `UC_SERVER_CLIENT_SECRET` before `server.client-secret` in `etc/conf/server.properties`.
- Helm: use the server image from the chart's release, which is the default. An older server image doesn't read these
  variables, so S3 and ADLS lose their secrets, and the server can't log in to a PostgreSQL database that needs a
  password (`server.db.postgresqlConfig.passwordSecretName` set).
- Helm: the rendered `server.properties` no longer holds `server.client-secret`, `s3.secretKey.<n>` or
  `adls.clientSecret.<n>`. The init container has `OAUTH_CLIENT_SECRET`, `S3_SECRET_KEY_<n>` and
  `ADLS_CLIENT_SECRET_<n>` only when `server.config.override` or `extraProperties` is set, so a template that uses
  them keeps working; that is deprecated. A container added through `extraInitContainers` or `extraContainers` that
  needs these secrets must read them from the Kubernetes Secret itself.
- Helm: the rendered `hibernate.properties` no longer holds the password, and the init container no longer has
  `DB_PASSWORD`. A container added through `extraInitContainers` or `extraContainers` that needs the password must
  read it from the Secret named by `server.db.postgresqlConfig.passwordSecretName`. In a `server.db.overrideConfig`
  template, remove the line `hibernate.connection.password=${DB_PASSWORD}`.

## Local file system storage

Unity Catalog can store data on the server's local file system, which is convenient for development and single-machine
setups. The server reads and writes local storage only where an administrator configures it:

- a server storage root with a local path, such as `storage-root.tables` or `storage-root.models`;
- an external location with a `file:` URL, and the catalogs, schemas, tables, and volumes under it;
- the roots in `server.external-local-roots`, for external Iceberg tables.

For local locations, the server reads, writes, and deletes some files itself, with its own operating-system identity:
for example, the metadata of tables created through the Iceberg REST catalog, and the directories of managed tables,
volumes, and models. Clients that share the file system read and write data files with their own identity. So the
server acts on behalf of every principal that can write those directories:

- Make local storage directories writable only by principals you trust with the server's file access.
- The server does not follow a symbolic link at or below a table's location, but it does not check the directories
    above the location. A principal that can replace one of those with a link can redirect the server's reads,
    writes, and deletes elsewhere.
- The check runs when an operation starts, so a link created while an operation runs is not detected.

For a deployment shared by users who should not have each other's file access, use cloud storage, where the server
vends credentials scoped to each location.

### Storage credential cache

When the server vends temporary cloud storage credentials, it can reuse a recently vended credential
for the same location, privileges, and credential binding instead of calling the cloud provider again
on every request. The cache is disabled by default. The database binding for a location is always
re-read, so changing its role or external ID changes the cache lookup on the next request. For
per-bucket configuration (any cloud), the cache lookup uses the location and privileges.

For AWS storage credentials, UC includes the stored external ID in both the cache key and the
`AssumeRole` request. Updating the IAM role on a UC storage credential generates a new external ID,
even when the role ARN is unchanged. The next request uses the new binding and cannot reuse
credentials cached under the previous external ID. Ensure the AWS role's trust policy allows the
newly returned external ID before requesting new credentials. Temporary credentials already vended
under the previous external ID stay valid until they expire. See [AWS credential setup](aws.md) for
configuring the external ID in the role's trust policy.

The cache is controlled by these keys:

| Property | Default | Description |
| --- | --- | --- |
| `server.storage-credential-cache.enabled` | `false` | Whether to reuse vended credentials. When `false`, every request vends a fresh credential from the cloud provider. |
| `server.storage-credential-cache.max-size` | `1000` | The maximum number of distinct credentials to keep. |
| `server.storage-credential-cache.max-age` | `PT5M` | The longest a vended credential is reused. It is counted from when the credential was vended, and serving the credential from the cache does not extend it. A cached credential is handed out with its remaining lifetime, so clients receive at least the credential's lifetime minus the max age (55 minutes for a 1-hour credential with the default); lower it if clients need more. A credential is never reused within one minute of its expiry, and one that lives one minute or less is not cached. Keep it short so that a cloud-side change, such as a trust-policy update, takes effect for new requests within this time. |

Durations use the ISO-8601 format (for example `PT1M` is one minute, `PT5M` is five minutes).

## Logging

The server logs are located at `etc/logs/server.log`. The log level and log rolling policy can be set in log4j2 config
file: `etc/conf/server.log4j2.properties`.

With the default log4j2 config, the log rolls over every hour, and within an hour each time it reaches 10MB. Each
rolled-over file is compressed into `etc/logs/server-<yyyy-MM-dd-HH>-<n>.log.gz`, where `<n>` numbers the archives of
that hour, starting at 1, up to 20. At each rollover, archives last written more than 7 days ago are deleted, and so are
the oldest archives once all archives together exceed 1GB.

With the default log4j2 config, each log line names the host that wrote it. A line written while serving a request
also carries an ID the server generates for that request, as `request_id=<id>`, so all the lines one request writes
can be found together. To add them to a log format of your own, use `${hostName}` and `%X{request_id}`.

## Authentication and Authorization

Please refer to [Authentication and Authorization](./auth.md) section for more information.
