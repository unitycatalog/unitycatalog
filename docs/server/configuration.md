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
    example `v0.5.0`), while builds from the `main` branch append `-SNAPSHOT` (for example
    `v0.5.0-SNAPSHOT`).

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

### Storage credential cache

When the server vends temporary cloud storage credentials, it can reuse a recently vended credential
for the same location, privileges, and role instead of calling the cloud provider again on every
request. The database binding for a location is always re-read, so a rebind (for example pointing a
location at a different role) takes effect immediately. The cache is controlled by these keys:

| Property | Default | Description |
| --- | --- | --- |
| `server.storage-credential-cache.enabled` | `true` | Whether to reuse vended credentials. When `false`, every request vends a fresh credential from the cloud provider. |
| `server.storage-credential-cache.max-size` | `1000` | The maximum number of distinct credentials to keep. |
| `server.storage-credential-cache.renewal-lead-time` | `PT1M` | How far before a credential's own expiry it is refreshed, so callers never receive a credential on the verge of expiring. |
| `server.storage-credential-cache.max-age` | `PT5M` | The longest a vended credential is reused before it is refreshed, regardless of its own expiry. This is deliberately short: Unity Catalog cannot observe a cloud-side trust-policy change, so it bounds how long a credential keeps being served after such a change. |

Durations use the ISO-8601 format (for example `PT1M` is one minute, `PT5M` is five minutes).

## Logging

The server logs are located at `etc/logs/server.log`. The log level and log rolling policy can be set in log4j2 config
file: `etc/conf/server.log4j2.properties`.

## Authentication and Authorization

Please refer to [Authentication and Authorization](./auth.md) section for more information.
