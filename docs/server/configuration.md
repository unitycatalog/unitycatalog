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
- `server.external-local-roots`: A comma-separated list of local directories (plain paths or `file:` URLs) under
    which external Iceberg tables may be created through the Iceberg REST catalog without an external location. The
    server creates these tables' directories and writes their metadata with its own OS identity, so it accepts a
    local location only when the location is under an external location or strictly under one of these roots (the
    root itself is not accepted). Unset by default: a local external Iceberg table then needs an external location.
    No securable governs a root, so any principal who can create a table can use it. Each entry must be a local
    path; an entry such as an `s3://` URL fails server startup. As with `storage-root.*`, a relative path containing
    `/` (for example `./uc-external`) is resolved against the server's working directory, and a directory that does not
    exist yet is accepted.

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

## Logging

The server logs are located at `etc/logs/server.log`. The log level and log rolling policy can be set in log4j2 config
file: `etc/conf/server.log4j2.properties`.

## Authentication and Authorization

Please refer to [Authentication and Authorization](./auth.md) section for more information.
