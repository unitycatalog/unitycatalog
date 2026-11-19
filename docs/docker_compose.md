# Docker Compose

To start Unity Catalog in Docker Compose in one command, install the latest
version of [Docker Desktop](https://www.docker.com/products/docker-desktop/) and
run the following from the project's root directory:

```sh
docker compose up -d
```

This starts the Unity Catalog server and UI. You can access the UI at
`http://localhost:3000` and the server at `http://localhost:8080`. Clients like
DuckDB or Spark running on the host machine will be able to interact on those
ports with the containers running Unity Catalog.

To use the Unity Catalog CLI, attach to a shell in the Unity Catalog server
container:

```sh
docker exec -it unitycatalog-server-1 /bin/bash
```

Use the Unity Catalog CLI from the attached shell to interact with the server:

```sh
bin/uc table list --catalog unity --schema default
```

To remove the containers and persistent volumes, `exit` the attached shell and
run the following from the host machine:

```sh
docker compose down --volumes --remove-orphans
```

Refer to the main [Quickstart](quickstart.md) for more examples of how to
interact with the catalog.

## Server image contents

The server image contains the server, the `bin/uc` CLI, their runtime dependencies,
configuration, and sample data. It is not a development image: it does not contain
complete source trees, API specifications, sbt and the build scripts, or the full
dependency cache. Some paths under `clients/` and `target/` are present because the
generated classpath files reference them; do not rely on other build artifacts
being present.

Java 17 is a linked runtime with all JDK modules and their tools, including `javac`
and `keytool`, but without `jmods`, development headers, or JDK sources. To build
applications or create another runtime with `jlink`, use a full JDK in a separate
build stage rather than building on this image.

The server runs as uid 100 and gid 101. Its home is `/home/unitycatalog`. The build
artifacts are read-only by default, and the server has write access under `etc/`
for configuration, logs, the database, and sample data. The container starts the
server with `bin/start-uc-server`, and the server reads its configuration from
`etc/conf/`.

## Configurations

Docker Compose is configured in the `./compose.yaml` file.

The configuration will create a bind mount to the local files in `./etc/conf`.
The UC server can be configured by editing the configuration files on the host,
and the mount will reflect the changes in the container.

Set secrets as environment variables of the `server` service, not in the mounted
`server.properties` or `hibernate.properties`, where they are deprecated (see
[Secrets](./server/configuration.md#secrets)). For example, in `compose.yaml`:

```yaml
services:
  server:
    environment:
      UC_S3_SECRETKEY_0: ${UC_S3_SECRETKEY_0}
      UC_HIBERNATE_CONNECTION_PASSWORD: ${UC_HIBERNATE_CONNECTION_PASSWORD}
```

Compose fills in each `${...}` from your shell or a `.env` file, so the secrets stay
out of `compose.yaml`.

The configuration will also create a persistent, named volume to store the
server's data. This will persist between restarts of the container. See the
[deployment](./server/deployment.md) page for more details on how to configure other
databases like Postgres.
