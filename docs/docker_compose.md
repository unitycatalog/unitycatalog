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

## Configurations

Docker Compose is configured in the `./compose.yaml` file.

The configuration will create a bind mount to the local files in `./etc/conf`.
The UC server can be configured by editing the configuration files on the host,
and the mount will reflect the changes in the container.

Set secrets as environment variables of the `server` service, not in the mounted
`server.properties`, where they are deprecated (see
[Secrets](./server/configuration.md#secrets)). For example, in `compose.yaml`:

```yaml
services:
  server:
    environment:
      SERVER_CLIENT_SECRET: ${SERVER_CLIENT_SECRET}
      HIBERNATE_CONNECTION_PASSWORD: ${HIBERNATE_CONNECTION_PASSWORD}
```

Compose fills in each `${...}` from your shell or a `.env` file, so the secrets stay
out of `compose.yaml`. The database password works the same way; keeping it in the
mounted `hibernate.properties` is deprecated.

The server also keeps the key it signs its tokens with in the mounted `./etc/conf`,
generating it on first start. That is deprecated: create a key pair, mount the two
files into the container, and set `server.token-signing-private-key-file` and
`server.token-signing-public-key-file` to them (see
[Configuration](./server/configuration.md#configuration)).

The configuration will also create a persistent, named volume to store the
server's data. This will persist between restarts of the container. See the
[deployment](./server/deployment.md) page for more details on how to configure other
databases like Postgres.
