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

Refer the the main [Quickstart](quickstart.md) for more examples of how to
interact with the catalog.

## Configurations

Docker Compose is configured in the `./compose.yaml` file.

The configuration will create a bind mount to the local files in `./etc/conf`.
The UC server can be configured by editing the configuration files on the host,
and the mount will reflect the changes in the container.

The congfiguration will also create a persistent, named volume to store the
server's data. This will persist between restarts of the container. See the
[deployment](./deployment.md) page for more details on how to configure other
databases like Postgres.
