# Docker Developer Docs

## Overview
Build the Unity Catalog Docker image for a specific ref by locally checking out
the ref and then running `docker build -t <tag> .` from the main project
directory.

It is recommended to use the forthcoming offical DockerHub image, with the
specific version tag, rather than building the images locally from a ref.

## Local object storage (MinIO)

Start S3-compatible storage for catalog managed tables and models:

```sh
docker compose -f docker/compose.yaml up -d
```

See [minio/README.md](minio/README.md) for `server.properties` settings and
verification steps.

## Local OIDC (auth testing)

To run Unity Catalog with authorization enabled, start a local OpenID Connect
provider:

```sh
docker compose -f docker/oidc/compose.yaml up -d
```

See [oidc/README.md](oidc/README.md) for Keycloak setup, `server.properties`
snippets, and a lighter mock-oauth2 alternative.

### MinIO + OIDC together

```sh
docker compose -f docker/compose.yaml -f docker/oidc/compose.yaml up -d
```

Merge `minio/server.properties.snippet` and `oidc/server.properties.snippet`
into `etc/conf/server.properties`.

## Spark SQL (optional profile)

Spark **4.1.2** with Unity Catalog, Delta Lake, and `hadoop-aws` for MinIO:

```sh
docker/spark/run-test.sh
```

See [spark/README.md](spark/README.md). UC always runs on the host via
`bin/start-uc-server`. Merge `spark/server.properties.snippet` so vended S3
credentials use an endpoint reachable from inside Docker (`host.docker.internal:9000`).

## Further Reading
To extend the Unity Catalog image, refer to the [Docker
documentation](https://docs.docker.com/build/building/base-images/) on building
images using base images.
