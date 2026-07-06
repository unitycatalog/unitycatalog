# Docker Developer Docs

## Integration tests (`docker/tests`)

End-to-end JUnit suite (UC + Celonis OAuth + MinIO + Spark). Prerequisites:

1. OAuth stack: `docker compose -f docker/oidc/compose.yaml up -d`
2. Team exists in team DB for `UC_OAUTH_TEAM` (default `dev`)
3. UC server in docker mode: `UC_SERVER_MODE=docker UC_DOCKER_IMAGE=... ./docker/start-uc-for-tests.sh`

OAuth tenant env vars (defaults shown):

```sh
UC_OAUTH_TEAM=dev
UC_OAUTH_REALM=dev.celonis.cloud
# derived: host=dev.dev.celonis.cloud, issuer=https://dev.dev.celonis.cloud
```

No `/etc/hosts` entry is required for docker-mode tests; the test JVM maps `UC_OAUTH_HOST`
to loopback via `jdk.net.hosts.file` and the UC container uses `--add-host`.

Run:

```sh
UC_SERVER_MODE=docker UC_DOCKER_IMAGE=... ./docker/tests/run-tests.sh
```

See [oidc/README.md](oidc/README.md) and [tests/run-tests.sh](tests/run-tests.sh) for options.

## Overview
Build the Unity Catalog Docker image for a specific ref by locally checking out
the ref and then running `docker build -t <tag> .` from the main project
directory.

It is recommended to use the forthcoming offical DockerHub image, with the
specific version tag, rather than building the images locally from a ref.

## Further Reading
To extend the Unity Catalog image, refer to the [Docker
documentation](https://docs.docker.com/build/building/base-images/) on building
images using base images.
