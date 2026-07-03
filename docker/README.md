# Docker Developer Docs

## Integration tests (`docker/tests`)

End-to-end JUnit suite (UC + Celonis OAuth + MinIO + Spark). Prerequisites:

1. `/etc/hosts`: `127.0.0.1 dev.dev.celonis.cloud`
2. OAuth stack: `docker compose -f docker/oidc/compose.yaml up -d`
3. UC server: `./docker/start-uc-for-tests.sh` (binary mode merges OIDC truststore for JWKS)

Run:

```sh
./docker/tests/run-tests.sh
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
