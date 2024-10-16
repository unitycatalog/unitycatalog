# Docker Developer Docs

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
