# syntax=docker.io/docker/dockerfile:1.7-labs@sha256:b99fecfe00268a8b556fad7d9c37ee25d716ae08a5d7320e6d51c4dd83246894
FROM oven/bun:1.3.14-slim@sha256:d56a2534ffd262e92c12fd3249d3924d296d97086da773f821d7d0477435ea04 AS bun
FROM node:18@sha256:c6ae79e38498325db67193d391e6ec1d224d96c693a8a4d943498556716d3783

ARG PROXY_HOST=server
WORKDIR /ui

# Bun is the package manager and script runner; react-scripts itself still runs on Node.
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun

COPY package.json bun.lock ./

RUN <<EOF
set -ex
apt-get update
apt-get install -y jq
bun install --frozen-lockfile
# TODO: configure this directly from the ui app
# Work around to set the proxy host; required for running locally with docker-compose
jq --arg proxy "http://$PROXY_HOST:8080" '.proxy = $proxy' package.json > temp.json
mv temp.json package.json
apt-get clean
EOF

COPY --exclude=package.json --exclude=bun.lock . ./

CMD ["bun", "run", "start"]
