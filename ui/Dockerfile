# syntax=docker.io/docker/dockerfile:1.7-labs
FROM node:18

ARG PROXY_HOST=server
WORKDIR /ui

COPY package.json yarn.lock ./

# TODO: configure this directly from the ui app
# Work around to set the proxy host; required for running locally with docker-compose
RUN <<EOF
set -ex
apt-get update
apt-get install -y jq
jq --arg proxy "http://$PROXY_HOST:8080" '.proxy = $proxy' package.json > temp.json
mv temp.json package.json
# '--network-timeout 1000000' is workaround for yarn install to work in github actions
# https://github.com/yarnpkg/yarn/issues/9004
yarn install --network-timeout 1000000
apt-get clean
EOF

COPY --exclude=package.json --exclude=yarn.lock . ./

CMD ["yarn", "start"]
