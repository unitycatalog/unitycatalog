# syntax=docker.io/docker/dockerfile:1.7-labs@sha256:b99fecfe00268a8b556fad7d9c37ee25d716ae08a5d7320e6d51c4dd83246894
FROM node:18@sha256:c6ae79e38498325db67193d391e6ec1d224d96c693a8a4d943498556716d3783

ARG PROXY_HOST=server

# Optional corporate npm mirror, e.g. --build-arg NPM_REGISTRY=https://npm-proxy.example.com/
ARG NPM_REGISTRY=

WORKDIR /ui

COPY package.json yarn.lock ./

# TODO: configure this directly from the ui app
# Work around to set the proxy host; required for running locally with docker-compose
RUN <<EOF
set -ex
apt-get update
apt-get install -y jq
if [ -n "$NPM_REGISTRY" ]; then
  printf 'registry=%s\n' "$NPM_REGISTRY" > .npmrc
  # Yarn Classic fetches from lockfile "resolved" URLs and ignores .npmrc for those.
  # Corporate DNS often sinkholes registry.yarnpkg.com to 127.0.0.1, so rewrite
  # public registry hosts in the lockfile to the mirror for this build only.
  npm_registry_origin=$(printf '%s' "$NPM_REGISTRY" | sed -E 's|/*$||')
  sed -i \
    -e "s#https://registry.yarnpkg.com#${npm_registry_origin}#g" \
    -e "s#https://registry.npmjs.com#${npm_registry_origin}#g" \
    -e "s#https://registry.npmjs.org#${npm_registry_origin}#g" \
    yarn.lock
fi
jq --arg proxy "http://$PROXY_HOST:8080" '.proxy = $proxy' package.json > temp.json
mv temp.json package.json
# '--network-timeout 1000000' is workaround for yarn install to work in github actions
# https://github.com/yarnpkg/yarn/issues/9004
yarn install --network-timeout 1000000
# The mirror is a build-time concern only; keep it out of the published image.
rm -f .npmrc
apt-get clean
EOF

COPY --exclude=package.json --exclude=yarn.lock . ./

CMD ["yarn", "start"]
