#!/bin/bash

#Script to help create server and cli jar files via SBT. Then to build and push as Docker image.

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
APP_VERSION=$(awk -F ' := ' '{print $2}' version.sbt | tr -d '"')
PLATFORMS="linux/amd64,linux/arm64"

run_sbt() {
  SBT_COMMAND="$ROOT_DIR/build/sbt -J-Xms4G -J-Xmx4G -info clean assembly"
  echo "Running SBT to generate Server and CLI JAR: $SBT_COMMAND"
  $SBT_COMMAND || exit
}

SERVER_TARGET_DIR="server/target"
SERVER_JAR=$(find "$SERVER_TARGET_DIR" -name "unitycatalog-server*.jar" | head -n 1)
if [ -z "$SERVER_JAR" ]; then
    echo "Server JAR not found starting with 'unitycatalog-server*' in the target directory '$SERVER_TARGET_DIR'."
    run_sbt
fi

CLI_TARGET_DIR="examples/cli/target"
CLI_JAR=$(find "$CLI_TARGET_DIR" -name "unitycatalog-cli-*.jar" | head -n 1)
if [ -z "$CLI_JAR" ]; then
    echo "CLI JAR not found starting with 'unitycatalog-cli-*' in the target directory '$CLI_TARGET_DIR'."
    run_sbt
fi

echo "Setting up docker for multi-platform builds using buildx"
docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name builder
docker buildx inspect --bootstrap builder

echo "Running Docker build command, version=$APP_VERSION, platforms=$PLATFORMS"
(cd "$ROOT_DIR/.."; docker buildx build \
  --platform "$PLATFORMS" \
  -t datacatering/unitycatalog:"$APP_VERSION" --push .)
