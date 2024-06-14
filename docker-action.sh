#!/bin/bash

APP_VERSION=$(awk -F ' := ' '{print $2}' version.sbt | tr -d '"')
PLATFORMS="linux/amd64,linux/arm64"

run_sbt() {
  SBT_COMMAND="./build/sbt -info clean assembly"
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

docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name builder
docker buildx inspect --bootstrap builder

docker buildx build \
  --platform "$PLATFORMS" \
  -t datacatering/unitycatalog:"$APP_VERSION" --push .
