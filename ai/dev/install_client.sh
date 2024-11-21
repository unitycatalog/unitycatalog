#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "${SCRIPT_DIR}/../.." || exit 1

build/sbt pythonClient/generate

CLIENT_TARGET_DIR="./clients/python/target"
if [ ! -d "$CLIENT_TARGET_DIR" ]; then
    log "Error: Client target directory '$CLIENT_TARGET_DIR' does not exist."
    exit 1
fi

# Install the current branch's unitycatalog-client package
pip install "$CLIENT_TARGET_DIR/.[dev]"

# Install unitycatalog-ai core dev package from the current branch
pip install "./ai/core/.[dev]"
