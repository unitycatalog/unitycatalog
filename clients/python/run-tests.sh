#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to the root directory of the project
cd "${SCRIPT_DIR}/../.." || exit 1

# Generate the client library
build/sbt pythonClient/generate

# Sync all pinned test dependencies from the lock file
cd "${SCRIPT_DIR}"
uv sync --frozen

# Install the generated library into the uv-managed venv (skip dep resolution since
# all transitive deps are already pinned in uv.lock)
uv pip install --no-deps "${SCRIPT_DIR}/target/"

# Run the tests from the repo root so that bin/start-uc-server is resolved correctly
cd "${SCRIPT_DIR}/../.."
uv run --project "${SCRIPT_DIR}" pytest "${SCRIPT_DIR}/tests"
