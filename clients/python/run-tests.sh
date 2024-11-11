#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to the root directory of the project
cd "${SCRIPT_DIR}/../.." || exit 1

# Generate the client library
build/sbt pythonClient/generate

# Install the generated library
pip install "${SCRIPT_DIR}/target/"

# Run the tests
pytest "${SCRIPT_DIR}/tests"