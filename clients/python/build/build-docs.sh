#!/bin/bash

# ==============================================================================
# Script: build-docs.sh
# Description:
#   This script generates the Python API Docs for the OpenAPI Generated Python
#   Client SDK using pdoc.
#
#   Prerequisites:
#     - Generate the Python Client SDK package by running
#       `build/sbt pythonClient/generate` from the repository root.
#
# Usage:
#   ./build-docs.sh
#
# ==============================================================================

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display error messages and exit
error() {
    echo "Error: $1" >&2
    exit 1
}

# Function to display success messages
success() {
    echo "Success: $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT_DIR="$(cd "$SCRIPT_DIR/../../../" && pwd)"

# ==========================
# Step 1: Sync pinned doc tools from the lock file
# ==========================

cd "$REPO_ROOT_DIR/clients/python"
uv sync --group docs --frozen

# ==========================
# Step 2: Verify Generated SDK Files
# ==========================

SDK_DIR="$REPO_ROOT_DIR/clients/python/target/src/unitycatalog"

if [ ! -d "$SDK_DIR" ]; then
    error "SDK directory '${SDK_DIR}' does not exist. Please generate the Python Client SDK by running 'build/sbt pythonClient/generate' from the repository root."
fi

# Check if the directory is not empty
if [ -z "$(ls -A "$SDK_DIR")" ]; then
    error "SDK directory '${SDK_DIR}' is empty. Please ensure that the SDK generation step completed successfully."
fi

# Check for __init__.py to confirm it's a Python module
if [ ! -f "$SDK_DIR/client/__init__.py" ]; then
    error "SDK directory '${SDK_DIR}' is missing '__init__.py'. Ensure it is a valid Python package."
fi

success "SDK directory '${SDK_DIR}' exists and contains files."

# ==========================
# Step 3: Set PYTHONPATH
# ==========================

export PYTHONPATH="$REPO_ROOT_DIR/clients/python/target"

# ==========================
# Step 4: Generate Documentation
# ==========================

OUTPUT_DIR="$REPO_ROOT_DIR/clients/python/target/docs"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

echo "Generating documentation using pdoc..."

cd "$REPO_ROOT_DIR"
"$REPO_ROOT_DIR/clients/python/.venv/bin/pdoc" unitycatalog -o "$OUTPUT_DIR" > /dev/null

if [ $? -eq 0 ]; then
    success "Documentation successfully generated at '${OUTPUT_DIR}'."
else
    error "Documentation generation failed. Please check the pdoc output for details."
fi
