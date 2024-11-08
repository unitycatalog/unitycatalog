#!/bin/bash

# ==============================================================================
# Script: build-docs.sh
# Description:
#   This script generates the Python API Docs for the OpenAPI Generated Python
#   Client SDK using pdoc.
#
#   Prerequisites:
#     - Ensure that `pdoc==15.0.0` is installed.
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

# ==========================
# Step 1: Verify pdoc Installation
# ==========================

REQUIRED_PDOC_VERSION="15.0.0"

# Check if pdoc is installed
if ! command -v pdoc &> /dev/null
then
    error "`pdoc` is not installed. Please install it by running 'pip install pdoc==${REQUIRED_PDOC_VERSION}'."
fi

# Retrieve installed pdoc version using pip show
INSTALLED_PDOC_VERSION=$(pip show pdoc | grep '^Version:' | awk '{print $2}')

if [ "$INSTALLED_PDOC_VERSION" != "$REQUIRED_PDOC_VERSION" ]; then
    error "`pdoc` version mismatch. Required: ${REQUIRED_PDOC_VERSION}, Installed: ${INSTALLED_PDOC_VERSION}. Please install the correct version using 'pip install pdoc==${REQUIRED_PDOC_VERSION}'."
fi

# ==========================
# Step 2: Verify Generated SDK Files
# ==========================

SDK_DIR="clients/python/target/src/unitycatalog"

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

# Export PYTHONPATH to include the parent directory of the SDK
export PYTHONPATH="$(pwd)/clients/python/target"

# ==========================
# Step 4: Generate Documentation
# ==========================

OUTPUT_DIR="clients/python/target/docs"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Inform the user that documentation generation is starting
echo "Generating documentation using pdoc..."

# Run pdoc to generate the documentation
# Redirect standard output to /dev/null to suppress verbose output
# Allow standard error to be displayed for warnings and errors
pdoc unitycatalog -o "$OUTPUT_DIR" > /dev/null

# Check if pdoc ran successfully
if [ $? -eq 0 ]; then
    success "Documentation successfully generated at '${OUTPUT_DIR}'."
else
    error "Documentation generation failed. Please check the pdoc output for details."
fi
