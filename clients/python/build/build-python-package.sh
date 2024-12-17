#!/bin/bash

# Generates the packaged source for the Unity Catalog Python Client SDK

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT_DIR="$(cd "$SCRIPT_DIR/../../../" && pwd)"
cd "$REPO_ROOT_DIR"

TARGET_DIR="$REPO_ROOT_DIR/clients/python/target"
DIST_DIR="$TARGET_DIR/dist"

if ! python -m build --version &>/dev/null; then
    echo "Python build module not found. Installing..."
    python -m pip install --upgrade build
fi

if ! python -m twine --version &>/dev/null; then
    echo "Twine module not found. Installing..."
    python -m pip install --upgrade twine
fi

if [ -d "$TARGET_DIR" ]; then
    cd "$TARGET_DIR"
else
    echo "Target directory does not exist: $TARGET_DIR"
    exit 1
fi

if [ -d "$DIST_DIR" ]; then
    echo "Cleaning up previous build files..."
    rm -rf "$DIST_DIR"
fi

mkdir -p "$DIST_DIR"

echo "Building the package..."
python -m build --outdir "$DIST_DIR"

echo "Build completed. The following files are ready for deployment:"
ls "$DIST_DIR"

echo "Running twine check on the package..."
python -m twine check "$DIST_DIR"/*

echo "Packaging complete. You can now deploy the package to PyPI with twine upload $DIST_DIR/*"
