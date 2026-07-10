#!/bin/bash

# Generates the packaged source for the Unity Catalog Python Client SDK

set -e

# clients/python/build/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# clients/python/build -> clients/python -> clients -> <repo-root>
REPO_ROOT_DIR="$(cd "$SCRIPT_DIR" && cd ../../.. && pwd)"
cd "$REPO_ROOT_DIR"

TARGET_DIR="$REPO_ROOT_DIR/clients/python/target"
DIST_DIR="$TARGET_DIR/dist"

# Sync pinned build tools from the lock file
cd "$REPO_ROOT_DIR/clients/python"
uv sync --group package-build --frozen
# shellcheck source=/dev/null
source .venv/bin/activate

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
