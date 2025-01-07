#!/bin/bash

# Validates the generated package based on the existence of specific files within .whl and .tar.gz archives

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT_DIR="$(cd "$SCRIPT_DIR/../../../" && pwd)"
cd "$REPO_ROOT_DIR"

TARGET_DIR="$REPO_ROOT_DIR/clients/python/target/dist"

# Files to check within the archives
required_files=(
    "api_client.py"
    "configuration.py"
    "create_table.py"
    "create_catalog.py"
    "create_schema.py"
)

# Define color codes for formatting
GREEN='\033[1;32m'
NB_GREEN='\033[0;32m'
RED='\033[1;31m'
NC='\033[0m'

# Loop through each file in the dist folder and check if it's a .whl or .tar.gz file
for archive in "$TARGET_DIR"/*.whl "$TARGET_DIR"/*.tar.gz; do
    if [ -f "$archive" ]; then
        echo "Checking archive: $archive"

        # Extract the archive temporarily
        temp_dir=$(mktemp -d)
        if [[ "$archive" == *.whl ]]; then
            unzip -q "$archive" -d "$temp_dir"  # Extract .whl file
        elif [[ "$archive" == *.tar.gz ]]; then
            tar -xzf "$archive" -C "$temp_dir"  # Extract .tar.gz file
        fi

        # Look for the required files inside the extracted directory
        for file in "${required_files[@]}"; do
            file_path=$(find "$temp_dir" -type f -name "$file")
            if [ -n "$file_path" ]; then
                echo -e "${NB_GREEN}File $file found in $archive.${NC}"
            else
                echo -e "${RED}ERROR: File $file not found in $archive.${NC}"
                rm -rf "$temp_dir"
                exit 1
            fi
        done

        # Clean up the temporary directory after checking
        rm -rf "$temp_dir"
    fi
done

echo -e "${GREEN}All required files are present in the archives.${NC}"
