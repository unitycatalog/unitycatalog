#!/bin/bash

# Script for updating version numbers in preparation for a release.
# Usage: ./prepare_release.sh <release_version>
# Example: ./prepare_release.sh 1.2.3

# Exit immediately if a command exits with a non-zero status.
set -e

# Define color codes for output formatting.
RED='\033[0;31m'    # Red color for errors
GREEN='\033[0;32m'  # Green color for success messages
BOLD='\033[1m'       # Bold text
NC='\033[0m'         # No Color (reset)

# Function: usage
# Description:
#   Displays the correct usage of the script and exits.
usage() {
  echo -e "${BOLD}Usage:${NC} $0 <release_version>"
  echo -e "${BOLD}Example:${NC} $0 1.2.3"
  exit 1
}

# Function: update_version_in_file
# Description:
#   Updates the version number in a specified file by replacing lines matching a pattern.
# Parameters:
#   $1 - Path to the file to be updated.
#   $2 - Regex pattern to identify the line containing the version.
#   $3 - The new version line to replace the matched line.
update_version_in_file() {
  local file_path="$1"
  local pattern="$2"
  local new_version_line="$3"
  local temp_file

  if [[ -f "$file_path" ]]; then
    temp_file=$(mktemp)
    while IFS= read -r line; do
      # If the line matches the pattern, replace it with the new version line.
      if [[ $line =~ $pattern ]]; then
        echo "$new_version_line" >> "$temp_file"
      else
        echo "$line" >> "$temp_file"
      fi
    done < "$file_path"
    mv "$temp_file" "$file_path"
    echo "Updated version in $file_path"
  else
    echo -e "${RED}${BOLD}Error:${NC} File not found: $file_path" >&2
    exit 1
  fi
}

# Function: update_init_version
# Description:
#   Specifically updates the __version__ variable in a Python __init__.py file.
# Parameters:
#   $1 - Path to the __init__.py file.
#   $2 - The new version string.
update_init_version() {
  local file_path="$1"
  local new_version="$2"
  local temp_file

  if [[ -f "$file_path" ]]; then
    temp_file=$(mktemp)
    while IFS= read -r line; do
      # If the line defines __version__, replace it with the new version.
      if [[ $line =~ ^__version__[[:space:]]*=[[:space:]]*\"[^\"]+\" ]]; then
        echo "__version__ = \"$new_version\"" >> "$temp_file"
      else
        echo "$line" >> "$temp_file"
      fi
    done < "$file_path"
    mv "$temp_file" "$file_path"
    echo "Updated __version__ in $file_path"
  else
    echo -e "${RED}${BOLD}Error:${NC} File not found: $file_path" >&2
    exit 1
  fi
}

# Function: validate_version_in_file
# Description:
#   Validates that the version number in a specified file matches the expected version.
# Parameters:
#   $1 - Path to the file to validate.
#   $2 - Regex pattern to extract the version.
#   $3 - The expected version string.
validate_version_in_file() {
  local file_path="$1"
  local pattern="$2"
  local expected_version="$3"
  local actual_version

  if [[ -f "$file_path" ]]; then
    actual_version=$(grep -E "$pattern" "$file_path" | head -n1 | awk -F '"' '{print $2}')
    if [[ "$actual_version" == "$expected_version" ]]; then
      echo -e "Validation passed for $file_path: version is ${BOLD}${GREEN}$expected_version${NC}"
    else
      echo -e "${RED}${BOLD}Error:${NC} Validation failed for $file_path. Expected version: $expected_version, Found: $actual_version" >&2
      exit 1
    fi
  else
    echo -e "${RED}${BOLD}Error:${NC} File not found during validation: $file_path" >&2
    exit 1
  fi
}

# -------------------- Main Script Logic --------------------

# Check if exactly one argument (release version) is provided.
if [[ $# -ne 1 ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Release version not provided."
  usage
fi

# Assign the first argument to RELEASE_VERSION variable.
RELEASE_VERSION="$1"

# Version validation regex to accept formats like:
# 1.2.3, 1.2.3-beta, 1.2.3beta, 1.2.3.beta, 1.2.3rc1, 1.2.3.dev0, etc.
if [[ ! "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-]?[A-Za-z0-9]+)?$ ]]; then
  echo -e "${RED}${BOLD}Error:${NC} ${RED}Invalid release version format.${NC}"
  usage
fi

# Define the root directory of the repository.
REPO_ROOT="$(pwd)"

# Define paths to the files that need to be updated with the new version.
PYPROJECT_PATH="$REPO_ROOT/clients/python/target/pyproject.toml"
SETUP_PY_PATH="$REPO_ROOT/clients/python/target/setup.py"
INIT_PY_PATH="$REPO_ROOT/clients/python/target/src/unitycatalog/client/__init__.py"

# -------------------- Updating Version Files --------------------

# Update version in pyproject.toml
# Pattern matches lines starting with 'version ='
PYPROJECT_PATTERN='^[[:space:]]*version[[:space:]]*='
PYPROJECT_NEW_LINE="version = \"$RELEASE_VERSION\""
update_version_in_file "$PYPROJECT_PATH" "$PYPROJECT_PATTERN" "$PYPROJECT_NEW_LINE"

# Update version in setup.py
# Pattern matches lines starting with 'version ='
SETUP_PY_PATTERN='^[[:space:]]*version[[:space:]]*='
SETUP_PY_NEW_LINE="    version=\"$RELEASE_VERSION\","
update_version_in_file "$SETUP_PY_PATH" "$SETUP_PY_PATTERN" "$SETUP_PY_NEW_LINE"

# Update __version__ in __init__.py
INIT_PY_PATTERN='^__version__[[:space:]]*=[[:space:]]*\"[^\"]+\"'
update_init_version "$INIT_PY_PATH" "$RELEASE_VERSION"

# -------------------- Validating Updates --------------------

# Validate that pyproject.toml has been updated correctly.
validate_version_in_file "$PYPROJECT_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"

# Validate that setup.py has been updated correctly.
validate_version_in_file "$SETUP_PY_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"

# Validate that __init__.py has been updated correctly.
validate_version_in_file "$INIT_PY_PATH" '^__version__' "$RELEASE_VERSION"

# -------------------- Completion Message --------------------

# Output a success message indicating all files have been updated.
echo -e "${GREEN}${BOLD}All files have been successfully updated to version $RELEASE_VERSION.${NC}"
