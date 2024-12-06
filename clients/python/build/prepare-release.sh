#!/bin/bash

# Script for updating version numbers in preparation for a release.
# Usage: ./prepare_release.sh <release_version>
# Example: ./prepare_release.sh 1.2.3

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
  echo -e "${BOLD}Usage:${NC} $0 <release_version>"
  echo -e "${BOLD}Example:${NC} $0 1.2.3"
  exit 1
}

update_version_in_file() {
  local file_path="$1"
  local pattern="$2"
  local new_version_line="$3"
  local temp_file

  if [[ -f "$file_path" ]]; then
    temp_file=$(mktemp)
    while IFS= read -r line; do
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

update_init_version() {
  local file_path="$1"
  local new_version="$2"
  local temp_file

  if [[ -f "$file_path" ]]; then
    temp_file=$(mktemp)
    while IFS= read -r line; do
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

if [[ $# -ne 1 ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Release version not provided."
  usage
fi

RELEASE_VERSION="$1"

# Version validation regex to accept formats like:
# 1.2.3, 1.2.3-beta, 1.2.3beta, 1.2.3.beta, 1.2.3rc1, 1.2.3.dev0, etc.
if [[ ! "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-]?[A-Za-z0-9]+)?$ ]]; then
  echo -e "${RED}${BOLD}Error:${NC} ${RED}Invalid release version format.${NC}"
  usage
fi

REPO_ROOT="$(pwd)"

PYPROJECT_PATH="$REPO_ROOT/clients/python/target/pyproject.toml"
SETUP_PY_PATH="$REPO_ROOT/clients/python/target/setup.py"
INIT_PY_PATH="$REPO_ROOT/clients/python/target/src/unitycatalog/client/__init__.py"

# Update pyproject.toml
PYPROJECT_PATTERN='^[[:space:]]*version[[:space:]]*='
PYPROJECT_NEW_LINE="version = \"$RELEASE_VERSION\""
update_version_in_file "$PYPROJECT_PATH" "$PYPROJECT_PATTERN" "$PYPROJECT_NEW_LINE"

# Update setup.py
SETUP_PY_PATTERN='^[[:space:]]*version[[:space:]]*='
SETUP_PY_NEW_LINE="    version=\"$RELEASE_VERSION\","
update_version_in_file "$SETUP_PY_PATH" "$SETUP_PY_PATTERN" "$SETUP_PY_NEW_LINE"

# Update __init__.py
INIT_PY_PATTERN='^__version__[[:space:]]*=[[:space:]]*\"[^\"]+\"'
update_init_version "$INIT_PY_PATH" "$RELEASE_VERSION"

validate_version_in_file "$PYPROJECT_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"
validate_version_in_file "$SETUP_PY_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"
validate_version_in_file "$INIT_PY_PATH" '^__version__' "$RELEASE_VERSION"

echo -e "${GREEN}${BOLD}All files have been successfully updated to version $RELEASE_VERSION.${NC}"
