#!/bin/bash

# Script for updating version numbers, building, and validating core and integrations.
# Usage: ./prepare_release.sh <release_version> <package1> [<package2> ...]
# Example: ./prepare_release.sh 1.2.3 core langchain openai

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
  echo -e "${BOLD}Usage:${NC} $0 <release_version> <package1> [<package2> ...]"
  echo -e "${BOLD}Example:${NC} $0 1.2.3 core langchain openai"
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

update_version_py() {
  local file_path="$1"
  local new_version="$2"
  local temp_file

  if [[ -f "$file_path" ]]; then
    temp_file=$(mktemp)
    while IFS= read -r line; do
      if [[ $line =~ ^VERSION[[:space:]]*=[[:space:]]*\"[^\"]+\" ]]; then
        echo "VERSION = \"$new_version\"" >> "$temp_file"
      else
        echo "$line" >> "$temp_file"
      fi
    done < "$file_path"
    mv "$temp_file" "$file_path"
    echo "Updated VERSION in $file_path"
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

get_package_path() {
  local package="$1"
  if [[ "$package" == "core" ]]; then
    echo "$AI_DIR/core"
  else
    echo "$INTEGRATIONS_DIR/$package"
  fi
}

process_package() {
  local package="$1"
  local package_path
  package_path=$(get_package_path "$package")

  if [[ ! -d "$package_path" ]]; then
    echo -e "${RED}${BOLD}Error:${NC} Package directory not found: $package_path" >&2
    exit 1
  fi

  echo -e "${BOLD}Processing package:${NC} $package"

  PYPROJECT_PATH="$package_path/pyproject.toml"
  VERSION_PY_PATH="$package_path/src/unitycatalog/ai/${package}/version.py"

  PYPROJECT_PATTERN='^[[:space:]]*version[[:space:]]*='
  PYPROJECT_NEW_LINE="version = \"$RELEASE_VERSION\""
  update_version_in_file "$PYPROJECT_PATH" "$PYPROJECT_PATTERN" "$PYPROJECT_NEW_LINE"

  update_version_py "$VERSION_PY_PATH" "$RELEASE_VERSION"

  validate_version_in_file "$PYPROJECT_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"
  validate_version_in_file "$VERSION_PY_PATH" '^VERSION' "$RELEASE_VERSION"

  DIST_DIR="$package_path/dist"

  if [[ -d "$DIST_DIR" ]]; then
    echo "Cleaning existing dist directory: $DIST_DIR"
    rm -rf "$DIST_DIR"
  fi

  echo "Building the package for $package..."
  (cd "$package_path" && python -m build --outdir dist)

  echo "Running twine check on the built package..."
  TWINE_FILES=("$DIST_DIR"/*)
  if [[ -z "${TWINE_FILES[*]}" ]]; then
    echo -e "${RED}${BOLD}Error:${NC} No build artifacts found in $DIST_DIR" >&2
    exit 1
  fi

  python -m twine check "$DIST_DIR"/*

  echo -e "${GREEN}Successfully built and validated package '$package' version $RELEASE_VERSION.${NC}"
}

if [[ $# -lt 2 ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Insufficient arguments. Provide a version number and at least one package name." >&2
  usage
fi

RELEASE_VERSION="$1"
shift
PACKAGES=("$@")

# Version validation regex to accept formats like:
# 1.2.3, 1.2.3-beta, 1.2.3beta, 1.2.3.beta, 1.2.3rc1, 1.2.3.dev0, etc.
if [[ ! "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-]?[A-Za-z0-9]+)?$ ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Invalid release version format."
  usage
fi

REPO_ROOT="$(pwd)"
AI_DIR="$REPO_ROOT/ai"
INTEGRATIONS_DIR="$AI_DIR/integrations"

validate_package() {
  local package="$1"
  if [[ "$package" != "core" && ! -d "$INTEGRATIONS_DIR/$package" ]]; then
    echo -e "${RED}${BOLD}Error:${NC} Invalid package name: $package" >&2
    exit 1
  fi
}

for package in "${PACKAGES[@]}"; do
  validate_package "$package"
done

for package in "${PACKAGES[@]}"; do
  process_package "$package"
done

echo -e "${GREEN}${BOLD}All specified packages have been successfully updated to version $RELEASE_VERSION.${NC}"
