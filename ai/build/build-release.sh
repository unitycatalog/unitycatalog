#!/bin/bash

# Script for updating version numbers, building, and validating core and integrations.
# Usage: ./prepare_release.sh <release_version> <package1> [<package2> ...]
# Example: ./prepare_release.sh 1.2.3 core langchain openai

# Exit immediately if a command exits with a non-zero status.
set -e

# Define color codes for output formatting.
RED='\033[0;31m'    # Red color for error messages
GREEN='\033[0;32m'  # Green color for success messages
BOLD='\033[1m'       # Bold text
NC='\033[0m'         # No Color (reset)

# --------------------------------------------------
# Function: usage
# Description:
#   Displays the correct usage of the script and exits.
# Parameters:
#   None
# Usage:
#   usage
# --------------------------------------------------
usage() {
  echo -e "${BOLD}Usage:${NC} $0 <release_version> <package1> [<package2> ...]"
  echo -e "${BOLD}Example:${NC} $0 1.2.3 core langchain openai"
  exit 1
}

# --------------------------------------------------
# Function: update_version_in_file
# Description:
#   Updates the version number in a specified file by replacing lines that match a given pattern.
# Parameters:
#   $1 - Path to the file to be updated.
#   $2 - Regex pattern to identify the line containing the version.
#   $3 - The new version line to replace the matched line.
# Usage:
#   update_version_in_file <file_path> <pattern> <new_version_line>
# --------------------------------------------------
update_version_in_file() {
  local file_path="$1"
  local pattern="$2"
  local new_version_line="$3"
  local temp_file

  if [[ -f "$file_path" ]]; then
    # Create a temporary file to store the updated content.
    temp_file=$(mktemp)
    while IFS= read -r line; do
      if [[ $line =~ $pattern ]]; then
        # Replace the matched line with the new version line.
        echo "$new_version_line" >> "$temp_file"
      else
        # Keep the original line.
        echo "$line" >> "$temp_file"
      fi
    done < "$file_path"
    # Overwrite the original file with the updated content.
    mv "$temp_file" "$file_path"
    echo "Updated version in $file_path"
  else
    # If the file does not exist, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} File not found: $file_path" >&2
    exit 1
  fi
}

# --------------------------------------------------
# Function: update_version_py
# Description:
#   Specifically updates the VERSION variable in a Python version.py file.
# Parameters:
#   $1 - Path to the version.py file.
#   $2 - The new version string.
# Usage:
#   update_version_py <file_path> <new_version>
# --------------------------------------------------
update_version_py() {
  local file_path="$1"
  local new_version="$2"
  local temp_file

  if [[ -f "$file_path" ]]; then
    # Create a temporary file to store the updated content.
    temp_file=$(mktemp)
    while IFS= read -r line; do
      if [[ $line =~ ^VERSION[[:space:]]*=[[:space:]]*\"[^\"]+\" ]]; then
        # Replace the VERSION line with the new version.
        echo "VERSION = \"$new_version\"" >> "$temp_file"
      else
        # Keep the original line.
        echo "$line" >> "$temp_file"
      fi
    done < "$file_path"
    # Overwrite the original file with the updated content.
    mv "$temp_file" "$file_path"
    echo "Updated VERSION in $file_path"
  else
    # If the file does not exist, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} File not found: $file_path" >&2
    exit 1
  fi
}

# --------------------------------------------------
# Function: validate_version_in_file
# Description:
#   Validates that the version number in a specified file matches the expected version.
# Parameters:
#   $1 - Path to the file to validate.
#   $2 - Regex pattern to extract the version.
#   $3 - The expected version string.
# Usage:
#   validate_version_in_file <file_path> <pattern> <expected_version>
# --------------------------------------------------
validate_version_in_file() {
  local file_path="$1"
  local pattern="$2"
  local expected_version="$3"
  local actual_version

  if [[ -f "$file_path" ]]; then
    # Extract the actual version from the file using grep and awk.
    actual_version=$(grep -E "$pattern" "$file_path" | head -n1 | awk -F '"' '{print $2}')
    if [[ "$actual_version" == "$expected_version" ]]; then
      # If versions match, output a success message.
      echo -e "Validation passed for $file_path: version is ${BOLD}${GREEN}$expected_version${NC}"
    else
      # If versions do not match, output an error and exit.
      echo -e "${RED}${BOLD}Error:${NC} Validation failed for $file_path. Expected version: $expected_version, Found: $actual_version" >&2
      exit 1
    fi
  else
    # If the file does not exist during validation, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} File not found during validation: $file_path" >&2
    exit 1
  fi
}

# --------------------------------------------------
# Function: get_package_path
# Description:
#   Determines the file system path for a given package name.
# Parameters:
#   $1 - Name of the package (e.g., core, langchain, openai).
# Returns:
#   Echoes the path to the package directory.
# Usage:
#   get_package_path <package_name>
# --------------------------------------------------
get_package_path() {
  local package="$1"
  if [[ "$package" == "core" ]]; then
    echo "$AI_DIR/core"
  else
    echo "$INTEGRATIONS_DIR/$package"
  fi
}

# --------------------------------------------------
# Function: process_package
# Description:
#   Processes a single package by updating its version, building the package, and validating the build.
# Parameters:
#   $1 - Name of the package to process.
# Usage:
#   process_package <package_name>
# --------------------------------------------------
process_package() {
  local package="$1"
  local package_path
  package_path=$(get_package_path "$package")

  if [[ ! -d "$package_path" ]]; then
    # If the package directory does not exist, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} Package directory not found: $package_path" >&2
    exit 1
  fi

  echo -e "${BOLD}Processing package:${NC} $package"

  # Define paths to the files that need to be updated.
  PYPROJECT_PATH="$package_path/pyproject.toml"
  VERSION_PY_PATH="$package_path/src/unitycatalog/ai/${package}/version.py"

  # Define the pattern and new line for updating pyproject.toml.
  PYPROJECT_PATTERN='^[[:space:]]*version[[:space:]]*='
  PYPROJECT_NEW_LINE="version = \"$RELEASE_VERSION\""
  
  # Update the version in pyproject.toml.
  update_version_in_file "$PYPROJECT_PATH" "$PYPROJECT_PATTERN" "$PYPROJECT_NEW_LINE"

  # Update the VERSION variable in version.py.
  update_version_py "$VERSION_PY_PATH" "$RELEASE_VERSION"

  # Validate that the versions have been correctly updated.
  validate_version_in_file "$PYPROJECT_PATH" '^[[:space:]]*version[[:space:]]*=' "$RELEASE_VERSION"
  validate_version_in_file "$VERSION_PY_PATH" '^VERSION' "$RELEASE_VERSION"

  DIST_DIR="$package_path/dist"

  if [[ -d "$DIST_DIR" ]]; then
    # If the dist directory exists, clean it to remove old build artifacts.
    echo "Cleaning existing dist directory: $DIST_DIR"
    rm -rf "$DIST_DIR"
  fi

  echo "Building the package for $package..."
  # Navigate to the package directory and build the package.
  (cd "$package_path" && python -m build --outdir dist)

  echo "Running twine check on the built package..."
  TWINE_FILES=("$DIST_DIR"/*)
  if [[ -z "${TWINE_FILES[*]}" ]]; then
    # If no build artifacts are found, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} No build artifacts found in $DIST_DIR" >&2
    exit 1
  fi

  # Validate the built package using twine.
  python -m twine check "$DIST_DIR"/*

  echo -e "${GREEN}Successfully built and validated package '$package' version $RELEASE_VERSION.${NC}"
}

# --------------------------------------------------
# Main Script Logic
# --------------------------------------------------

# Check if at least two arguments are provided (release_version and at least one package).
if [[ $# -lt 2 ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Insufficient arguments. Provide a version number and at least one package name." >&2
  usage
fi

# Assign the first argument to RELEASE_VERSION and shift to process package names.
RELEASE_VERSION="$1"
shift
PACKAGES=("$@")

# Version validation regex to accept formats like:
# 1.2.3, 1.2.3-beta, 1.2.3beta, 1.2.3.beta, 1.2.3rc1, 1.2.3.dev0, etc.
if [[ ! "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-]?[A-Za-z0-9]+)?$ ]]; then
  echo -e "${RED}${BOLD}Error:${NC} Invalid release version format."
  usage
fi

# Define repository root and directories for AI core and integrations.
REPO_ROOT="$(pwd)"
AI_DIR="$REPO_ROOT/ai"
INTEGRATIONS_DIR="$AI_DIR/integrations"

# --------------------------------------------------
# Function: validate_package
# Description:
#   Validates that the provided package name is either 'core' or exists within the integrations directory.
# Parameters:
#   $1 - Name of the package to validate.
# Usage:
#   validate_package <package_name>
# --------------------------------------------------
validate_package() {
  local package="$1"
  if [[ "$package" != "core" && ! -d "$INTEGRATIONS_DIR/$package" ]]; then
    # If the package is neither 'core' nor a valid integration, output an error and exit.
    echo -e "${RED}${BOLD}Error:${NC} Invalid package name: $package" >&2
    exit 1
  fi
}

# Validate each provided package name.
for package in "${PACKAGES[@]}"; do
  validate_package "$package"
done

# Process each validated package.
for package in "${PACKAGES[@]}"; do
  process_package "$package"
done

# Output a success message after all packages have been processed.
echo -e "${GREEN}${BOLD}All specified packages have been successfully updated to version $RELEASE_VERSION.${NC}"
