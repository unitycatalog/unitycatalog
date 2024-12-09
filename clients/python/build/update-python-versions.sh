#!/bin/bash

# Script for updating the Python versions in pyproject.toml and setup.py to keep versions 
# synchronized with version.sbt

set -e

read_sbt_version() {
  local version_sbt_path="$1"
  local version
  version=$(cut -d '"' -f2 "$version_sbt_path")
  if [[ -n "$version" ]]; then
    echo "$version"
  else
    echo "Error: Could not find version in $version_sbt_path" >&2
    exit 1
  fi
}

convert_version_for_python() {
  local sbt_version="$1"
  if [[ $sbt_version == *-SNAPSHOT ]]; then
    echo "${sbt_version%-SNAPSHOT}.dev0"
  else
    echo "$sbt_version"
  fi
}

update_version_in_pyproject() {
  local file_path="$1"
  local python_version="$2"
  if [[ -f "$file_path" ]]; then
    > tmp
    while IFS= read -r line; do
      if [[ $line =~ ^version[[:space:]]*= ]]; then
        echo "version = \"$python_version\"" >> tmp
      else
        echo "$line" >> tmp
      fi
    done < "$file_path"
    mv tmp "$file_path"
    echo "Updated version in $file_path to $python_version"
  else
    echo "Error: File not found: $file_path" >&2
    exit 1
  fi
}

update_version_in_setup() {
  local file_path="$1"
  local python_version="$2"
  if [[ -f "$file_path" ]]; then
    > tmp
    while IFS= read -r line; do
      if [[ $line =~ ^[[:space:]]*version[[:space:]]*= ]]; then
        echo "    version=\"$python_version\"," >> tmp
      else
        echo "$line" >> tmp
      fi
    done < "$file_path"
    mv tmp "$file_path"
    echo "Updated version in $file_path to $python_version"
  else
    echo "Error: File not found: $file_path" >&2
    exit 1
  fi
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
version_sbt_path="$script_dir/../../../version.sbt"

if [[ ! -f "$version_sbt_path" ]]; then
  echo "Error: version.sbt file not found at $version_sbt_path" >&2
  exit 1
fi

sbt_version="$(read_sbt_version "$version_sbt_path")"
echo "SBT version: $sbt_version"

python_version="$(convert_version_for_python "$sbt_version")"
echo "Python version: $python_version"

update_version_in_pyproject "$script_dir/pyproject.toml" "$python_version"
update_version_in_setup "$script_dir/setup.py" "$python_version"
