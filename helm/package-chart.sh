#!/bin/bash

# Packages the chart for a Unity Catalog release: chart version X.Y.Z runs server image vX.Y.Z.
# Usage: helm/package-chart.sh <version> [destination]

set -euo pipefail

version="${1:?Usage: $0 <version without the leading v, e.g. 0.8.0> [destination]}"
destination="${2:-.}"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Version must be X.Y.Z without a leading v, got: $version" >&2
  exit 1
fi

helm package "$(dirname "$0")" --version "$version" --app-version "v$version" --destination "$destination"
