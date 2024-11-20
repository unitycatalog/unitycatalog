#!/bin/bash

set -euo pipefail

COMPOSE_DIR="${1:-$GITHUB_WORKSPACE}"

echo "Tearing down Unity Catalog service with Docker Compose in directory: $COMPOSE_DIR"

cd "$COMPOSE_DIR"

docker compose down

echo "Unity Catalog service torn down."
