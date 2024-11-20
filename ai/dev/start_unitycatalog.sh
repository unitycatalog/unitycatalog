#!/bin/bash

set -euo pipefail

COMPOSE_DIR="${1:-$GITHUB_WORKSPACE}"

echo "Starting Unity Catalog service with Docker Compose in directory: $COMPOSE_DIR"

cd "$COMPOSE_DIR"

docker compose up -d

echo "Unity Catalog service started."
