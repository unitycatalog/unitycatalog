#!/bin/bash

set -euo pipefail

# Usage: wait_unitycatalog.sh [health_check_url] [max_retries] [sleep_interval]

# Default values
HEALTH_CHECK_URL="${1:-http://localhost:8080/api/2.1/unity-catalog/catalogs}"
MAX_RETRIES="${2:-30}"
SLEEP_INTERVAL="${3:-2}"

echo "Waiting for Unity Catalog service to be ready at $HEALTH_CHECK_URL"

for ((i=1; i<=MAX_RETRIES; i++)); do
    echo "Attempt $i/$MAX_RETRIES: Checking service..."

    # Attempt to fetch the catalogs
    if response=$(curl -s "$HEALTH_CHECK_URL"); then
        # Check if the response contains '"name":"unity"'
        if echo "$response" | grep -q '"name":"unity"'; then
            echo "Unity Catalog service is ready."
            exit 0
        else
            echo "Attempt $i/$MAX_RETRIES: 'unity' catalog not found. Retrying in $SLEEP_INTERVAL seconds..."
        fi
    else
        echo "Attempt $i/$MAX_RETRIES: Failed to connect to $HEALTH_CHECK_URL. Retrying in $SLEEP_INTERVAL seconds..."
    fi

    sleep "$SLEEP_INTERVAL"
done

echo "Unity Catalog service did not become ready within expected time."
exit 1
