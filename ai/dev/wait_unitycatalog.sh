#!/bin/bash

set -euo pipefail

# Usage: wait_unitycatalog.sh <health_check_url> [max_retries] [sleep_interval]

HEALTH_CHECK_URL="${1:-http://localhost:8080/api/2.1/unity-catalog/catalogs}"
MAX_RETRIES="${2:-30}"
SLEEP_INTERVAL="${3:-2}"

echo "Waiting for Unity Catalog service to be ready at $HEALTH_CHECK_URL"

for ((i=1;i<=MAX_RETRIES;i++)); do
    # Fetch the response and HTTP status code
    response=$(curl -s -w "%{http_code}" "$HEALTH_CHECK_URL")
    http_status="${response: -3}"
    body="${response::-3}"

    if [ "$http_status" -eq 200 ] && echo "$body" | grep -q '"name":"unity"'; then
        echo "Unity Catalog service is ready."
        exit 0
    else
        echo "Attempt $i/$MAX_RETRIES: Service not ready yet. Retrying in $SLEEP_INTERVAL seconds..."
        sleep "$SLEEP_INTERVAL"
    fi
done

echo "Unity Catalog service did not become ready within expected time."
exit 1
