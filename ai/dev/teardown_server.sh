#!/bin/bash

set -euo pipefail

echo "Stopping Unity Catalog Server..."

if [ -f uc_server.pid ]; then
    PID=$(cat uc_server.pid)
    echo "Found PID file with PID: $PID"
    if kill -0 "$PID" 2>/dev/null; then
        echo "Killing UC server (PID $PID)..."
        kill "$PID" || echo "Failed to kill PID $PID"
        sleep 2
        # Force kill if still running
        if kill -0 "$PID" 2>/dev/null; then
            echo "Force killing UC server (PID $PID)..."
            kill -9 "$PID" 2>/dev/null || true
        fi
        echo "UC server stopped successfully"
    else
        echo "Process with PID $PID is not running"
    fi
    rm -f uc_server.pid
else
    echo "No PID file found. Attempting to find UC server process..."
    # If PID file doesn't exist, attempt to find and kill the server process
    UC_PIDS=$(pgrep -f 'io.unitycatalog.server.UnityCatalogServer' || true)
    if [ -n "$UC_PIDS" ]; then
        echo "Found UC server process(es): $UC_PIDS"
        echo "$UC_PIDS" | xargs -r kill || echo "Failed to kill some processes"
        sleep 2
        # Force kill any remaining
        UC_PIDS=$(pgrep -f 'io.unitycatalog.server.UnityCatalogServer' || true)
        if [ -n "$UC_PIDS" ]; then
            echo "Force killing remaining process(es): $UC_PIDS"
            echo "$UC_PIDS" | xargs -r kill -9 2>/dev/null || true
        fi
        echo "UC server stopped successfully"
    else
        echo "No UC server process found (may already be stopped)"
    fi
fi

echo "Teardown complete"