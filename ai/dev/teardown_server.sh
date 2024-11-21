#!/bin/bash

set -euo pipefail

if [ -f uc_server.pid ]; then
    kill $(cat uc_server.pid) || echo "Server already stopped"
else
    # If PID file doesn't exist, attempt to find and kill the server process
    UC_PID=$(pgrep -f 'bin/start-uc-server')
    if [ -n "$UC_PID" ]; then
        kill $UC_PID
    fi
fi