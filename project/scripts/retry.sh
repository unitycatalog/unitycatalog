#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -lt 2 ]]; then
  echo "usage: $0 <attempts> <command> [args...]"
  exit 2
fi

attempts="$1"
shift

for attempt in $(seq 1 "$attempts"); do
  if "$@"; then
    exit 0
  fi

  if [[ "$attempt" -eq "$attempts" ]]; then
    exit 1
  fi

  echo "Command failed on attempt $attempt/$attempts; retrying after a short backoff"
  sleep 5
done
