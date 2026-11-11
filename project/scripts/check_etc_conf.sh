#!/usr/bin/env bash
# Fails if any etc/conf directory differs from the checkout: a new file (ignored or not), or an
# edited or deleted tracked one. Run in CI after the tests: a server built without its own
# configurationFolder writes its signing keys, certs.json and token.txt into etc/conf.
set -euo pipefail

changed=$(git status --porcelain --ignored --untracked-files=all -- ':(glob)**/etc/conf/**')
if [ -n "$changed" ]; then
  echo "::error::Tests changed etc/conf; give each test server its own configurationFolder:"
  echo "$changed"
  exit 1
fi
