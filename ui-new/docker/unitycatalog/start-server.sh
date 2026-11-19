#!/bin/sh
# Runs bin/start-uc-server with UC_SERVER_CREDENTIAL_ENCRYPTION_KEY, which the
# Helm chart sets from server.credentialEncryptionKeySecret. When it is empty,
# the key config-init generated into the configuration volume is used instead.

set -eu

if [ -z "${UC_SERVER_CREDENTIAL_ENCRYPTION_KEY:-}" ]; then
  key_file=etc/conf/credential-encryption.key
  if [ ! -s "$key_file" ]; then
    echo "[start-server] $key_file is missing; config-init writes it, so start the server with docker compose up" >&2
    exit 1
  fi
  UC_SERVER_CREDENTIAL_ENCRYPTION_KEY="$(cat "$key_file")"
  export UC_SERVER_CREDENTIAL_ENCRYPTION_KEY
fi

exec ./bin/start-uc-server "$@"
