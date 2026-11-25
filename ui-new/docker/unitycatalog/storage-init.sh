#!/bin/sh
# Registers RustFS with Unity Catalog as an s3_compatible storage credential and
# an external location for the bucket, so the server reaches the bucket at
# RustFS's endpoint and vends short-lived, path-scoped sessions for it. Runs on
# every start: an existing credential or location is updated to the current
# settings, which also re-encrypts the credential with the current key.

set -eu

server="${UC_SERVER:-http://unitycatalog:8080}"
endpoint="${RUSTFS_ENDPOINT:-http://rustfs:9000}"
# The bucket config-init creates; see the note there on why it is fixed.
bucket=uc-warehouse
credential=rustfs
location=rustfs_warehouse

log() {
  echo "[storage-init] $*"
}

uc() {
  ./bin/uc "$@" --server "$server" --output json
}

# The CLI exits 0 when it rejects its arguments, printing help instead of JSON,
# so a call succeeded only if its JSON output names the entity.
names() {
  printf '%s\n' "$2" | grep -q "[{,]\"name\":\"$1\"[,}]"
}

fail() {
  log "$1" >&2
  printf '%s\n' "$2" | grep -v '^[[:space:]]*at ' >&2
  exit 1
}

# Compose starts this once the server is healthy, but `docker compose restart`
# restarts every service at once.
log "waiting for Unity Catalog at $server"
attempt=0
until uc catalog list >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    log "Unity Catalog did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

# Creates the entity when the server reports it missing and updates it when it
# exists. Any other answer stops the script with the CLI's output.
register() {
  entity=$1
  name=$2
  shift 2
  if output="$(uc "$entity" get --name "$name" 2>&1)" && names "$name" "$output"; then
    action=update
  elif printf '%s\n' "$output" | grep -q '"error_code":"NOT_FOUND"'; then
    action=create
  else
    fail "could not look up $entity $name:" "$output"
  fi
  log "${action%e}ing $entity $name"
  if ! output="$(uc "$entity" "$action" --name "$name" "$@" 2>&1)" || ! names "$name" "$output"; then
    fail "could not $action $entity $name:" "$output"
  fi
}

# RustFS ignores the role: its STS issues sessions with the key's own
# permissions, narrowed by the session policy the server sends.
register credential "$credential" \
  --s3_endpoint "$endpoint" \
  --s3_access_key_id "${RUSTFS_ACCESS_KEY:-rustfsadmin}" \
  --s3_secret_access_key "${RUSTFS_SECRET_KEY:-rustfsadmin}" \
  --s3_role_arn arn:aws:iam::000000000000:role/unity-catalog

register external_location "$location" \
  --url "s3://$bucket" \
  --credential_name "$credential"
