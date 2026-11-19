#!/bin/sh
# Runs config-init.sh against a stand-in aws and temporary directories, three
# times, and checks the configuration it writes and the key it keeps.

set -eu

script="$(cd "$(dirname "$0")" && pwd)/config-init.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin" "$work/templates" "$work/conf"
printf 'server.port=8080\n' > "$work/templates/server.properties"
printf 'hibernate.connection.url=jdbc:h2:file:./etc/db/h2db\n' > "$work/templates/hibernate.properties"
conf="$work/conf"
key="$conf/credential-encryption.key"

# The stand-in reports the bucket as existing once create-bucket has run.
cat > "$work/bin/aws" <<'EOF'
#!/bin/sh
case "$*" in
  *head-bucket*) [ -f "$AWS_STATE/bucket" ] ;;
  *create-bucket*) touch "$AWS_STATE/bucket"; echo create-bucket >> "$AWS_STATE/calls" ;;
esac
EOF
chmod +x "$work/bin/aws"
: > "$work/calls"

run() {
  if ! PATH="$work/bin:$PATH" AWS_STATE="$work" CONF_DIR="$conf" \
    TEMPLATES_DIR="$work/templates" CONF_OWNER="$(id -u):$(id -g)" \
    sh "$script" > "$work/out" 2>&1; then
    echo "FAIL config-init exited non-zero:"
    sed 's/^/     /' "$work/out"
    exit 1
  fi
}

failures=0

# expect <description> <test arguments>
expect() {
  description=$1
  shift
  if [ "$@" ]; then
    echo "ok   $description"
  else
    echo "FAIL $description"
    failures=$((failures + 1))
  fi
}

count() {
  grep -c -x -F "$1" "$2" || true
}

last_url() {
  grep '^hibernate.connection.url=' "$conf/hibernate.properties" | tail -n 1
}

run
expect "creates the bucket" "$(count create-bucket "$work/calls")" = 1
expect "keeps the template's settings" "$(count server.port=8080 "$conf/server.properties")" = 1
expect "appends the storage overrides" \
  "$(count server.s3-compatible-storage.enabled=true "$conf/server.properties")" = 1
expect "points the server at PostgreSQL" \
  "$(last_url)" = hibernate.connection.url=jdbc:postgresql://postgres:5432/ucdb
expect "leaves no temporary files" -z "$(find "$conf" -name '*.tmp')"
expect "generates a 32-byte key" "$(base64 -d < "$key" | wc -c | tr -d ' ')" = 32
expect "keeps the key private" -n "$(find "$key" -perm 600)"
first_key="$(cat "$key")"

run
expect "appends the overrides once on a second run" \
  "$(count server.s3-compatible-storage.enabled=true "$conf/server.properties")" = 1
expect "keeps the key on a second run" "$(cat "$key")" = "$first_key"
expect "doesn't create the bucket again" "$(count create-bucket "$work/calls")" = 1

: > "$key"
printf 'stale\n' > "$conf/.removed.properties.tmp"
run
expect "regenerates an empty key" "$(base64 -d < "$key" | wc -c | tr -d ' ')" = 32
expect "replaces the empty key with a new one" "$(cat "$key")" != "$first_key"
expect "doesn't put a stale temporary file in place" ! -e "$conf/removed.properties"

[ "$failures" -eq 0 ]
