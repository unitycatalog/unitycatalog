#!/bin/sh
# Runs start-server.sh with a stand-in bin/start-uc-server, and checks which
# credential encryption key it passes on and that it stops without one.

set -eu

script="$(cd "$(dirname "$0")" && pwd)/start-server.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/etc/conf"
key_file="$work/etc/conf/credential-encryption.key"

cat > "$work/bin/start-uc-server" <<'EOF'
#!/bin/sh
echo "started with key=${UC_SERVER_CREDENTIAL_ENCRYPTION_KEY:-} args=$*"
EOF
chmod +x "$work/bin/start-uc-server"

failures=0

# check <description> <variable> <key file: "-" for none, "" for empty>
#   <expected exit status> <expected output>
check() {
  rm -f "$key_file"
  if [ "$3" != - ]; then printf '%s' "$3" > "$key_file"; fi
  status=0
  output="$(cd "$work" && UC_SERVER_CREDENTIAL_ENCRYPTION_KEY="$2" sh "$script" --flag 2>&1)" ||
    status=$?
  if [ "$status" = "$4" ] && printf '%s\n' "$output" | grep -q -F "$5"; then
    echo "ok   $1"
  else
    echo "FAIL $1: exit $status, output: $output"
    failures=$((failures + 1))
  fi
}

check "the variable wins over the key file" from-env from-file 0 "started with key=from-env args=--flag"
check "the key file is used when the variable is empty" "" from-file 0 "started with key=from-file args=--flag"
check "a missing key file stops the launcher" "" - 1 "credential-encryption.key is missing"
check "an empty key file stops the launcher" "" "" 1 "credential-encryption.key is missing"

[ "$failures" -eq 0 ]
