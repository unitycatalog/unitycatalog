#!/bin/sh
# Runs storage-init.sh against a stand-in bin/uc for each answer the CLI can
# give, and checks which writes it makes, with which arguments, and how it exits.

set -eu

script="$(cd "$(dirname "$0")" && pwd)/storage-init.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin"

# The stand-in answers per STUB_MODE and records each write's arguments in
# STUB_CALLS. The real CLI exits 0 when it rejects its arguments, printing help.
cat > "$work/bin/uc" <<'EOF'
#!/bin/sh
name=
prev=
for arg in "$@"; do
  if [ "$prev" = --name ]; then name=$arg; fi
  prev=$arg
done
json="{\"name\":\"$name\",\"id\":\"1\"}"
help="Error occurred while parsing the command.
Usage: bin/uc <entity> <operation> [options]"
case "$2" in
  list) ;;
  get)
    case "$STUB_MODE" in
      exists | exists-write-help) echo "$json" ;;
      error) echo '503 - {"error_code":"UNAVAILABLE"}'; exit 1 ;;
      get-help) echo "$help" ;;
      *) echo '404 - {"error_code":"NOT_FOUND"}'; exit 1 ;;
    esac ;;
  *)
    echo "$*" >> "$STUB_CALLS"
    case "$STUB_MODE" in
      write-help | exists-write-help) echo "$help" ;;
      write-error) echo '400 - {"error_code":"INVALID_ARGUMENT"}'; exit 1 ;;
      location-write-help) if [ "$1" = external_location ]; then echo "$help"; else echo "$json"; fi ;;
      *) echo "$json" ;;
    esac ;;
esac
EOF
chmod +x "$work/bin/uc"

failures=0

# check <mode> <expected exit status> <expected writes, as "entity operation,">
check() {
  : > "$work/calls"
  status=0
  (cd "$work" && STUB_MODE=$1 STUB_CALLS="$work/calls" sh "$script" > "$work/out" 2>&1) || status=$?
  writes="$(cut -d ' ' -f 1,2 < "$work/calls" | tr '\n' ',')"
  if [ "$status" = "$2" ] && [ "$writes" = "$3" ]; then
    echo "ok   $1"
  else
    echo "FAIL $1: exit $status, writes [$writes]; want exit $2, writes [$3]"
    sed 's/^/     /' "$work/out"
    failures=$((failures + 1))
  fi
}

# passed <description> <arguments one write of the last check must contain>
passed() {
  if grep -q -F -e "$2" "$work/calls"; then
    echo "ok   $1"
  else
    echo "FAIL $1: no write had [$2]; writes were:"
    sed 's/^/     /' "$work/calls"
    failures=$((failures + 1))
  fi
}

check exists 0 "credential update,external_location update,"
check missing 0 "credential create,external_location create,"
passed "the credential names RustFS's endpoint" \
  "credential create --name rustfs --s3_endpoint http://rustfs:9000"
passed "the location covers the bucket with the credential" \
  "external_location create --name rustfs_warehouse --url s3://uc-warehouse --credential_name rustfs"
check error 1 ""
check get-help 1 ""
check write-help 1 "credential create,"
check exists-write-help 1 "credential update,"
check write-error 1 "credential create,"
check location-write-help 1 "credential create,external_location create,"

[ "$failures" -eq 0 ]
