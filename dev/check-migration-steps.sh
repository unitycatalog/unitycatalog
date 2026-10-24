#!/usr/bin/env bash
#
# Fails when HEAD edits, deletes or renames a schema migration step that exists on <base-ref>.
# Databases already ran those steps, so a change to the schema must be a new step.
#
# Usage: dev/check-migration-steps.sh <base-ref>
# Needs the history back to the merge base of <base-ref> and HEAD (CI: fetch-depth: 0).

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <base-ref>" >&2
  exit 2
fi

steps_dir=server/src/main/resources/db/migration

if ! merge_base=$(git merge-base "$1" HEAD); then
  echo "No merge base between $1 and HEAD; fetch the full history (fetch-depth: 0)." >&2
  exit 2
fi

changed=$(git diff --diff-filter=MDR --name-status "$merge_base" HEAD -- "$steps_dir")
if [[ -n "$changed" ]]; then
  echo "These merged migration steps changed since $1:" >&2
  echo "$changed" >&2
  echo "Merged steps can't change; add a new step under $steps_dir/<db>/ instead." >&2
  exit 1
fi
echo "No merged migration step changed since $1."
