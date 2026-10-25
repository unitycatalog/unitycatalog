#!/usr/bin/env bash
#
# Fails when HEAD edits, deletes or renames a schema migration step that exists on <base-ref>.
# Databases already ran those steps, so a change to the schema must be a new step.
# Also fails on any new step when <base-branch> is a release branch (branch-*): the step's number
# could clash with a different step on main, and the next upgrade would break.
#
# Usage: dev/check-migration-steps.sh <base-ref> [<base-branch>]
# Needs the history back to the merge base of <base-ref> and HEAD (CI passes HEAD^1, the merge
# commit's first parent, so fetch-depth: 2 is enough). CI passes the pull request's base branch
# name as <base-branch>.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <base-ref> [<base-branch>]" >&2
  exit 2
fi
base_branch=${2:-}

steps_dir=server/src/main/resources/db/migration

if ! merge_base=$(git merge-base "$1" HEAD); then
  echo "No merge base between $1 and HEAD; fetch more history." >&2
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

added=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- "$steps_dir")
if [[ -n "$added" && "$base_branch" == branch-* ]]; then
  echo "These migration steps are new on the release branch $base_branch:" >&2
  echo "$added" >&2
  echo "Release branches take no new steps; add the step on main." >&2
  exit 1
fi
