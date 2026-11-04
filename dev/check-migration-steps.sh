#!/usr/bin/env bash
#
# Fails when HEAD edits, deletes or renames a schema migration step, or a step's effect file, that
# exists on <base-ref>. Databases already ran those steps, and the effect files tell
# --upgrade-schema what they did, so a change to the schema must be a new step.
# Fails when a new step's version is not above every step on <base-ref> for that database:
# databases already past it would never run it (Flyway refuses such a step).
# Also fails on any new step when <base-branch> is a release branch (branch-*): the step's number
# could clash with a different step on main, and the next upgrade would break.
#
# Usage: dev/check-migration-steps.sh <base-ref> [<base-branch>]
# Needs the history back to the merge base of <base-ref> and HEAD (CI passes HEAD^1, the merge
# commit's first parent, so fetch-depth: 2 is enough). CI passes the pull request's base branch
# name as <base-branch>.

set -euo pipefail
# The paths below are relative to the repository root.
cd "$(git rev-parse --show-toplevel)"

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <base-ref> [<base-branch>]" >&2
  exit 2
fi
base_branch=${2:-}

steps_dir=server/src/main/resources/db/migration
effects_dir=server/src/main/resources/schema-effects

if ! merge_base=$(git merge-base "$1" HEAD); then
  echo "No merge base between $1 and HEAD; fetch more history." >&2
  exit 2
fi

changed=$(git diff --diff-filter=MDRT --name-status "$merge_base" HEAD -- "$steps_dir" "$effects_dir")
if [[ -n "$changed" ]]; then
  echo "These merged migration steps or effect files changed since $1:" >&2
  echo "$changed" >&2
  echo "Merged steps can't change; add a new step under $steps_dir/<db>/ instead." >&2
  exit 1
fi
echo "No merged migration step or effect file changed since $1."

# The version of a step file name V<version>__<description>.sql.
version_of() { basename "$1" | sed -n 's/^V\([0-9]*\)__.*/\1/p'; }

added_steps=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- "$steps_dir")
for step in $added_steps; do
  folder=$(dirname "$step")
  highest=$(git ls-tree --name-only "$merge_base" -- "$folder/" \
    | while read -r f; do version_of "$f"; done | sort -n | tail -n 1)
  version=$(version_of "$step")
  if [[ -n "$highest" && ( -z "$version" || "$version" -le "$highest" ) ]]; then
    echo "The new step $step is not above step $highest, the highest on $1." >&2
    echo "Renumber it above every merged step." >&2
    exit 1
  fi
done

added=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- "$steps_dir" "$effects_dir")
if [[ -n "$added" && "$base_branch" == branch-* ]]; then
  echo "These migration steps are new on the release branch $base_branch:" >&2
  echo "$added" >&2
  echo "Release branches take no new steps; add the step on main." >&2
  exit 1
fi
