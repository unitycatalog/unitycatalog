#!/usr/bin/env bash
#
# Fails when HEAD edits, deletes or renames a schema migration step (SQL or Java), the shared Java
# migration helper, or a step's effect file that exists on <base-ref>. Databases already ran those
# steps, and the effect files tell --upgrade-schema what they did, so a change to the schema must
# be a new step.
# Fails when a new step's version (SQL or Java) is not above every step on <base-ref>:
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
java_steps_dir=server/src/main/java/io/unitycatalog/server/migrate
java_step_glob="$java_steps_dir/V*.java"
java_helper="$java_steps_dir/DropRangeCheckMigration.java"

if ! merge_base=$(git merge-base "$1" HEAD); then
  echo "No merge base between $1 and HEAD; fetch more history." >&2
  exit 2
fi

changed=$(git diff --diff-filter=MDRT --name-status "$merge_base" HEAD -- \
  "$steps_dir" "$effects_dir" "$java_step_glob" "$java_helper")
if [[ -n "$changed" ]]; then
  echo "These merged migration steps, helpers, or effect files changed since $1:" >&2
  echo "$changed" >&2
  echo "Merged steps can't change; add a new step instead." >&2
  exit 1
fi
echo "No merged migration step, helper, or effect file changed since $1."

# A release branch takes no new steps at all, so say that before checking their numbers.
added=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- \
  "$steps_dir" "$effects_dir" "$java_step_glob")
if [[ -n "$added" && "$base_branch" == branch-* ]]; then
  echo "These migration steps are new on the release branch $base_branch:" >&2
  echo "$added" >&2
  echo "Release branches take no new steps; add the step on main." >&2
  exit 1
fi

# The version of a step file name V<version>__<description>.(sql|java) or effect file V<version>.txt.
version_of_step() { basename "$1" | sed -n 's/^V\([0-9]*\)__.*/\1/p'; }
version_of_effect() { basename "$1" | sed -n 's/^V\([0-9]*\)\.txt$/\1/p'; }

# Highest step version on <base-ref> across SQL steps, Java steps, and schema-effects.
highest_on_base() {
  {
    git ls-tree -r --name-only "$1" -- "$steps_dir/" "$java_step_glob" \
      | while read -r f; do version_of_step "$f"; done
    git ls-tree -r --name-only "$1" -- "$effects_dir/" \
      | while read -r f; do version_of_effect "$f"; done
  } | awk '/^[0-9]+$/ { print $1 + 0 }' | sort -n | tail -n 1
}

highest=$(highest_on_base "$1")

added_steps=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- \
  "$steps_dir" "$java_step_glob")
for step in $added_steps; do
  version=$(version_of_step "$step")
  if [[ ! "$version" =~ ^[0-9]+$ ]]; then
    echo "The new step $step has a version that is not a whole number." >&2
    echo "Name it V<number>__<description>.sql or V<number>__<description>.java." >&2
    exit 1
  fi
  # 10# keeps bash from reading a leading zero (V08) as octal.
  version=$((10#$version))
  if [[ -n "$highest" && "$version" -le "$highest" ]]; then
    echo "The new step $step is not above step $highest, the highest on $1." >&2
    echo "Renumber it above every merged step." >&2
    exit 1
  fi
done
