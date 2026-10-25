#!/usr/bin/env bash
#
# Fails when HEAD edits, deletes or renames a schema migration step that exists on <base-ref>.
# Databases already ran those steps, so a change to the schema must be a new step.
# Also fails when HEAD adds a step numbered at or below the highest step a database runs on
# <base-ref> (its SQL folder and the Java steps): a database that already ran that highest step
# would refuse the new one below it.
# And when <base-branch> is a release branch (branch-*), it fails on any new step: the step's
# number could clash with a different step on main, and the next upgrade would break.
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

sql_steps=server/src/main/resources/db/migration
java_steps=server/src/main/java/io/unitycatalog/server/migrate/steps

if ! merge_base=$(git merge-base "$1" HEAD); then
  echo "No merge base between $1 and HEAD; fetch more history." >&2
  exit 2
fi

# The package's package-info.java is documentation, not a step.
changed=$(git diff --diff-filter=MDR --name-status "$merge_base" HEAD -- \
  "$sql_steps" "$java_steps" ":(exclude)$java_steps/package-info.java")
if [[ -n "$changed" ]]; then
  echo "These merged migration steps changed since $1:" >&2
  echo "$changed" >&2
  echo "Merged steps can't change; add a new step under $sql_steps/<db>/ or $java_steps/ instead." >&2
  exit 1
fi
echo "No merged migration step changed since $1."

added=$(git diff --diff-filter=A --name-only "$merge_base" HEAD -- \
  "$sql_steps" "$java_steps" ":(exclude)$java_steps/package-info.java")
if [[ -z "$added" ]]; then
  exit 0
fi

if [[ "$base_branch" == branch-* ]]; then
  echo "These migration steps are new on the release branch $base_branch:" >&2
  echo "$added" >&2
  echo "Release branches take no new steps; add the step on main." >&2
  exit 1
fi

# The version of a step file: "V1_1__name.sql" is 1.1.
version() {
  local name=${1##*/}
  name=${name#V}
  echo "${name%%__*}" | tr _ .
}

# Whether version $1 is above version $2. Flyway compares the parts numerically, a missing
# part counting as 0: 10 is above 9, and 2 equals 2.0.
above() {
  local -a a b
  IFS=. read -ra a <<<"$1"
  IFS=. read -ra b <<<"$2"
  local i x y
  for ((i = 0; i < ${#a[@]} || i < ${#b[@]}; i++)); do
    x=$((10#${a[i]:-0}))
    y=$((10#${b[i]:-0}))
    if ((x != y)); then
      ((x > y))
      return
    fi
  done
  return 1
}

# The highest step version at ref $1 under the paths that follow, or empty when they have none.
base_max() {
  local ref=$1 max="" file v
  shift
  for file in $(git ls-tree -r --name-only "$ref" -- "$@"); do
    if [[ "${file##*/}" != V*__* ]]; then
      continue
    fi
    v=$(version "$file")
    if [[ -z "$max" ]] || above "$v" "$max"; then
      max=$v
    fi
  done
  echo "$max"
}

# A database runs its SQL folder and the Java steps, so a SQL step must be above both, and a
# Java step above every folder.
failed=0
for file in $added; do
  # Checks (checks/<db>/C*.sql) are not steps; the rule above keeps them from changing.
  [[ ${file##*/} == V* ]] || continue
  v=$(version "$file")
  if [[ "$file" == "$java_steps"/* ]]; then
    max=$(base_max "$1" "$sql_steps" "$java_steps")
    scope="of any database"
  else
    max=$(base_max "$1" "$(dirname "$file")" "$java_steps")
    scope="in its folder and the Java steps"
  fi
  if [[ -n "$max" ]] && ! above "$v" "$max"; then
    echo "$file is version $v, not above $max, the highest step $scope on $1." >&2
    echo "Renumber it above $max." >&2
    failed=1
  fi
done
if ((failed)); then
  exit 1
fi
echo "Every new migration step is numbered above the steps on $1."
