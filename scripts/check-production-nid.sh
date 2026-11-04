#!/usr/bin/env bash
# Whether a live nest directory is exactly graph-allocations-nest's `production` branch, the files
# `verify` checks Lodestar's answers with. Both NIDs come from one binary, so a release that changes
# how a NID is computed cannot raise a false alarm.
#
#   scripts/check-production-nid.sh <nuthatch-binary> <live nest dir> [--override]
#
# Exit 0 when they match (or on --override), 4 when they differ, 5 when either NID cannot be computed.
# PRODUCTION_REPO and PRODUCTION_BRANCH name the branch; the tests point them at a local repository.
set -euo pipefail

bin=${1:?usage: check-production-nid.sh <nuthatch-binary> <live nest dir> [--override]}
live=${2:?usage: check-production-nid.sh <nuthatch-binary> <live nest dir> [--override]}
override=${3:-}
repo=${PRODUCTION_REPO:-https://github.com/nuthatch-org/graph-allocations-nest}
branch=${PRODUCTION_BRANCH:-production}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git clone -q --depth 1 --branch "$branch" "$repo" "$work/nest" \
  || { echo "refusing: could not clone $branch of $repo" >&2; exit 5; }

nid_of() { # <dir>: the NID, or exit 5 naming why
  local out
  out=$("$bin" nest nid --dir "$1" 2>"$work/err") \
    || { echo "refusing: \`nest nid\` failed on $1: $(tail -n 3 "$work/err")" >&2; exit 5; }
  out=$(printf '%s\n' "$out" | tail -n 1)
  printf '%s\n' "$out" | grep -qxE '[0-9a-f]{64}' \
    || { echo "refusing: \`nest nid\` on $1 printed no NID: $out" >&2; exit 5; }
  printf '%s\n' "$out"
}
want=$(nid_of "$work/nest")
have=$(nid_of "$live")

if [ "$want" = "$have" ]; then
  echo "ok   production: $live is $branch of $repo ($have)"
elif [ "$override" = --override ]; then
  echo "*** OVERRIDE: $live computes $have, $branch computes $want ***"
else
  echo "refusing: $live computes NID $have, but $branch of $repo computes $want. Commit the box's files to that branch, or put the branch's files on the box; --override rolls anyway" >&2
  exit 4
fi
