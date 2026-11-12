#!/usr/bin/env bash
# RFC-0054 A4: delete the head count from this checkout. CI then builds and tests what is left, so
# "remove it and a self-hoster loses nothing" is a job, not a paragraph. Destructive: run it in CI
# or a throwaway worktree, never in a tree with work in it.
set -euo pipefail
cd "$(dirname "$0")/.."

# Every hook outside src/count.rs ends in this marker; finding fewer means one was reformatted onto
# two lines or renamed, and deleting the rest would leave a half-hook that hides the fault.
marker='// RFC-0054 hook'
expected=9

rm src/count.rs
hooks=$(grep -rlF -- "$marker" src tests || true)
found=$(grep -rhF -- "$marker" src tests | wc -l | tr -d ' ')
if [ "$found" != "$expected" ]; then
  echo "delete-count: expected $expected hook lines, found $found:" >&2
  grep -rnF -- "$marker" src tests >&2 || true
  exit 1
fi
for f in $hooks; do
  sed -i.bak "\\|$marker|d" "$f"
  rm "$f.bak"
done

left=$(grep -rnE '(^|[^A-Za-z0-9_])count::|mod count;|CountArgs' src tests || true)
if [ -n "$left" ]; then
  echo "delete-count: references survive the deletion:" >&2
  echo "$left" >&2
  exit 1
fi
echo "delete-count: removed src/count.rs and $found hook lines from: $(echo $hooks | tr '\n' ' ')"
