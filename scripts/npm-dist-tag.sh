#!/usr/bin/env bash
# Print the npm dist-tag <version> publishes under (#3150): `next` for a prerelease, `line-X.Y` for a
# patch below the registry's current `latest`, else `latest`. A registry error other than E404 (package
# not published yet) fails the script rather than reading as "latest is unset" and taking `latest` back.
#
#   scripts/npm-dist-tag.sh 5.23.1
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <version>" >&2
  exit 2
fi
v=$1

case "$v" in
  *-*) echo next; exit 0 ;;
esac

err=$(mktemp)
trap 'rm -f "$err"' EXIT
# NPM is overridable so the test can stand in for the registry.
if now=$("${NPM:-npm}" view nuthatch-indexer dist-tags.latest 2>"$err"); then
  :
elif grep -q 'E404' "$err"; then
  now=
else
  echo "npm view nuthatch-indexer failed, not guessing a dist-tag:" >&2
  cat "$err" >&2
  exit 1
fi

if [ -n "$now" ] && [ "$(printf '%s\n%s\n' "$now" "$v" | sort -V | tail -1)" != "$v" ]; then
  echo "line-${v%.*}"
else
  echo latest
fi
echo "latest is ${now:-unset}" >&2
