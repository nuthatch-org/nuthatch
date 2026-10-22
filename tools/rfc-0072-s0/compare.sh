#!/usr/bin/env bash
# Gate 1: every statement's response body from two passes, byte for byte, and every facts.json.
# Each pass serves each day from a fresh process, so no answer is /sql's memo of the other.
# Usage: compare.sh <pass-a-root> <pass-b-root>   prints the differing files and a count
set -euo pipefail
a=$1 b=$2
n=0 same=0
for f in "$a"/*/*.json "$a"/*/*.sql; do
  rel=${f#"$a"/}
  n=$((n + 1))
  if cmp -s "$f" "$b/$rel"; then same=$((same + 1)); else echo "differs: $rel"; fi
done
[ "$(cd "$a" && ls */* | grep -v -e timings -e '\.samples$' | sort)" = "$(cd "$b" && ls */* | grep -v -e timings -e '\.samples$' | sort)" ] ||
  echo "the two passes wrote different sets of files"
echo "$n files, $same identical, $((n - same)) differing"
