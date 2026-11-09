#!/usr/bin/env bash
# Print `true` if <tag> may move the container image's `:latest`, else `false` (#2736). Reads the
# repository's tags, one per line, on stdin. Only a plain vX.Y.Z that is the highest of them qualifies:
# a prerelease is never `:latest`, and a patch on an older line must not move it back.
#
#   git ls-remote --tags --refs origin 'v*' | sed 's#.*refs/tags/##' | scripts/release-latest.sh v5.18.0
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <tag>  (tags on stdin)" >&2
  exit 2
fi
tag=$1
# Drained first, so an early answer cannot SIGPIPE the `git ls-remote` feeding it.
tags=$(cat)
stable='^v[0-9]+\.[0-9]+\.[0-9]+$'

if ! [[ $tag =~ $stable ]]; then
  echo false
  exit 0
fi
newest=$({ grep -E "$stable" <<<"$tags" || true; echo "$tag"; } | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
if [ "v$newest" = "$tag" ]; then echo true; else echo false; fi
