#!/usr/bin/env bash
# facts.sh for every day of a pass. Usage: facts-all.sh <pass-root> <nest-name> <metadata.json> <token|-> [grt-bounds.tsv]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$1
shift
for d in "$root"/????-??-??; do "$here/facts.sh" "$d" "$@"; done
