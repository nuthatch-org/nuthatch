#!/usr/bin/env bash
# Merge one day's answers (day.sh's out-dir) into facts.json.
# Usage: facts.sh <day-dir> <nest-name> <metadata.json> <token-address|-> [grt-bounds.tsv]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
dir=$1 nest=$2 meta=$3 token=$4 btsv=${5:-}

shas=$(for f in "$dir"/*.sql; do
  printf '%s\t%s\n' "$(basename "$f" .sql)" "$(shasum -a 256 "$f" | cut -d' ' -f1)"
done | jq -R -s 'split("\n") | map(select(. != "") | split("\t") | {key: .[0], value: .[1]}) | from_entries')
if [ -n "$btsv" ]; then
  hb=$(awk '{ print $1, $2 }' "$btsv" | jq -R -s 'split("\n") | map(select(. != "") | split(" ") | map(tonumber))')
else
  hb=null
fi
for f in "$dir"/*.json; do
  case $(basename "$f") in window.json | facts.json) continue ;; esac
  jq -c --arg id "$(basename "$f" .json)" '{id: $id, rows}' "$f"
done | jq -s -S --argjson w "$(cat "$dir/window.json")" --arg nest "$nest" --argjson meta "$(cat "$meta")" \
  --arg token "$token" --argjson shas "$shas" --argjson hb "$hb" -f "$here/facts.jq" >"$dir/facts.json"
