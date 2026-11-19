#!/usr/bin/env bash
# Run every example query in configs/<name>.json against a live nest and fail if one errors.
# Usage: showcase/check-examples.sh <nest-url> [config.json]
#   showcase/check-examples.sh https://nuthatch-indexer.com/nests/arc-usdc
set -euo pipefail
url=${1:?usage: check-examples.sh <nest-url> [config.json]}; url=${url%/}
cfg=${2:-$(dirname "$0")/configs/${url##*/}.json}
[ -f "$cfg" ] || { echo "no config $cfg"; exit 2; }
fail=0
while IFS=$'\t' read -r title sqltext; do
  # 3 s apart keeps well under the proxy's 30 a minute per address
  out=$(curl -sG -m 30 -w '\n%{http_code}' "$url/sql" --data-urlencode "q=$sqltext")
  code=${out##*$'\n'}; body=${out%$'\n'*}
  n=$(printf '%s' "$body" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("count","ERR"))' 2>/dev/null || echo ERR)
  if [ "$code" = 200 ] && [ "$n" != ERR ]; then echo "ok   $n rows  $title"; else echo "FAIL $code  $title: ${body:0:200}"; fail=1; fi
  sleep 3
done < <(python3 -I -c 'import json,sys
for x in json.load(open(sys.argv[1]))["queries"]: print(x["title"]+"\t"+x["sql"])' "$cfg")
exit $fail
