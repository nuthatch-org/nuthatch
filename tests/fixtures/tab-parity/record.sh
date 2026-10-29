#!/bin/bash
# record.sh ADDR FROM TO OUT: every header's hash and timestamp in [FROM, TO] and ADDR's logs, as
# OUT.gz for fixture-rpc's `recorded` chain. Keyless public endpoints; nothing here is a secret.
set -euo pipefail
a=$1; from=$2; to=$3; out=$4
H=https://rpc.mevblocker.io
L=https://mainnet.gateway.tenderly.co
tmp=$(mktemp -d)
b=$from
while [ $b -le $to ]; do
  e=$((b + 99)); [ $e -gt $to ] && e=$to
  body=$(seq -f '%.0f' $b $e | awk '{printf "%s{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x%x\",false]}", (NR>1?",":""), $1, $1}')
  for try in 1 2 3 4 5; do
    curl -s -m 60 -X POST -H 'content-type: application/json' --data "[$body]" $H > "$tmp/h$b.json"
    if jq -e "type==\"array\" and length==$((e - b + 1)) and all(.[]; .result.hash != null)" "$tmp/h$b.json" >/dev/null 2>&1; then break; fi
    sleep 3
  done
  jq -e "length==$((e - b + 1))" "$tmp/h$b.json" >/dev/null
  b=$((e + 1)); sleep 0.3
done
jq -s '[.[][] | .result | [.number, .hash, .timestamp]]' "$tmp"/h*.json > "$tmp/headers.json"
b=$from
while [ $b -le $to ]; do
  e=$((b + 999)); [ $e -gt $to ] && e=$to
  for try in 1 2 3 4 5; do
    curl -s -m 60 -X POST -H 'content-type: application/json' --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getLogs\",\"params\":[{\"address\":\"$a\",\"fromBlock\":\"$(printf 0x%x $b)\",\"toBlock\":\"$(printf 0x%x $e)\"}]}" $L > "$tmp/l$b.json"
    if jq -e '.result | type=="array"' "$tmp/l$b.json" >/dev/null 2>&1; then break; fi
    sleep 3
  done
  jq -e '.result | type=="array"' "$tmp/l$b.json" >/dev/null
  b=$((e + 1)); sleep 0.3
done
jq -s '[.[] | .result[]]' "$tmp"/l*.json > "$tmp/logs.json"
jq -n --slurpfile h "$tmp/headers.json" --slurpfile l "$tmp/logs.json" --arg tip "$to" --arg addr "$a" '
  { contract: $addr, tip: ($tip | tonumber),
    headers: $h[0],
    logs: $l[0] }' > "$out"
rm -rf "$tmp"
jq '{tip, headers: (.headers | length), logs: (.logs | length)}' "$out"
gzip -9nf "$out"
