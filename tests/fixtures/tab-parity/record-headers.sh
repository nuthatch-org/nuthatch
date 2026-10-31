#!/bin/bash
# record-headers.sh FROM TO OUT: every header in [FROM, TO] whole, as eth_getBlockByNumber gives it
# without its transactions, as OUT.gz for fixture-rpc's `recorded --headers` (RFC-0077 S5: the module
# hashes them up to a verified block). Keyless public endpoint; nothing here is a secret.
set -euo pipefail
from=$1; to=$2; out=$3
H=${H:-https://ethereum-rpc.publicnode.com}
tmp=$(mktemp -d)
b=$from
while [ $b -le $to ]; do
  e=$((b + 49)); [ $e -gt $to ] && e=$to
  body=$(seq -f '%.0f' $b $e | awk '{printf "%s{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x%x\",false]}", (NR>1?",":""), $1, $1}')
  for try in 1 2 3 4 5 6; do
    curl -s -m 60 -X POST -H 'content-type: application/json' --data "[$body]" $H > "$tmp/h$b.json" || true
    if jq -e "type==\"array\" and length==$((e - b + 1)) and all(.[]; .result.hash != null)" "$tmp/h$b.json" >/dev/null 2>&1; then break; fi
    sleep 3
  done
  jq -e "length==$((e - b + 1))" "$tmp/h$b.json" >/dev/null
  b=$((e + 1)); sleep 0.2
done
jq -s -c '[.[][] | .result | del(.transactions, .withdrawals, .uncles, .size, .totalDifficulty)] | sort_by([(.number | ltrimstr("0x") | length), .number])' "$tmp"/h*.json > "$out"
rm -rf "$tmp"
jq '{headers: length, first: .[0].number, last: .[-1].number}' "$out"
gzip -9nf "$out"
