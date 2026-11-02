#!/bin/bash
# record-receipts.sh BLOCKS OUT: eth_getBlockReceipts for each block number in the file BLOCKS (one a
# line), as OUT.gz for fixture-rpc's `recorded --receipts` (RFC-0081 S4: the module hashes a block's
# receipts to its header's receiptsRoot). Each receipt keeps what the root covers, less its logsBloom,
# which is its logs' bloom and recomputed by whoever needs it; a transaction hash is kept only on a
# receipt that holds a log of CONTRACT. Keyless public endpoint; nothing here is a secret.
set -euo pipefail
blocks=$1; out=$2
H=${H:-https://ethereum-rpc.publicnode.com}
C=${CONTRACT:-0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a}
tmp=$(mktemp -d)
while read -r b; do
  [ -n "$b" ] || continue
  hexb=$(printf '0x%x' "$b")
  for try in 1 2 3 4 5 6; do
    curl -s --compressed -m 60 -X POST -H 'content-type: application/json' \
      --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockReceipts\",\"params\":[\"$hexb\"]}" $H > "$tmp/answer.json" || true
    if jq -e '.result | type == "array" and length > 0' "$tmp/answer.json" >/dev/null 2>&1; then break; fi
    sleep 3
  done
  jq -e '.result | length > 0' "$tmp/answer.json" >/dev/null
  jq -c --arg b "$b" --arg c "$C" '{($b): [.result[] | {type, status, cumulativeGasUsed}
      + (if any(.logs[]; (.address | ascii_downcase) == $c) then {transactionHash} else {} end)
      + {logs: [.logs[] | [.address, .topics, .data]]}]}' "$tmp/answer.json" > "$tmp/r$b.json"
  sleep 0.1
done < "$blocks"
jq -s -c 'add' "$tmp"/r*.json > "$out"
rm -rf "$tmp"
jq '{blocks: length}' "$out"
gzip -9nf "$out"
