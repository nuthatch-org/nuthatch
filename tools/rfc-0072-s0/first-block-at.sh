#!/usr/bin/env bash
# First block whose timestamp is >= a unix time, by bisection over eth_getBlockByNumber.
# Only for a nest with block_timestamps = false, which has no clock of its own (RFC-0072 §5.6).
# Usage: first-block-at.sh <rpc-url> <unix-time> <lo-block> <hi-block>   prints "<time> <block> <block-time>"
set -euo pipefail
rpc=$1 t=$2 lo=$3 hi=$4

ts() {
  local n
  for _ in 1 2 3 4 5; do
    n=$(curl -sS --max-time 20 -X POST "$rpc" -H 'content-type: application/json' \
      -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$(printf '0x%x' "$1")\",false]}" |
      jq -r '.result.timestamp // empty') && [ -n "$n" ] && { echo $((n)); return; }
    sleep 1
  done
  echo "no timestamp for block $1" >&2
  exit 1
}

# Invariant: ts(lo) < t <= ts(hi).
[ "$(ts "$lo")" -lt "$t" ] || { echo "lo $lo is not before $t" >&2; exit 1; }
[ "$(ts "$hi")" -ge "$t" ] || { echo "hi $hi is not at or after $t" >&2; exit 1; }
while [ $((hi - lo)) -gt 1 ]; do
  mid=$(((lo + hi) / 2))
  if [ "$(ts "$mid")" -ge "$t" ]; then hi=$mid; else lo=$mid; fi
done
echo "$t $hi $(ts "$hi")"
