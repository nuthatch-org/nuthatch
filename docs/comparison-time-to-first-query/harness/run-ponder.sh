#!/usr/bin/env bash
# usage: run-ponder.sh <project dir> <log>
set -uo pipefail
dir=$1; log=$2
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.2f", b - a }'; }
gql() { curl -s -m 5 -H 'content-type: application/json' --data "{\"query\":\"$1\"}" http://localhost:42069/graphql; }
rm -rf "$dir/.ponder"
cd "$dir"
t1=$(now)
PONDER_RPC_URL_1=https://eth.drpc.org npm run dev > "$log" 2>&1 &
pid=$!
first=""
while :; do
  r=$(gql '{ transfers(limit: 1) { items { id } } }' 2>/dev/null)
  if [ -z "$first" ] && echo "$r" | grep -q '"id"'; then first=$(since "$t1"); echo "first query $first s"; fi
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:42069/ready)
  [ "$code" = 200 ] && break
  sleep 0.2
done
echo "ready $(since "$t1") s"
q='{ t: transfers(where: {blockNumber_gte: \"26400000\", blockNumber_lte: \"26400999\"}) { totalCount } a: approvals(where: {blockNumber_gte: \"26400000\", blockNumber_lte: \"26400999\"}) { totalCount } d: deposits(where: {blockNumber_gte: \"26400000\", blockNumber_lte: \"26400999\"}) { totalCount } w: withdrawals(where: {blockNumber_gte: \"26400000\", blockNumber_lte: \"26400999\"}) { totalCount } }'
gql "$q"; echo
kill $pid; sleep 1; pkill -f "ponder dev" || true
