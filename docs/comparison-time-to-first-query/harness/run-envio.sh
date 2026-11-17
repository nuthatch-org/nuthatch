#!/usr/bin/env bash
# usage: run-envio.sh <project dir> <log>
set -uo pipefail
dir=$1; log=$2
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.2f", b - a }'; }
gql() { curl -s -m 5 -H 'content-type: application/json' -H 'x-hasura-admin-secret: testing' --data "{\"query\":\"$1\"}" http://localhost:8080/v1/graphql; }
cd "$dir"
rm -rf .envio/cache
t1=$(now)
pnpm dev > "$log" 2>&1 &
pid=$!
first=""
while :; do
  kill -0 $pid 2>/dev/null || { echo "envio dev exited"; break; }
  if [ -z "$first" ]; then
    r=$(gql '{ WETH_Transfer(limit: 1) { id } }' 2>/dev/null)
    if echo "$r" | grep -q '"id"'; then first=$(since "$t1"); echo "first query $first s"; fi
  fi
  m=$(gql '{ _meta { chainId progressBlock isReady } }' 2>/dev/null)
  echo "$m" >> "$log.meta"
  if echo "$m" | grep -q '"isReady" *: *true'; then break; fi
  sleep 0.2
done
echo "ready $(since "$t1") s"
gql '{ _meta { chainId progressBlock isReady eventsProcessed } t: WETH_Transfer_aggregate { aggregate { count } } a: WETH_Approval_aggregate { aggregate { count } } d: WETH_Deposit_aggregate { aggregate { count } } w: WETH_Withdrawal_aggregate { aggregate { count } } }'; echo
kill $pid; sleep 2
npx envio stop > /dev/null 2>&1
