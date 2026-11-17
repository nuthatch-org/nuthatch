#!/usr/bin/env bash
# usage: run-graph.sh <graph dir holding docker-compose.yml and weth/>
set -uo pipefail
dir=$1
export DOCKER_HOST=${DOCKER_HOST:-unix://$HOME/.colima/default/docker.sock}
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.2f", b - a }'; }
gql() { curl -s -m 5 -H 'content-type: application/json' --data "{\"query\":\"$1\"}" http://localhost:8000/subgraphs/name/weth; }
cd "$dir"
rm -rf data
t0=$(now)
docker compose -p ttfq-graph up -d > up.log 2>&1
until curl -s -o /dev/null http://localhost:8020/; do sleep 0.2; done
echo "graph-node admin port up $(since "$t0") s"
cd weth
t1=$(now)
npx graph codegen > ../codegen.log 2>&1 && npx graph build > ../build.log 2>&1
echo "codegen+build $(since "$t1") s"
until npx graph create --node http://localhost:8020/ weth > ../create.log 2>&1; do sleep 1; done
npx graph deploy --node http://localhost:8020/ --ipfs http://localhost:5001 weth --version-label v0.0.1 > ../deploy.log 2>&1 || { echo "deploy failed"; cat ../deploy.log; exit 1; }
echo "deployed $(since "$t0") s after compose up"
t2=$(now)
first=""
while :; do
  if [ -z "$first" ]; then
    r=$(gql '{ transfers(first: 1) { id } }')
    if echo "$r" | grep -q '"id"'; then first=$(since "$t0"); echo "first query $first s after compose up ($(since "$t2") s after deploy)"; fi
  fi
  m=$(gql '{ _meta { block { number } hasIndexingErrors } }')
  echo "$m" >> ../meta.log
  b=$(echo "$m" | jq -r '.data._meta.block.number // 0' 2>/dev/null || echo 0)
  [ "${b:-0}" -ge 26400999 ] && break
  echo "$m" | grep -q '"hasIndexingErrors":true' && { echo "indexing error"; break; }
  sleep 0.5
done
echo "range indexed $(since "$t0") s after compose up ($(since "$t2") s after deploy)"
for t in transfer approval deposit withdrawal; do
  printf '%s=' $t
  docker compose -p ttfq-graph exec -T postgres psql -U graph-node -d graph-node -tAc \
    "select count(*) from sgd1.$t where block_number between 26400000 and 26400999"
done
