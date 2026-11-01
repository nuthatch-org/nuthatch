#!/usr/bin/env bash
# RFC-0080 S0: N shapes on one follower at tip load on the multinest (Uniswap V4) fixture.
#
#   BIN=target/release/nuthatch SHAPES=10000 bash tools/rfc-0080-s0/run.sh <out-dir>
#
# A writer backfills 200 blocks of the point-read gate's chain (one contract, the ten-event V4
# `PoolManager` ABI, 200 logs a block), then follows TIP_BLOCKS more one block a poll. A follower
# started with NUTHATCH_SHAPES_S0 filters every delta for SHAPES shapes over the swap table, and a
# Node client holds a live copy of every one of them. At the end the copies are checked against the
# writer's own /sql, and the follower's per-delta costs are summarised. SHAPES=0 is the baseline:
# the same follower with the hook on and nothing to filter for.
set -euo pipefail

OUT="${1:?usage: run.sh <out-dir>}"
BIN="${BIN:-target/release/nuthatch}"
SHAPES="${SHAPES:-10000}"
MODE="${MODE:-uniform}"
TIP=20000
BACKFILL=200
TIP_BLOCKS="${TIP_BLOCKS:-300}"
POLL="${POLL:-1s}"
FINAL=$((TIP + TIP_BLOCKS))
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
rm -rf "$OUT/rt" "$OUT"/ready "$OUT"/done "$OUT"/copies.json

free_port() { node -e 'const s=require("net").createServer();s.listen(0,()=>{console.log(s.address().port);s.close()})'; }
RPC_PORT=$(free_port); W_PORT=$(free_port); F_PORT=$(free_port); S_PORT=$(free_port)
pids=""
trap 'for p in $pids; do kill "$p" 2>/dev/null || true; done' EXIT

RPC_BIN="$(bash "$ROOT/scripts/fixture-rpc.sh")"
"$RPC_BIN" multinest --port "$RPC_PORT" --abi "$ROOT/.github/workflows/multinest-abi.json" \
  --contracts 1 --logs-per-block 200 --initial-tip "$TIP" --final-tip "$FINAL" --tip-step 1 \
  >"$OUT/rpc.log" 2>&1 &
pids="$pids $!"
for _ in $(seq 1 40); do
  curl -fsS -m 2 -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
    "127.0.0.1:$RPC_PORT" >/dev/null 2>&1 && break
  sleep 0.25
done

RT="$OUT/rt"
mkdir -p "$RT/nests/s0/abis"
cp "$ROOT/.github/workflows/multinest-abi.json" "$RT/nests/s0/abis/pool_manager.json"
cat >"$RT/nests/s0/nuthatch.toml" <<TOML
[nest]
name = "s0"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "pool_manager"
address = "$(printf '0x%040x' 1)"
abi = "abis/pool_manager.json"
TOML
cat >"$RT/roost.toml" <<TOML
[roost]
name = "s0"
nests = ["s0"]
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
TOML
"$BIN" migrate --dir "$RT" >"$OUT/migrate.log" 2>&1

NUTHATCH_ADMIN_TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export NUTHATCH_ADMIN_TOKEN
"$BIN" dev --dir "$RT" --listen "127.0.0.1:$W_PORT" --backfill "$BACKFILL" --poll-interval "$POLL" \
  >"$OUT/writer.log" 2>&1 &
WPID=$!
pids="$pids $WPID"
last_block() {
  curl -fsS -m 5 -G "127.0.0.1:$W_PORT/s0/sql" --data-urlencode "q=SELECT max(block_number) AS n FROM pool_manager__swap" 2>/dev/null \
    | sed -n 's/.*"rows":\[{"n":\([0-9]*\)}\].*/\1/p'
}
for _ in $(seq 1 600); do
  kill -0 "$WPID" 2>/dev/null || { tail -5 "$OUT/writer.log"; echo "FAIL: the writer exited"; exit 1; }
  b="$(last_block || true)"
  [ -n "$b" ] && [ "$b" -ge "$TIP" ] && break
  sleep 1
done
echo "writer at $(last_block) (backfill to $TIP done)"

node "$HERE/shapes.mjs" "$SHAPES" "$MODE" >"$OUT/shapes.json"
NUTHATCH_SHAPES_S0="$OUT/shapes.json" NUTHATCH_SHAPES_S0_LISTEN="127.0.0.1:$S_PORT" \
  "$BIN" serve --runtime "$RT" --follow "http://127.0.0.1:$W_PORT" --listen "127.0.0.1:$F_PORT" \
  >"$OUT/follower.log" 2>&1 &
FPID=$!
pids="$pids $FPID"
DATASET=""
for _ in $(seq 1 120); do
  DATASET="$(curl -fsS -m 2 "127.0.0.1:$S_PORT/datasets" 2>/dev/null \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s).find(x=>x.ready);if(d)console.log(d.dataset)})' || true)"
  [ -n "$DATASET" ] && break
  sleep 0.5
done
[ -n "$DATASET" ] || { tail -20 "$OUT/follower.log"; echo "FAIL: the follower never held a copy"; exit 1; }
echo "follower holds $DATASET; RSS before clients: $(ps -o rss= -p "$FPID") KiB"

if [ "$SHAPES" -gt 0 ]; then
  node --max-old-space-size=8192 "$HERE/client.mjs" "http://127.0.0.1:$S_PORT" "$DATASET" \
    "$OUT/shapes.json" 0 "$SHAPES" "$OUT/ready" "$OUT/done" "$OUT/copies.json" \
    >"$OUT/client.log" 2>&1 &
  CPID=$!
  pids="$pids $CPID"
  for _ in $(seq 1 1800); do [ -f "$OUT/ready" ] && break; sleep 0.5; done
  [ -f "$OUT/ready" ] || { tail -20 "$OUT/client.log"; echo "FAIL: clients never attached"; exit 1; }
fi
G0="$(curl -fsS "127.0.0.1:$S_PORT/datasets" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s)[0].generation))')"
echo "attached at generation $G0; following to block $FINAL"

: >"$OUT/rss.tsv"
stable=0
prev=""
for _ in $(seq 1 1800); do
  echo "$(date +%s) $(ps -o rss= -p "$FPID")" >>"$OUT/rss.tsv"
  b="$(last_block || true)"
  g="$(curl -fsS -m 2 "127.0.0.1:$S_PORT/datasets" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s)[0].generation))' || true)"
  if [ -n "$b" ] && [ "$b" -ge "$FINAL" ] && [ "$g" = "$prev" ]; then
    stable=$((stable + 1))
    [ "$stable" -ge 4 ] && break
  else
    stable=0
  fi
  prev="$g"
  sleep 1
done
echo "writer at $(last_block), follower at generation $prev"

if [ "$SHAPES" -gt 0 ]; then
  touch "$OUT/done"
  wait "$CPID"
fi
curl -fsS "127.0.0.1:$S_PORT/stats/$DATASET" >"$OUT/stats.json"
node "$HERE/verify.mjs" "$OUT" "http://127.0.0.1:$W_PORT/s0" "$G0" | tee "$OUT/report.json"
