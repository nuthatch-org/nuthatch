#!/usr/bin/env bash
# RFC-0080 §20: one run of S0's cost gate against the shipped `/sync`.
#
#   BIN=target/release/nuthatch tools/rfc-0080-s0/run.sh <mix> [shapes] [deltas]
#
# The point-read gate's chain (`fixture-rpc multinest`, the ten-event Uniswap V4 `PoolManager` ABI,
# 200 logs a block), a writer runtime publishing its feed with `[mounts.sync]` on the mount, and a
# follower (`serve --runtime --follow`). <shapes> live streams (default 10,000) attach, then the gate
# is read over the next <deltas> deltas (default 300): of the follower's apply
# (`nuthatch_follower_apply_seconds`), the share spent on its `/sync` subscribers
# (`nuthatch_follower_sync_seconds`), against the rest of the apply. Prints one `result` line.
set -euo pipefail

MIX=${1:?mix: uniform | hot:<k> | all-tables}
SHAPES=${2:-10000}
DELTAS=${3:-300}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
BIN=${BIN:-$ROOT/target/release/nuthatch}
RPC_BIN=${RPC_BIN:-$(bash "$ROOT/scripts/fixture-rpc.sh")}
ABI="$ROOT/.github/workflows/multinest-abi.json"
OUT=$(mktemp -d)
ulimit -n 20000 2>/dev/null || ulimit -n 10240

free_port() { node -e 'const s=require("net").createServer().listen(0,()=>{console.log(s.address().port);s.close()})'; }
say() { printf '%s\n' "$*" >&2; }
pids=""
cleanup() { for p in $pids; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

TIP=20000
rpc_port=$(free_port)
"$RPC_BIN" multinest --port "$rpc_port" --abi "$ABI" --contracts 1 --logs-per-block 200 \
  --initial-tip "$TIP" --final-tip $((TIP + 20000)) --tip-step 1 >"$OUT/rpc.log" 2>&1 &
pids="$pids $!"

rt=$OUT/runtime
mkdir -p "$rt/nests/s0/abis"
cp "$ABI" "$rt/nests/s0/abis/pool_manager.json"
cat >"$rt/nests/s0/nuthatch.toml" <<TOML
[nest]
name = "s0"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$rpc_port"]
schema_version = 1

[[contracts]]
alias = "pool_manager"
address = "$(printf '0x%040x' 1)"
abi = "abis/pool_manager.json"
start_block = $((TIP - 200))
TOML
cat >"$rt/roost.toml" <<TOML
[roost]
name = "s0"
nests = ["s0"]
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$rpc_port"]
TOML
"$BIN" migrate --dir "$rt" >"$OUT/migrate.log" 2>&1

NUTHATCH_ADMIN_TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export NUTHATCH_ADMIN_TOKEN
w_port=$(free_port)
w=http://127.0.0.1:$w_port
json() { curl -fsS -m 30 "$@"; }

# `[mounts.sync]` on the one mount: every table, with the columns the mixes name.
others="approval donate initialize modify_liquidity operator_set ownership_transferred \
protocol_fee_controller_updated protocol_fee_updated transfer"
{
  echo
  echo "[mounts.sync]"
  printf 'tables = { pool_manager__swap = ["sender", "id", "address", "tx_hash"]'
  for t in $others; do printf ', pool_manager__%s = ["tx_hash"]' "$t"; done
  echo ' }'
  echo
  echo "[runtime.publish]"
  echo "target = \"$OUT/mirror\""
  echo 'interval = "1s"'
  echo 'followers = true'
} >>"$rt/mounts.toml"

node "$HERE/shapes.mjs" "$SHAPES" "$MIX" >"$OUT/shapes.jsonl"

"$BIN" dev --dir "$rt" --listen "127.0.0.1:$w_port" --poll-interval 1s >"$OUT/writer.log" 2>&1 &
pids="$pids $!"
f_port=$(free_port)
f=http://127.0.0.1:$f_port
"$BIN" serve --runtime "$rt" --follow "$w" --listen "127.0.0.1:$f_port" >"$OUT/follower.log" 2>&1 &
fpid=$!
pids="$pids $fpid"
for _ in $(seq 1 300); do
  r=$(json "$f/s0/ready" 2>/dev/null || true)
  printf '%s' "$r" | grep -q '"writer":"connected"' && break
  sleep 1
done
printf '%s' "$r" | grep -q '"writer":"connected"' || { say "the follower never connected: $(tail -5 "$OUT/follower.log")"; exit 2; }

metric() { # <name> <sum|count>
  json "$f/metrics" | awk -v n="$1_$2" '$1==n {print $2}'
}

node "$HERE/client.mjs" "$f" s0 "$OUT/shapes.jsonl" >"$OUT/client.out" 2>"$OUT/client.err" &
cpid=$!
pids="$pids $cpid"
attach_started=$(date +%s)
until grep -q '^attached' "$OUT/client.out"; do
  kill -0 "$cpid" 2>/dev/null || { say "the client died: $(cat "$OUT/client.err")"; exit 2; }
  [ $(( $(date +%s) - attach_started )) -lt 1800 ] || { say "attach did not finish in 30 min"; exit 2; }
  sleep 2
done
attached=$(sed -n 's/^attached //p' "$OUT/client.out")
attach_s=$(( $(date +%s) - attach_started ))
# The window opens with the follower at the writer's tip, so the attach's backlog is not in it.
for _ in $(seq 1 120); do
  json "$f/s0/ready" 2>/dev/null | grep -q '"lag_generations":0' && break
  sleep 1
done

a0=$(metric nuthatch_follower_apply_seconds sum); c0=$(metric nuthatch_follower_apply_seconds count)
s0=$(metric nuthatch_follower_sync_seconds sum); n0=$(metric nuthatch_follower_sync_seconds count)
rss=()
until [ $(( $(metric nuthatch_follower_apply_seconds count) - c0 )) -ge "$DELTAS" ]; do
  rss+=("$(ps -o rss= -p "$fpid" | tr -d ' ')")
  sleep 2
done
a1=$(metric nuthatch_follower_apply_seconds sum); c1=$(metric nuthatch_follower_apply_seconds count)
s1=$(metric nuthatch_follower_sync_seconds sum); n1=$(metric nuthatch_follower_sync_seconds count)
kill -TERM "$cpid"; sleep 1
client=$(tail -1 "$OUT/client.out")
rss_med=$(printf '%s\n' "${rss[@]}" | sort -n | awk '{a[NR]=$1} END {print int(a[int((NR+1)/2)]/1024)}')
load=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || cut -d' ' -f1 /proc/loadavg)

node -e '
const [mix, shapes, attached, attach_s, a0, a1, c0, c1, s0, s1, n0, n1, rss, client, load] = process.argv.slice(1);
const apply = a1 - a0, sync = s1 - s0, deltas = c1 - c0, synced = n1 - n0;
const rest = apply - sync;
console.log(`result mix=${mix} shapes=${shapes} attached=${attached} attach_s=${attach_s} deltas=${deltas} ` +
  `synced_deltas=${synced} apply_mean_us=${(apply / deltas * 1e6).toFixed(0)} ` +
  `sync_mean_us=${(sync / Math.max(synced, 1) * 1e6).toFixed(0)} rest_mean_us=${(rest / deltas * 1e6).toFixed(0)} ` +
  `share=${(100 * sync / rest).toFixed(1)}% rss_mib=${rss} load=${load} client="${client}"`);
' "$MIX" "$SHAPES" "$attached" "$attach_s" "$a0" "$a1" "$c0" "$c1" "${s0:-0}" "${s1:-0}" "${n0:-0}" "${n1:-0}" "$rss_med" "$client" "$load"
say "logs in $OUT"
