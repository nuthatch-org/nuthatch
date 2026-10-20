#!/usr/bin/env bash
#
# Backfill events/sec on a fixed, locally served chain: tracked on every PR, not gated (#1723).
#
# `nuthatch bench backfill --seal-direct --concurrency 4` over blocks 1 to 20,000 of the chain
# `fixture-rpc footprint` serves: one contract, four Transfers a block, 80,000 rows. It is
# RFC-0052 S2's scenario (scripts/publish-throughput-gate.sh) without the mirror, so there is no
# third party and no secret. BATCHES batches of RUNS runs; the bench reports each batch's median,
# and the figure reported is the median of those.
#
# Why there is no floor: the runner's batches agree to 4.2%, but the scenario cannot see the
# regressions a floor would exist for. A 4x decode cost moved it about 1%, and on the 4-core runner
# concurrency 1 reads faster than 4. docs/benchmarks.md has the figures.
#
# COMMIT overrides the report's `commit`. On a pull_request the checkout is GitHub's merge ref, on no
# branch, so `git rev-parse HEAD` records a commit nobody can resolve (#1918); CI passes the PR head.
#
# Env: BIN (target/release/nuthatch), OUT (backfill-throughput-report.json), BATCHES (3), RUNS (15),
#      CONCURRENCY (4), RPC_PORT (8547), COMMIT (unset: keep the checkout's HEAD).
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
OUT="${OUT:-backfill-throughput-report.json}"
BATCHES="${BATCHES:-3}"
RUNS="${RUNS:-15}"
CONCURRENCY="${CONCURRENCY:-4}"
RPC_PORT="${RPC_PORT:-8547}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TO_BLOCK=20000
EXPECT=$(( TO_BLOCK * 4 ))
LABEL="backfill throughput: blocks 1-$TO_BLOCK, 4 Transfers a block, seal-direct, concurrency $CONCURRENCY, locally-served chain"
WORK="$(mktemp -d)"

RPC_BIN="$(bash "$HERE/../../scripts/fixture-rpc.sh")"
"$RPC_BIN" footprint "$RPC_PORT" &
RPC_PID=$!
trap 'kill "$RPC_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  curl -fsS -m 2 -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
    "127.0.0.1:$RPC_PORT" >/dev/null 2>&1 && break
  sleep 0.25
done

mkdir -p "$WORK/nest/abis"
cat > "$WORK/nest/nuthatch.toml" <<TOML
[nest]
name = "backfill-throughput"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/usdc.json"
TOML
cat > "$WORK/nest/abis/usdc.json" <<'JSON'
[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
  {"name":"from","type":"address","indexed":true},
  {"name":"to","type":"address","indexed":true},
  {"name":"value","type":"uint256","indexed":false}]}]
JSON

medians=()
for b in $(seq 1 "$BATCHES"); do
  "$BIN" bench backfill --dir "$WORK/nest" --from 1 --to "$TO_BLOCK" --runs "$RUNS" \
    --seal-direct --concurrency "$CONCURRENCY" --label "$LABEL" --out "$WORK/batch-$b.json" \
    > "$WORK/batch-$b.log" 2>&1 || { tail -30 "$WORK/batch-$b.log" >&2; echo "FAIL: batch $b did not finish"; exit 1; }
  events="$(jq -r '.events' "$WORK/batch-$b.json")"
  if [ "$events" != "$EXPECT" ]; then
    tail -30 "$WORK/batch-$b.log" >&2
    echo "FAIL: batch $b decoded $events events, not $EXPECT. A rate over the wrong amount of work measures nothing."
    exit 1
  fi
  m="$(jq -r '.events_per_sec' "$WORK/batch-$b.json")"
  echo "batch $b: median $m ev/s over $RUNS runs, $(jq -r '.peak_rss_mb' "$WORK/batch-$b.json") MB peak RSS"
  medians+=("$m")
done

sorted="$(printf '%s\n' "${medians[@]}" | sort -g)"
median="$(echo "$sorted" | awk '{a[NR]=$1} END {print (NR%2) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2}')"
lo="$(echo "$sorted" | head -1)"
hi="$(echo "$sorted" | tail -1)"
spread="$(awk -v lo="$lo" -v hi="$hi" -v m="$median" 'BEGIN {printf "%.1f", (hi-lo)*100/m}')"
jq --argjson median "$median" --argjson batches "$(printf '%s\n' "${medians[@]}" | jq -s .)" \
  --arg commit "${COMMIT:-}" \
  '.events_per_sec = $median | .batch_medians = $batches | .runs = (.runs * ($batches | length))
   | if $commit != "" then .commit = $commit[0:7] else . end' \
  "$WORK/batch-1.json" > "$OUT"

line="median ${median} ev/s across $BATCHES batches of $RUNS (batch medians ${lo}-${hi}, spread ${spread}%), tracked, not gated"
echo "backfill: $line"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "### backfill throughput"; echo "$line"; } >> "$GITHUB_STEP_SUMMARY"
fi
