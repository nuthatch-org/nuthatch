#!/usr/bin/env bash
#
# Tip lag: how long after a block appears at the chain's tip its rows answer through /sql (#1884).
#
# `fixture-rpc footprint --moving` serves its chain, so the tip advances one block, four Transfers,
# each time this script asks. Each sample waits a random 0-999 ms, advances the tip, then polls /sql
# every 10 ms until the new block's four rows are there. Two figures per sample:
#
#   lag         block appears -> rows queryable. What a reader sees; it includes up to one poll
#               interval of waiting for nuthatch to ask for the tip.
#   after seen  nuthatch's first eth_blockNumber that reported the block -> rows queryable. The
#               fetch, decode and commit alone, with the poll wait taken out.
#
# The poll interval is 1 s, the least `--poll-interval` accepts; mainnet's default is 12 s, which
# would make `lag` a measurement of the timer.
#
# Only the after-seen p50 can carry a ceiling. `lag` p50 moves 100 ms between runs on the poll phase,
# and the p99s are preemption; docs/benchmarks.md has the figures. The ceiling is the runner's, so
# it is set in ci.yml and unset here.
#
# Env: BIN (target/release/nuthatch), OUT (tip-lag-report.json), SAMPLES (100), WARMUP (5),
#      PORT (8290), RPC_PORT (8548), MAX_SEEN_P50_MS (unset: recorded, not gated).
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
OUT="${OUT:-tip-lag-report.json}"
SAMPLES="${SAMPLES:-100}"
WARMUP="${WARMUP:-5}"
PORT="${PORT:-8290}"
RPC_PORT="${RPC_PORT:-8548}"
MAX_SEEN_P50_MS="${MAX_SEEN_P50_MS:-}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIP=20000
WORK="$(mktemp -d)"

if [ -z "${EPOCHREALTIME:-}" ]; then
  echo "FAIL: needs bash 5 for EPOCHREALTIME (this is ${BASH_VERSION})"
  exit 1
fi
now_ms() { local t="${EPOCHREALTIME/[.,]/}"; echo "${t:0:13}"; }

RPC_BIN="$(bash "$HERE/../../scripts/fixture-rpc.sh")"
"$RPC_BIN" footprint "$RPC_PORT" --moving &
RPC_PID=$!
DEV_PID=
trap 'kill "$RPC_PID" ${DEV_PID:+"$DEV_PID"} 2>/dev/null || true' EXIT
rpc() {
  curl -fsS -m 5 -X POST -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" "127.0.0.1:$RPC_PORT"
}
for _ in $(seq 1 40); do
  rpc eth_chainId '[]' >/dev/null 2>&1 && break
  sleep 0.25
done

mkdir -p "$WORK/nest/abis"
cat > "$WORK/nest/nuthatch.toml" <<TOML
[nest]
name = "tip-lag"
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

"$BIN" dev --dir "$WORK/nest" --listen "127.0.0.1:$PORT" --backfill 10 --poll-interval 1s \
  > "$WORK/dev.log" 2>&1 &
DEV_PID=$!

rows_at() {
  curl -s -m 5 -G "127.0.0.1:$PORT/sql" \
    --data-urlencode "q=SELECT count(*) n FROM usdc__transfer WHERE block_number = $1" 2>/dev/null \
    | jq -r '.rows[0].n // .[0].n // empty' 2>/dev/null || true
}

# Waits for block $1's four rows, at most 30 s. Prints the time they answered.
wait_rows() {
  local deadline=$(( $(now_ms) + 30000 )) n
  while [ "$(now_ms)" -lt "$deadline" ]; do
    n="$(rows_at "$1")"
    if [ "$n" = "4" ]; then now_ms; return 0; fi
    kill -0 "$DEV_PID" 2>/dev/null || break
    sleep 0.01
  done
  return 1
}

if ! wait_rows "$TIP" > /dev/null; then
  tail -30 "$WORK/dev.log" >&2
  echo "FAIL: the nest never served block $TIP's rows, so there is no tip to measure from."
  exit 1
fi

: > "$WORK/samples.jsonl"
for i in $(seq 1 $(( WARMUP + SAMPLES ))); do
  sleep "0.$(printf '%03d' $(( RANDOM % 1000 )))"
  adv="$(rpc fixture_advance '[]')"
  block="$(echo "$adv" | jq -r '.result.tip')"
  appeared="$(echo "$adv" | jq -r '.result.at | floor')"
  if ! ready="$(wait_rows "$block")"; then
    tail -30 "$WORK/dev.log" >&2
    echo "FAIL: block $block's rows were not queryable 30 s after it appeared."
    exit 1
  fi
  seen="$(rpc fixture_seen "[$block]" | jq -r '.result | floor')"
  if [ "$i" -gt "$WARMUP" ]; then
    echo "{\"block\":$block,\"lag_ms\":$(( ready - appeared )),\"after_seen_ms\":$(( ready - seen ))}" \
      >> "$WORK/samples.jsonl"
  fi
done

hardware="$(nproc 2>/dev/null || sysctl -n hw.ncpu) cores, $(uname -sm)"
commit="${GITHUB_SHA:-$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)}"
jq -s --arg label "tip lag: footprint fixture, moving tip, 4 Transfers a block, poll 1s" \
  --arg hardware "$hardware" --arg version "$("$BIN" --version)" --arg commit "$commit" '
  def stats(k): (map(.[k]) | sort) as $s | ($s | length) as $n |
    {p50: $s[(($n - 1) / 2 | floor)], p99: $s[((($n * 99 + 99) / 100 | floor) - 1)],
     min: $s[0], max: $s[$n - 1]};
  {label: $label, version: $version, commit: $commit, provider: "fixture-rpc footprint --moving, local",
   hardware: $hardware, poll_interval_ms: 1000,
   samples: length, lag_ms: stats("lag_ms"), after_seen_ms: stats("after_seen_ms"), raw: .}
' "$WORK/samples.jsonl" > "$OUT"

read -r p50 p99 sp50 sp99 < <(jq -r '"\(.lag_ms.p50) \(.lag_ms.p99) \(.after_seen_ms.p50) \(.after_seen_ms.p99)"' "$OUT")
gate="${MAX_SEEN_P50_MS:+ceiling ${MAX_SEEN_P50_MS} ms on after-seen p50}"
line="lag p50 ${p50} ms, p99 ${p99} ms; after seen p50 ${sp50} ms, p99 ${sp99} ms ($SAMPLES samples; ${gate:-not gated})"
echo "tip lag: $line"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "### tip lag"; echo "$line"; } >> "$GITHUB_STEP_SUMMARY"
fi

if [ -n "$MAX_SEEN_P50_MS" ] && [ "$sp50" -gt "$MAX_SEEN_P50_MS" ]; then
  echo "FAIL: after-seen p50 ${sp50} ms exceeds ${MAX_SEEN_P50_MS} ms. Fetch, decode or commit at the"
  echo "      tip got slower; the per-sample figures are in $OUT."
  exit 1
fi
