#!/usr/bin/env bash
#
# Measure peak resident memory of a single-chain index and enforce a ceiling.
#
# Runs the documented scenario - a nest, then `dev --backfill` - samples peak RSS while it indexes,
# and fails if it exceeds MAX_RSS_MB. The ceiling is set generously (256 MB) above the measured ~37 MB
# for a single contract, so this catches a regression rather than policing noise.
#
# **This talks to no third party.** It used to index mainnet USDC over the shipped public endpoints,
# which cost us twice (issue #260):
#
#   1. Repository secrets are not exposed to fork pull requests, so an external contribution could
#      never satisfy a *required* check. It failed on the honest "indexed 0 transfers, refusing to
#      false-pass" path, and a maintainer had to read the log and wave it through - which is how a
#      gate stops meaning anything. Our first external PR (#253) hit exactly this.
#   2. The number was not comparable between runs. Peak RSS depended on how many blocks a
#      rate-limited endpoint happened to serve, so "did this change cost us memory?" was unanswerable.
#
# Both are fixed by serving the chain ourselves (`fixture-rpc footprint`) and checking in the nest
# instead of calling `init`, which removes the ABI-resolution round trip too. Every run indexes
# exactly the same logs, so a change in peak RSS is a change in nuthatch.
#
# Env: BIN (default target/release/nuthatch), MAX_RSS_MB (default 256), PORT (default 8288),
#      RPC_PORT (default 8545).
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
MAX_RSS_MB="${MAX_RSS_MB:-256}"
PORT="${PORT:-8288}"
RPC_PORT="${RPC_PORT:-8545}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# The nest, written here rather than fetched: `init` would resolve an ABI over the network, which is
# the other third party this check used to depend on.
write_nest() {
  local dir="$1"
  mkdir -p "$dir/abis"
  cat > "$dir/nuthatch.toml" <<TOML
[nest]
name = "footprint"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/usdc.json"
TOML
  cat > "$dir/abis/usdc.json" <<'JSON'
[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
  {"name":"from","type":"address","indexed":true},
  {"name":"to","type":"address","indexed":true},
  {"name":"value","type":"uint256","indexed":false}]}]
JSON
}

# A fixed amount of work, so peak RSS is comparable between runs. The mock serves LOGS_PER_BLOCK=4
# `Transfer`s for every block, and `--backfill N` indexes the last N blocks from the tip - so the run
# is complete at exactly BACKFILL_BLOCKS x 4 rows, and we wait for that rather than stopping at
# whichever poll first crossed a threshold. Stopping early was the old behaviour and it made the
# number depend on how fast the machine happened to be, which is the comparability this check exists
# to provide.
BACKFILL_BLOCKS=2000
# `--backfill N` indexes the last N blocks *inclusive of the tip*, and the mock serves 4 logs per
# block - hence N*4 + 4. Measured, not derived: the first version of this asserted N*4 and failed at
# 8004 of 8000, which is the fixture being right and the arithmetic being wrong.
EXPECT=$(( BACKFILL_BLOCKS * 4 + 4 ))
TIP=20000

# Prints "<peak_rss_kb> <entities>" to stdout; all logs go to stderr.
measure() {
  local dir peak=0 rss last rows pid
  dir="$(mktemp -d)"
  write_nest "$dir"
  "$BIN" dev --dir "$dir" --listen "127.0.0.1:$PORT" --backfill "$BACKFILL_BLOCKS" --poll-interval 12s >"$dir/dev.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 80); do
    sleep 1
    kill -0 "$pid" 2>/dev/null || break
    rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')"
    if [ -n "$rss" ] && [ "$rss" -gt "$peak" ]; then peak="$rss"; fi
    # `entities` on `/` counts *derived* entities, not rows - asserting on it measured the balances
    # view rather than the backfill. Completion is the cursor reaching the mock's tip.
    last="$(curl -s "127.0.0.1:$PORT/" 2>/dev/null | grep -o '"last_block":"[0-9]*"' | grep -o '[0-9]*' || true)"
    if [ -n "$last" ] && [ "$last" -ge "$TIP" ]; then break; fi
  done
  # The row count is the number that has to be identical run to run.
  rows="$(curl -s -G "127.0.0.1:$PORT/sql" --data-urlencode \
    'q=SELECT count(*) n FROM usdc__transfer' 2>/dev/null \
    | grep -o '"n":[0-9]*' | grep -o '[0-9]*' || true)"
  rows="${rows:-0}"
  kill "$pid" 2>/dev/null || true
  if [ "$rows" -lt "$EXPECT" ]; then tail -30 "$dir/dev.log" >&2 || true; fi
  echo "$peak $rows"
}

out="$(measure || echo "0 0")"
peak="${out%% *}"; entities="${out##* }"

peak_mb=$(( (${peak:-0} + 1023) / 1024 ))
echo "peak RSS: ${peak_mb} MB over ${entities:-0}/${EXPECT} transfers (ceiling ${MAX_RSS_MB} MB)"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### footprint"
    echo "peak RSS **${peak_mb} MB** over ${entities:-0} transfers (ceiling ${MAX_RSS_MB} MB)"
  } >> "$GITHUB_STEP_SUMMARY"
fi

if [ "${entities:-0}" -lt "$EXPECT" ]; then
  echo "FAIL: indexed ${entities:-0} of $EXPECT transfers - the run did not complete, so the peak is"
  echo "      not comparable and this is not a pass. The chain is served locally, so this is a"
  echo "      nuthatch or fixture fault rather than a flaky endpoint; the indexer's log is above."
  exit 1
fi
if [ "$peak_mb" -gt "$MAX_RSS_MB" ]; then
  echo "FAIL: peak RSS ${peak_mb} MB exceeds ceiling ${MAX_RSS_MB} MB"
  exit 1
fi
echo "OK: within budget"
