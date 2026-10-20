#!/usr/bin/env bash
#
# The ≤2 GB per-cursor RAM budget, ABI-breadth half (#286).
#
# `multinest-footprint.sh` answers density and event *rate* (twenty nests, ten events). This answers
# ABI *breadth*: one nest, 31 event types (the SubgraphService figure named on #286), 31 logs/block
# so every table is hit every block, at tip after a backfill. More decoders, more tables, more
# keyspaces - a different pressure on the hot store than more rows through the same ten tables.
#
# Hermetic: `fixture-rpc multinest` serves the chain, the nest is written inline, topic0s come from the
# generated ABI. No secret, a fork can satisfy it.
#
# Absence-test guard: FINAL_TIP reached, row floor, **every one of the 31 tables non-empty**.
#
# MAX_RSS_MB=2048 is the product promise. REGRESSION_MB is unset until this scenario has a runner
# noise band of its own - do not copy the density job's 180 MB here.
#
# Env: BIN, MAX_RSS_MB (2048), REGRESSION_MB (unset = report only), EVENTS (31),
#      LOGS_PER_BLOCK (31), BACKFILL_BLOCKS (1000), INITIAL_TIP (20000), FINAL_TIP (20200),
#      TIP_STEP (8), PORT (8291), RPC_PORT (8548), REPORT.
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
MAX_RSS_MB="${MAX_RSS_MB:-2048}"
REGRESSION_MB="${REGRESSION_MB:-}"
NESTS=1
EVENTS="${EVENTS:-31}"
LOGS_PER_BLOCK="${LOGS_PER_BLOCK:-31}"
BACKFILL_BLOCKS="${BACKFILL_BLOCKS:-1000}"
INITIAL_TIP="${INITIAL_TIP:-20000}"
FINAL_TIP="${FINAL_TIP:-20200}"
TIP_STEP="${TIP_STEP:-8}"
PORT="${PORT:-8291}"
RPC_PORT="${RPC_PORT:-8548}"
REPORT="${REPORT:-}"
# Generous on purpose. Peak RSS barely depends on core count, but *wall clock* does, and a GitHub
# runner has 2 vCPUs against the 32 this was tuned on. A timeout here is reported as a failure to
# complete (which it is - an incomplete run's peak is not comparable), so it must not be tripped by a
# slow runner or the job becomes flaky and gets disabled.
TIMEOUT_S="${TIMEOUT_S:-1800}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ABI="$(mktemp)"
python3 - "$EVENTS" "$ABI" <<'PY'
import json, sys
n, path = int(sys.argv[1]), sys.argv[2]
events = []
for i in range(n):
    events.append({
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "a", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "b", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "v", "type": "uint256"},
        ],
        "name": f"Event{i:02d}",
        "type": "event",
    })
open(path, "w").write(json.dumps(events))
PY
TABLES="$(python3 - "$ABI" <<'PY'
import json, sys
abi = json.load(open(sys.argv[1]))
print(" ".join(f"wide__event{i:02d}" for i, e in enumerate(abi) if e.get("type") == "event"))
PY
)"
# The ABI file is in /tmp; delete it with the rest of the workdir, not here.

# Absolute, because the runtime directory is a tmpdir and a relative BIN would resolve against it.
# Checked rather than assumed: a missing binary otherwise surfaces as "dev exited early", which reads
# like a nuthatch fault and sent me looking in the wrong place once already.
[ -x "$BIN" ] || { echo "FAIL: no executable at BIN=$BIN (build it, or set BIN)"; exit 1; }
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

# Blocks each nest ends up holding: the backfill reaches back from INITIAL_TIP, and the tip-follow
# carries it to FINAL_TIP. Every nest sees every block of that span, so the row count is exact.
#
# `--backfill N` starts at `tip - N`, i.e. it indexes N+1 blocks inclusive of the tip - measured from
# a run ("cold start: backfilling from block 19950 (tip 20000)" for N=50), not derived. `footprint.sh`
# carries the same off-by-one for the same reason, and got it wrong the first time too.
FIRST_BLOCK=$(( INITIAL_TIP - BACKFILL_BLOCKS ))
TOTAL_BLOCKS=$(( FINAL_TIP - FIRST_BLOCK + 1 ))
EXPECT_ROWS=$(( TOTAL_BLOCKS * LOGS_PER_BLOCK ))
# The floor is deliberately below EXPECT_ROWS rather than equal to it: rows past finality are sealed
# to Parquet and `/sql` reads hot ∪ cold, so the total should be exact - but a *floor* is what stops
# a false pass, and pinning it to the exact figure would turn a sealing-boundary off-by-one into a
# budget failure. 90% is far below anything a real run produces and far above zero.
MIN_ROWS_PER_NEST=$(( EXPECT_ROWS * 9 / 10 ))

WORK="$(mktemp -d)"
RPC_PID=""
DEV_PID=""
cleanup() {
  [ -n "$DEV_PID" ] && kill "$DEV_PID" 2>/dev/null || true
  [ -n "$RPC_PID" ] && kill "$RPC_PID" 2>/dev/null || true
  rm -f "$ABI"
}
trap cleanup EXIT

# --- the runtime directory ---------------------------------------------------------------------
# Written here rather than fetched: `init` would resolve an ABI over the network, which is the third
# party this family of checks exists to not have. The `nests = [...]` form (RFC-0032) puts every nest
# on chain 1, so the runtime groups them onto exactly ONE cursor - which is what makes process RSS a
# fair proxy for cursor RSS.
write_runtime() {
  local dir="$1" n="$2" i
  mkdir -p "$dir/nests"
  {
    echo '[runtime]'
    echo 'name = "wideabi"'
    printf 'nests = ['
    for i in $(seq 1 "$n"); do printf '"n%d"' "$i"; [ "$i" -lt "$n" ] && printf ', '; done
    printf ']\n\n'
    echo '[[chains]]'
    echo 'chain = "mainnet"'
    echo 'chain_id = 1'
    echo "rpc_urls = [\"http://127.0.0.1:$RPC_PORT\"]"
  } > "$dir/mounts.toml"

  for i in $(seq 1 "$n"); do
    local nest="$dir/nests/n$i"
    mkdir -p "$nest/abis"
    cp "$ABI" "$nest/abis/wide.json"
    cat > "$nest/nuthatch.toml" <<TOML
[nest]
name = "n$i"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "wide"
address = "$(printf '0x%040x' "$i")"
abi = "abis/wide.json"
TOML
  done
}

# --- RSS ---------------------------------------------------------------------------------------
# `VmHWM` is the kernel's own high-water mark for the process, so the peak is exact rather than
# whatever a 1 Hz sampler happened to catch. `footprint.sh` samples with `ps` because it predates
# this and is portable; here the runner is Linux and an exact peak is worth having, since the whole
# question is "what is the worst this got to". Falls back to sampling off Linux.
peak_rss_kb() {
  local pid="$1"
  if [ -r "/proc/$pid/status" ]; then
    awk '/^VmHWM:/ {print $2}' "/proc/$pid/status"
  else
    ps -o rss= -p "$pid" 2>/dev/null | tr -d ' '
  fi
}
current_rss_kb() {
  local pid="$1"
  if [ -r "/proc/$pid/status" ]; then
    awk '/^VmRSS:/ {print $2}' "/proc/$pid/status"
  else
    ps -o rss= -p "$pid" 2>/dev/null | tr -d ' '
  fi
}

# A mounted nest's summary is `/<alias>`, with NO trailing slash - `/<alias>/` is a 404. (`/<alias>/health`
# and `/<alias>/sql` do take the slash, which is why the wrong one looks plausible right up until every
# poll comes back empty and the run reads as "never reached tip".)
nest_head() { curl -s -m 5 "127.0.0.1:$PORT/$1" 2>/dev/null | grep -o '"last_block":"[0-9]*"' | grep -o '[0-9]*' || true; }
nest_rows() {
  curl -s -m 15 -G "127.0.0.1:$PORT/$1/sql" --data-urlencode "q=SELECT count(*) n FROM $2" 2>/dev/null \
    | grep -o '"n":[0-9]*' | grep -o '[0-9]*' || true
}

echo "scenario: $NESTS nest, ${EVENTS}-event ABI (breadth), ${LOGS_PER_BLOCK} logs/block"
echo "          blocks $FIRST_BLOCK..$FINAL_TIP (backfill $BACKFILL_BLOCKS, then tip-follow to $FINAL_TIP)"
echo "          expecting $EXPECT_ROWS rows/nest, floor $MIN_ROWS_PER_NEST; ceiling $MAX_RSS_MB MB"

RPC_BIN="$(bash "$HERE/../../scripts/fixture-rpc.sh")"
"$RPC_BIN" multinest --port "$RPC_PORT" --abi "$ABI" \
  --contracts "$NESTS" --logs-per-block "$LOGS_PER_BLOCK" \
  --initial-tip "$INITIAL_TIP" --final-tip "$FINAL_TIP" --tip-step "$TIP_STEP" &
RPC_PID=$!
for _ in $(seq 1 40); do
  curl -fsS -m 2 -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
    "127.0.0.1:$RPC_PORT" >/dev/null 2>&1 && break
  sleep 0.25
done

write_runtime "$WORK" "$NESTS"
"$BIN" dev --dir "$WORK" --listen "127.0.0.1:$PORT" --backfill "$BACKFILL_BLOCKS" --poll-interval 12s \
  >"$WORK/dev.log" 2>&1 &
DEV_PID=$!

# Wait for every nest to reach FINAL_TIP, sampling RSS on the way. `at_tip` samples are taken only
# once the backfill has drained (head >= INITIAL_TIP), so the steady-state figure describes the live
# path rather than the burst that got there.
#
# Bounded on elapsed wall clock (`SECONDS`, bash's own since-shell-start counter), not on iteration
# count: each iteration is `sleep 1` plus up to NESTS+1 curls at `-m 5`, so a `seq 1 "$TIMEOUT_S"`
# loop bounds seconds-of-sleep, not seconds-of-wall-clock, and can run for hours under slow curls
# while TIMEOUT_S=1800 and the failure text both say 30 minutes (#449).
at_tip_samples=""
done_at=""
SECONDS=0
while [ "$SECONDS" -lt "$TIMEOUT_S" ]; do
  sleep 1
  kill -0 "$DEV_PID" 2>/dev/null || { echo "FAIL: dev exited early"; tail -40 "$WORK/dev.log"; exit 1; }
  head1="$(nest_head n1)"
  if [ -n "$head1" ] && [ "$head1" -ge "$INITIAL_TIP" ] 2>/dev/null; then
    at_tip_samples="$at_tip_samples $(current_rss_kb "$DEV_PID")"
  fi
  # Slowest nest decides: the cursor is only at FINAL_TIP when all of them are.
  behind=0
  for i in $(seq 1 "$NESTS"); do
    h="$(nest_head "n$i")"
    if [ -z "$h" ] || [ "$h" -lt "$FINAL_TIP" ] 2>/dev/null; then behind=1; break; fi
  done
  if [ "$behind" -eq 0 ]; then done_at="$SECONDS"; break; fi
done

PEAK_KB="$(peak_rss_kb "$DEV_PID")"
PEAK_MB=$(( (${PEAK_KB:-0} + 1023) / 1024 ))
AT_TIP_MB="$(printf '%s\n' $at_tip_samples | sort -n | awk '{a[NR]=$1} END {if (NR) printf "%d", (a[int((NR+1)/2)]+1023)/1024; else print 0}')"

# --- the guard: prove the scenario actually ran -------------------------------------------------
fail=0
if [ -z "$done_at" ]; then
  echo "FAIL: not every nest reached $FINAL_TIP within the timeout - the peak is not comparable and"
  echo "      this is not a pass. The chain is served locally, so this is a nuthatch or fixture fault."
  tail -40 "$WORK/dev.log"
  fail=1
fi

total_rows=0
for i in $(seq 1 "$NESTS"); do
  nest_total=0
  for tbl in $TABLES; do
    r="$(nest_rows "n$i" "$tbl")"; r="${r:-0}"
    if [ "$r" -eq 0 ]; then
      echo "FAIL: n$i.$tbl is empty. Every one of the ${EVENTS} tables must hold rows - an empty one"
      echo "      means that event's topic0 never matched, which decodes to nothing and would let this"
      echo "      check pass having measured a fraction of the ABI it claims to measure."
      fail=1
    fi
    nest_total=$(( nest_total + r ))
  done
  if [ "$nest_total" -lt "$MIN_ROWS_PER_NEST" ]; then
    echo "FAIL: n$i holds $nest_total rows, below the $MIN_ROWS_PER_NEST floor - the cursor was not"
    echo "      loaded, so 'under budget' would mean nothing."
    fail=1
  fi
  total_rows=$(( total_rows + nest_total ))
done

echo
echo "peak RSS:     ${PEAK_MB} MB   (ceiling ${MAX_RSS_MB} MB)"
echo "at-tip RSS:   ${AT_TIP_MB} MB   (median while following the live tip)"
echo "rows:         ${total_rows} across ${EVENTS} tables"
echo "reached tip:  ${done_at:-never} s"

if [ -n "$REPORT" ]; then
  cat > "$REPORT" <<JSON
{
  "scenario": "wide ABI, one nest, at tip",
  "nests": $NESTS,
  "events_in_abi": $EVENTS,
  "logs_per_block_per_contract": $LOGS_PER_BLOCK,
  "blocks": $TOTAL_BLOCKS,
  "backfill_blocks": $BACKFILL_BLOCKS,
  "tip_follow_blocks": $(( FINAL_TIP - INITIAL_TIP )),
  "rows_total": $total_rows,
  "peak_rss_mb": $PEAK_MB,
  "at_tip_rss_mb": ${AT_TIP_MB:-0},
  "budget_mb": $MAX_RSS_MB,
  "regression_ceiling_mb": ${REGRESSION_MB:-null},
  "seconds_to_final_tip": ${done_at:-null},
  "commit": "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)",
  "hardware": "$(nproc 2>/dev/null || echo '?') cores, $(awk '/MemTotal/ {printf "%d GB", int($2/1024/1024 + 0.5)}' /proc/meminfo 2>/dev/null || echo '? GB'), $(uname -s)"
}
JSON
  echo "report:       $REPORT"
fi

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### per-cursor RAM budget (wide ABI, one nest, at tip)"
    echo "peak RSS **${PEAK_MB} MB** / at-tip **${AT_TIP_MB} MB**, ceiling ${MAX_RSS_MB} MB"
    echo ""
    echo "1 nest, ${EVENTS}-event ABI, ${total_rows} rows. #286 breadth half."
  } >> "$GITHUB_STEP_SUMMARY"
fi

[ "$fail" -eq 0 ] || exit 1

if [ "$PEAK_MB" -gt "$MAX_RSS_MB" ]; then
  echo "FAIL: peak RSS ${PEAK_MB} MB exceeds the ${MAX_RSS_MB} MB per-cursor budget (CLAUDE.md"
  echo "      non-negotiable 2, RFC-0021). Density is RAM-bounded: this is a product promise, not"
  echo "      a tuning parameter. Do not raise this ceiling to make the build green."
  exit 1
fi

if [ -n "$REGRESSION_MB" ] && [ "$PEAK_MB" -gt "$REGRESSION_MB" ]; then
  echo "FAIL: peak RSS ${PEAK_MB} MB exceeds the ${REGRESSION_MB} MB regression ceiling."
  echo
  echo "      This is NOT the 2 GB budget - that one still has $(( MAX_RSS_MB - PEAK_MB )) MB of margin."
  echo "      It means this scenario now costs materially more memory than it did, which is worth"
  echo "      understanding while the cause is one PR wide rather than a year of drift."
  echo
  echo "      If the increase is understood and wanted, re-baseline: run the scenario enough times to"
  echo "      re-establish the noise band on THIS scenario (not the density job's) and move the"
  echo "      ceiling with the new numbers written down. Run it on the"
  echo "      hardware that enforces the ceiling: a band from a different box is not this box's band."
  echo "      Do not just nudge it up until it passes."
  exit 1
fi

echo "OK: within the per-cursor budget, $(( MAX_RSS_MB - PEAK_MB )) MB of margin"
[ -n "$REGRESSION_MB" ] && echo "OK: within the ${REGRESSION_MB} MB regression ceiling"
exit 0
