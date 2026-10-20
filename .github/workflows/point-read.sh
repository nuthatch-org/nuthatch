#!/usr/bin/env bash
#
# Measure entity point-read p50/p99 and enforce a ceiling (issue #283).
#
# CLAUDE.md names entity point-read p50/p99 as a CI artifact whose regressions fail the build.
# `nuthatch bench query` has measured it for a while; nothing compared the number to anything, so it
# was a print statement rather than a gate and every perf change to the storage path could regress it
# in silence.
#
# The chain is served locally by `fixture-rpc multinest` and the nest is written here rather than fetched,
# so there is **no secret and no third party** (issue #260). A fork PR can satisfy this check, and
# every run reads exactly the same rows - so a change in p99 is a change in nuthatch and not in
# somebody's rate limiter.
#
# **The scenario is the `per-cursor RAM budget` job's** (issue #424): the real ten-event Uniswap V4
# `PoolManager` ABI at 200 logs a block, leaving 12,800 rows hot across ten tables once everything
# past finality has sealed. It used to be `footprint.sh`'s single-event fixture, whose settled tip is
# 256 rows - a hot store small enough that a 32-core dev box and a 4-core runner measured the same
# number, which is a precise measurement of a case nobody runs. See the fixture block below.
#
# The ceilings were chosen by breaking the code on purpose and seeing what the numbers did, rather
# than by leaving "plenty of headroom" - see the justification in ci.yml, which carries the measured
# pair for the fixture in force.
#
# **p99 and p99.9 are recorded and gated on by nothing**, and that is a deliberate design call rather
# than an omission. Gating p50 loosely and tracking the tail precisely are two different jobs: a p99
# ceiling tight enough to catch a real regression flakes on a shared runner, and one loose enough not
# to flake catches nothing. This is not a judgement call here - it was measured. The linear-scan
# mutation that p50 catches at 18.15µs against an 8µs ceiling only moved p99 to 34.45µs, so the 150µs
# p99 backstop this script used to apply did **not** fire on the one regression the gate exists to
# catch. A ceiling that passes the known break is not a weak gate, it is decoration that reads as
# coverage. The tail is the number we track across releases and read with our eyes, in the artifact.
#
# `MAX_P99_US` therefore has no default. Set it to gate the tail deliberately (an operator on a quiet
# machine may well want to); leave it unset and p99 is reported and not enforced.
#
# **Running this on your own machine:** 8µs is `ubuntu-latest`'s number, and applying a ceiling
# measured on one machine to a different one is the mistake this gate already made once. A slower or
# busier box can fail it without anything being wrong, so raise it - `MAX_P50_US=200 bash
# .github/workflows/point-read.sh` reports the numbers without gating on them in any useful sense.
# There is deliberately no "off" for p50: it always applies, and `MAX_P50_US=0` would fail every run
# rather than disable the check. What CI enforces is set in ci.yml and is not affected by this
# default.
#
# **`BASELINE` checks the committed reference came from this machine** (issue #385).
# `docs/bench/point-read.json` is the number anyone re-baselining the ceiling will reach for, and for
# a year it was a 32-core dev-box artifact while the gate ran on a 4-core runner. Nothing detected
# that, because a committed JSON file cannot go stale loudly - it can only be read and believed. So
# CI now points `BASELINE` at it and this script compares the `hardware` the baseline records against
# the `hardware` the run it just did records. Mismatch is a hard failure.
#
# It compares provenance and **not values**, deliberately. A "measured p50 must be within Nx of the
# baseline" check is a second, much tighter ceiling wearing a different hat: the runner's own p50 has
# been seen from 0.58µs to 0.82µs at fixed commits and its p99.9 from 0.77µs to 23.02µs, so any factor
# loose enough not to flake is looser than the 8µs gate already is, and any factor tight enough to
# add information flakes. The one thing a committed baseline can be *definitively* wrong about is
# which machine produced it, so that is what is enforced.
#
# Unset by default, because the answer is machine-dependent and this script is documented for running
# by hand: on your own box the check would fail correctly and uselessly every time. What CI enforces
# is set in ci.yml.
#
# **Every scenario knob defaults to the enforced scenario** (#395's lesson, applied here by #424).
# `bash .github/workflows/point-read.sh` with no environment measures the thing CI gates, so anyone
# re-baselining from the tool's own default measures the right shape. The RSS harness once defaulted
# to a fifth of its enforced scenario while its FAIL message pointed people at the harness, which
# lands a re-derived ceiling *below* the healthy figure. CI therefore sets only what is genuinely the
# job's own - enforcement policy and where to write the artifact - and must not restate the scenario.
#
# Env: BIN (default target/release/nuthatch), MAX_P50_US (see ci.yml for the value CI uses and why),
#      MAX_P99_US (unset: recorded, not gated), PORT (default 8289), RPC_PORT (default 8546), OUT
#      (default point-read-report.json), LABEL (default names the fixture),
#      BASELINE (unset: not checked; CI sets docs/bench/point-read.json),
#      BACKFILL_BLOCKS (1000), LOGS_PER_BLOCK (200), TIP (20000), BACKFILL_TIMEOUT_S (900).
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
MAX_P50_US="${MAX_P50_US:-8}"
MAX_P99_US="${MAX_P99_US:-}"
BASELINE="${BASELINE:-}"
PORT="${PORT:-8289}"
RPC_PORT="${RPC_PORT:-8546}"
OUT="${OUT:-point-read-report.json}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- the fixture: a realistic nest, not a settled tip of 256 rows (issue #424) -------------------
#
# **The scenario is the gate.** #283's design call asked for the shape #286/#284 would actually
# perturb - "the hot store under a realistic nest, the same scenario family the RAM work will load" -
# and the first version of this script did not deliver it. It borrowed `footprint.sh`'s fixture: one
# contract, one event, four logs a block. Everything past finality is sealed to Parquet and pruned
# out of redb, and `get_entity` is a hot-store read, so what the gate actually measured was
# 64 blocks x 4 logs = **256 rows** - a hot store that fits in L2 cache, which is why a 32-core dev
# box and a 4-core runner produced the same number. Precise, cheap, and a measurement of a case
# nobody runs.
#
# So the fixture is now the `per-cursor RAM budget` job's chain, served by the same
# `fixture-rpc multinest` off the same real Uniswap V4 `PoolManager` ABI: ten event types, ten tables, and
# **200 logs per block**. That is the rate the RSS scenario puts on one cursor (20 nests x 10
# logs/block/contract), carried here by a single high-rate contract so that it lands in one nest's
# store - which is the store a point-read seeks in, since each nest gets its own redb. The hot store
# is 64 x 200 = **12,800 rows across 10 tables**, 50x what it was and the same order as the cursor
# the RAM work loads.
#
# Two gates, one chain shape, on purpose. `footprint.sh` stays on its small single-event fixture
# because a 256 MB ceiling over 60 MB is a sensitive regression tripwire; this one wants the dense
# case because a point-read gate's discriminating power is a function of how many rows a broken
# read path would have to walk.
#
# `--contracts 1` rather than 20: one nest with 20 aliases would carry 200 tables, not 10, and the
# table count is part of the shape being matched. The event *rate* is what sizes the hot store.
ABI="$HERE/multinest-abi.json"
BACKFILL_BLOCKS="${BACKFILL_BLOCKS:-1000}"
LOGS_PER_BLOCK="${LOGS_PER_BLOCK:-200}"
TIP="${TIP:-20000}"
# `--backfill N` indexes N+1 blocks: it starts at `tip - N` inclusive of the tip. Measured, not
# derived - `multinest-footprint.sh` carries the same off-by-one and got it wrong the first time too.
EXPECT=$(( (BACKFILL_BLOCKS + 1) * LOGS_PER_BLOCK ))

# **Point-reads see the unsealed tip, not the whole backfill.** Rows past finality are sealed to
# Parquet and pruned out of redb, so of the rows indexed only the last finality window is still in
# the hot store - and `get_entity` is a hot-store read. Measured on the old fixture: sealed_through
# 19,936 against a 20,000 tip, so exactly `FINALITY_DEPTH` blocks stay hot.
#
# This is written down because the first version of this script asserted `--min-reads 8004` and
# therefore failed *every* run: it was reasoned from the backfill size rather than run. If nuthatch's
# finality or sealing changes, this goes red and wants a human, which is the correct outcome rather
# than a silently shrinking sample.
#
# Named rather than left as a bare `64 * 200`, because both numbers are quotations from elsewhere and
# a reader who cannot see the source cannot check them: mainnet is `Finality::Depth(64)`
# (`src/chains.rs:77`) and the mock serves `--logs-per-block` per contract per block up to a fixed
# `TIP`, so the settled hot tip is blocks 19937..=20000.
#
# It is a **floor, not an equality**. A run whose seal loop has not caught up holds *more* than this,
# never fewer, so the check is satisfied at any point after the backfill completes rather than only in
# the settled state. Asserting equality would turn the seal loop's timing into a flaky gate.
FINALITY_DEPTH=64
HOT_EXPECT=$(( FINALITY_DEPTH * LOGS_PER_BLOCK ))

# The tip is **fixed**, unlike the RSS scenario's. That gate is about sustained operation at tip, so
# its tip has to move; this one measures an offline store after `dev` has been stopped, so a moving
# tip would buy nothing and cost the exact hot-row count that `--min-reads` is derived from.
RPC_BIN="$(bash "$HERE/../../scripts/fixture-rpc.sh")"
"$RPC_BIN" multinest --port "$RPC_PORT" --abi "$ABI" \
  --contracts 1 --logs-per-block "$LOGS_PER_BLOCK" \
  --initial-tip "$TIP" --final-tip "$TIP" --tip-step 0 &
RPC_PID=$!
trap 'kill "$RPC_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  curl -fsS -m 2 -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
    "127.0.0.1:$RPC_PORT" >/dev/null 2>&1 && break
  sleep 0.25
done

DIR="$(mktemp -d)"
mkdir -p "$DIR/abis"
cp "$ABI" "$DIR/abis/pool_manager.json"
# Contract 1 is address 0x…01, matching the fixture chain's derivation.
cat > "$DIR/nuthatch.toml" <<TOML
[nest]
name = "point-read"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "pool_manager"
address = "$(printf '0x%040x' 1)"
abi = "abis/pool_manager.json"
TOML

echo "fixture: 1 nest, 10-event V4 ABI, ${LOGS_PER_BLOCK} logs/block, blocks $(( TIP - BACKFILL_BLOCKS ))..$TIP"
echo "         expecting $EXPECT rows, of which $HOT_EXPECT stay hot past sealing"

"$BIN" dev --dir "$DIR" --listen "127.0.0.1:$PORT" --backfill "$BACKFILL_BLOCKS" --poll-interval 12s >"$DIR/dev.log" 2>&1 &
DEV_PID=$!
trap 'kill "$DEV_PID" 2>/dev/null || true; kill "$RPC_PID" 2>/dev/null || true' EXIT
# Generous, and generous for a stated reason: this fixture indexes 50x the rows the old one did, and
# a 2-vCPU runner takes minutes over it. A timeout here is reported as a failure to complete (which
# it is - an incomplete run's latencies are not a measurement), so tripping it on a slow runner would
# make the job flaky, and a flaky required check gets disabled.
BACKFILL_TIMEOUT_S="${BACKFILL_TIMEOUT_S:-900}"
for _ in $(seq 1 "$BACKFILL_TIMEOUT_S"); do
  sleep 1
  kill -0 "$DEV_PID" 2>/dev/null || break
  last="$(curl -s "127.0.0.1:$PORT/" 2>/dev/null | grep -o '"last_block":"[0-9]*"' | grep -o '[0-9]*' || true)"
  if [ -n "$last" ] && [ "$last" -ge "$TIP" ]; then break; fi
done

# **Every one of the ten tables must hold rows**, not just the total. The mock cycles the event types
# so all ten are populated, and an empty table means that event's topic0 never matched - which does
# not error, it decodes to nothing. Summing a total alone would let this gate measure a fraction of
# the ABI it claims to and call it a pass; `multinest-footprint.sh` guards its version of the same
# fixture the same way.
TABLES="pool_manager__approval pool_manager__donate pool_manager__initialize \
pool_manager__modify_liquidity pool_manager__operator_set pool_manager__ownership_transferred \
pool_manager__protocol_fee_controller_updated pool_manager__protocol_fee_updated \
pool_manager__swap pool_manager__transfer"
rows=0
empty=""
for tbl in $TABLES; do
  r="$(curl -s -m 30 -G "127.0.0.1:$PORT/sql" --data-urlencode "q=SELECT count(*) n FROM $tbl" \
    2>/dev/null | grep -o '"n":[0-9]*' | grep -o '[0-9]*' || true)"
  r="${r:-0}"
  [ "$r" -eq 0 ] && empty="$empty $tbl"
  rows=$(( rows + r ))
done

# `bench query` opens the store directly, so `dev` has to be gone first - not merely asked to go.
kill "$DEV_PID" 2>/dev/null || true
wait "$DEV_PID" 2>/dev/null || true

if [ -n "$empty" ]; then
  tail -30 "$DIR/dev.log" || true
  echo "FAIL: these tables are empty:$empty"
  echo "      Each is one event type's topic0. An unmatched topic0 decodes to nothing rather than"
  echo "      erroring, so this would otherwise be a green run over a fraction of the ABI."
  exit 1
fi
if [ "$rows" -lt "$EXPECT" ]; then
  tail -30 "$DIR/dev.log" || true
  echo "FAIL: indexed $rows of $EXPECT rows - the run did not complete, so its latencies are not a"
  echo "      measurement. The chain is served locally, so this is a nuthatch or fixture fault"
  echo "      rather than a flaky endpoint; the indexer's log is above."
  exit 1
fi

# `--min-reads` is not belt-and-braces on top of the row check above: an empty store samples no keys
# and reports p50 = 0µs, which passes any ceiling. The floor is what makes a green run mean something.
#
# p99 is passed only when asked for. An unset `--max-point-read-p99-us` is not a ceiling of infinity
# dressed up as a number: `check_gate` treats an absent limit as "not asked for" and reports the
# measurement, which is the whole point of recording the tail without enforcing it.
#
# The expansion below is `${p99_arg[@]+"${p99_arg[@]}"}` rather than the obvious `"${p99_arg[@]}"`,
# because this script is `set -u` and bash treats an *empty* array as unset when it expands one.
# Confirmed on `bash:3.2` (3.2.57, what macOS ships as `/bin/bash`): the bare form aborts with
# `arr[@]: unbound variable` before the bench runs. That is the default path now that `MAX_P99_US`
# has no value, and it is the path the header above tells operators to run by hand - so on a Mac the
# documented invocation would have died rather than measured. Bash 4.4+ made the bare form legal,
# which is why CI (ubuntu, bash 5.x) stays green either way and would never have caught this.
p99_arg=()
if [ -n "$MAX_P99_US" ]; then
  p99_arg=(--max-point-read-p99-us "$MAX_P99_US")
fi

set +e
"$BIN" bench query --dir "$DIR" --reads "$EXPECT" --iters 5 --out "$OUT" \
  --label "${LABEL:-point-read gate: $EXPECT rows indexed, $HOT_EXPECT hot across 10 tables, 10-event V4 ABI at $LOGS_PER_BLOCK logs/block, locally-served chain}" \
  --min-reads "$HOT_EXPECT" \
  --max-point-read-p50-us "$MAX_P50_US" \
  ${p99_arg[@]+"${p99_arg[@]}"} | tee "$DIR/bench.log"
status="${PIPESTATUS[0]}"
set -e

p50="$(grep -o '"point_read_p50_us": [0-9.]*' "$OUT" 2>/dev/null | grep -o '[0-9.]*$' || echo '?')"
p99="$(grep -o '"point_read_p99_us": [0-9.]*' "$OUT" 2>/dev/null | grep -o '[0-9.]*$' || echo '?')"
p999="$(grep -o '"point_read_p999_us": [0-9.]*' "$OUT" 2>/dev/null | grep -o '[0-9.]*$' || echo '?')"
# The tail is labelled "tracked" rather than given a fake ceiling, so a reader can see at a glance
# which number failed a build and which one is here to be read across releases.
if [ -n "$MAX_P99_US" ]; then
  p99_note="ceiling ${MAX_P99_US}µs"
else
  p99_note="tracked, not gated"
fi
echo "point-read over $rows rows: p50 ${p50}µs (ceiling ${MAX_P50_US}µs), p99 ${p99}µs ($p99_note), p99.9 ${p999}µs (tracked, not gated)"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### point-read latency"
    echo "p50 **${p50}µs** (ceiling ${MAX_P50_US}µs) over $rows rows"
    echo ""
    echo "Tail, recorded and not gated: p99 **${p99}µs** ($p99_note), p99.9 **${p999}µs**."
  } >> "$GITHUB_STEP_SUMMARY"
fi

# The provenance check runs whatever the ceiling did, and reports separately: a run that is both
# slow *and* baselined against the wrong machine wants to be told both things, and the second is the
# reason to distrust the first.
# `-f "$OUT"` because a run that died before writing its report has no hardware string to compare,
# and `status` is already non-zero on that path - reporting a bogus mismatch would bury the real
# failure under a second one.
if [ -n "$BASELINE" ] && [ -f "$OUT" ]; then
  if [ ! -f "$BASELINE" ]; then
    echo "FAIL: BASELINE=$BASELINE does not exist. The committed reference is what a re-baseline is"
    echo "      derived from; a missing one is not a pass."
    exit 1
  fi
  hw_of() { jq -r '.hardware // ""' "$1"; }
  base_hw="$(hw_of "$BASELINE")"
  this_hw="$(hw_of "$OUT")"
  if [ "$base_hw" != "$this_hw" ]; then
    echo "FAIL: $BASELINE records hardware '$base_hw', this run measured on '$this_hw'."
    echo "      The committed baseline has to come from the machine that enforces the gate. It did"
    echo "      not once before (#385): a 32-core dev-box artifact sat in docs/bench/point-read.json"
    echo "      while this job ran on a 4-core runner, and baseline and regression do not scale"
    echo "      together across hardware - the linear-scan mutation this gate exists to catch runs"
    echo "      *faster* on the runner (18.15µs) than on the dev box (24.17µs), so a ceiling derived"
    echo "      from the dev box left 1.21x of real margin rather than the 1.61x it claimed."
    echo "      Fix: take point-read-report.json from a green 'point-read latency' run on main and"
    echo "      commit it, or - if the runner spec genuinely changed - re-measure the ceiling here"
    echo "      and say so in docs/benchmarks.md. Do not edit the hardware field."
    exit 1
  fi

  # **And the same scenario, not just the same machine** (issue #424). The hardware check alone would
  # have stayed green through this very change: swapping the fixture from a 256-row settled tip to a
  # 12,800-row hot store leaves the runner spec untouched, so the committed baseline would go on
  # describing a scenario nobody measures any more while still passing its own provenance check. The
  # label is generated from the scenario parameters, so it drifts exactly when the scenario does.
  label_of() { jq -r '.label // ""' "$1"; }
  base_label="$(label_of "$BASELINE")"
  this_label="$(label_of "$OUT")"
  if [ "$base_label" != "$this_label" ]; then
    echo "FAIL: $BASELINE was measured on a different scenario."
    echo "      baseline: $base_label"
    echo "      this run: $this_label"
    echo "      A baseline for a scenario the gate no longer runs is not a baseline. The label is"
    echo "      derived from the fixture, so this fires when the fixture moves and the committed"
    echo "      artifact does not - which is how #424's 256-row hot store outlived the design call"
    echo "      that asked for a realistic one."
    echo "      Fix: refresh docs/bench/point-read.json from a green run of this job, and re-measure"
    echo "      the ceiling - a different hot-store size moves the baseline and the regression by"
    echo "      different factors."
    exit 1
  fi
  echo "OK: baseline $BASELINE matches this machine ('$base_hw') and this scenario"
fi

if [ "$status" -ne 0 ]; then
  echo "FAIL: the point-read gate rejected this run - see the message above."
  exit 1
fi
echo "OK: within the point-read ceilings"
