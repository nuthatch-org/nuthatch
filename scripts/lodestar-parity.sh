#!/usr/bin/env bash
# Continuous Lodestar parity, at a pinned block (#1076).
#
# An absent comparison must not read as agreement. This script exits 1 when it
# cannot reach the nest, when a view returns no rows, when GRAPH_API_KEY is
# unset so the subgraph side cannot be asked, or when comparable sides disagree.
#
# Exit status:
#   0  parity CLEAN - every comparison ran and agreed
#   2  parity NOT CLEAN - every gated comparison agreed, known differences remain (#1116, #1114, #1983)
#   1  anything else, including a genuine disagreement and any failure to compare
#   3  head mode only: the subgraph could not be asked at the nest's head, so nothing was compared
#   4  the subgraph side did not answer (auth or GraphQL error, HTTP error, timeout, malformed
#      answer, or behind a sealed pin), so nothing was compared; the reason is the NOT RUN line (#1818)
# 0 is the only status that means parity. 2 exists so "agrees" is distinguishable from
# "agrees on the parts we check", which is the distinction the epoch fields cost us.
#
# PARITY_MODE=sealed (the default) pins at the nest's sealed_through. PARITY_MODE=head (#1718) pins
# at its last_block, inside the hot window, and needs the subgraph's _meta.block.number at or above
# it; a subgraph or a gateway-routed indexer short of that head is exit 3, not a pass. The pin
# itself must not move during the run: last_block may not fall below it, and the nest's reorg and
# missed-checkpoint counters on /metrics must read the same before and after.
#
# The first run compared incomparable populations (all-time subgraph totals vs
# Horizon-only nest views) and reported four DIFFs that were not row disagreements.
# This version compares:
#   allocations  nest count vs subgraph allocations where isLegacy: false
#   disputes     nest ids vs subgraph disputes where isLegacy: false
#   epochs       field-by-field, each field from its own measured comparability epoch.
#                All six value fields are hard gates; only an exact fee boundary shift
#                (#1819) is reported KNOWN-DIFF, never OK.
#                start/end block are L2 vs L1 and are INCOMPARABLE, not DIFF.
#   escrow       counts, with a type breakdown; ids are different schemes
#
# On disagreement it prints the differing rows, not just a count.
#
# The comparison itself is tools/lodestar-parity, run from beside this script or from PARITY_COMPARE.
set -euo pipefail

# graph-allocations-nest-next on the Lodestar box. 8105 was its predecessor and no longer answers.
NEST=${NEST_URL:-http://127.0.0.1:8107}
BLOCK=${PINNED_BLOCK:-}
MODE=${PARITY_MODE:-sealed}
NETWORK_SG=DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp
GATEWAY=${GRAPH_GATEWAY:-https://gateway-arbitrum.network.thegraph.com}

# The nest indexes Horizon (`subgraph_service__*`) events only, so early epochs describe a different
# population on each side and comparing them is the category error all-time `allocationCount` vs
# Horizon-only allocations already illustrates. **Each field crosses into comparability at its own
# epoch**, and those are measured constants in tools/lodestar-parity rather than one number here - the
# reward trio at 1195, signal at 1105, the two fee fields at 1105. Every boundary asserts both halves
# of its claim: comparable above, and still disagreeing below. A boundary raised until a run goes
# green therefore fails instead.
#
# This is an optional **floor** an operator may set to narrow the window further. It is never a way
# to lower a measured boundary, and it defaults to unset.
EPOCH_PARITY_FROM=${EPOCH_PARITY_FROM:-}
case "${EPOCH_PARITY_FROM:-0}" in
  ''|*[!0-9]*) die_early=1 ;;
esac

die() { printf 'FAIL %s\n' "$*" >&2; exit 1; }
# Truthiness as these checks have always read it: null, false, 0, "", [] and {} all count as absent.
TRUTHY='def truthy: . != null and . != false and . != 0 and . != "" and . != [] and . != {};'
[ -z "${die_early:-}" ] || die "EPOCH_PARITY_FROM must be a decimal epoch, got ${EPOCH_PARITY_FROM}"
case "$MODE" in
  sealed) ;;
  head) [ -z "$BLOCK" ] || die "PINNED_BLOCK cannot be combined with PARITY_MODE=head, which pins at last_block" ;;
  *) die "PARITY_MODE must be sealed or head, got ${MODE}" ;;
esac

if [ -n "$BLOCK" ]; then
  case "$BLOCK" in
    ''|*[!0-9]*) die "PINNED_BLOCK must be a decimal block number, got ${BLOCK}" ;;
  esac
fi

ready=$(curl -fsS -m10 -A 'nuthatch-lodestar-parity' "$NEST/ready" || true)
[ -n "$ready" ] || die "nest at $NEST did not answer /ready"

jq -er "$TRUTHY"' if .ready | truthy then "ready last_block=\(.last_block) sealed_through=\(.sealed_through)" else error("not ready") end' \
  <<<"$ready" || die "nest /ready is not ready"

# Aggregate nest views do not expose historical versions or a reorg generation, so the comparison is
# pinned to the sealed boundary.
#
# Epoch aggregates were previously skipped here as having "no as-of surface". That is not true, and
# #1113 records the measurement. `lodestar_epochs` aggregates with no block filter, but every epoch
# except the newest is immutable at a pin: rewards group by the event's own `currentEpoch`, which is
# monotonic in block, and fees and signal bucket by `block_number` inside a window that closes as
# soon as the successor epoch is first observed. An epoch whose successor is already visible at the
# pin cannot change. Only the newest epoch is open, and it is excluded below.
VERSION=$(jq -r "$TRUTHY"' if .version | truthy then .version else "unknown" end' <<<"$ready")

if [ "$MODE" = head ]; then
  BLOCK=$(jq -er "$TRUTHY"' if .last_block | truthy then .last_block | tonumber | floor else error("no last_block") end' \
    <<<"$ready") || die "last_block is missing so there is no head to pin"
  echo "PINNED_BLOCK set to last_block=$BLOCK (head mode)"
elif [ -z "$BLOCK" ]; then
  BLOCK=$(jq -er "$TRUTHY"' if .sealed_through | truthy then .sealed_through | tonumber | floor else error("no sealed_through") end' \
    <<<"$ready") || die "sealed_through is missing so there is no immutable pin"
  echo "PINNED_BLOCK defaulted to sealed_through=$BLOCK"
fi

read -r _NEST_BLOCK NEST_SEALED <<EOF
$(jq -r 'if .last_block == null or .sealed_through == null then error("no boundary")
  else "\(.last_block | tonumber | floor) \(.sealed_through | tonumber | floor)" end' <<<"$ready")
EOF
[ -n "${NEST_SEALED:-}" ] || die "nest readiness lacks a sealed boundary"
# `-ge`, not `-eq`. What the pin needs is that the block is at or below a sealed boundary, so both
# sides describe immutable history. A pin *older* than the current watermark satisfies that more
# strongly, not less - more of it is settled. Demanding equality made every run a race against the
# sealer, which is a poor property for a check billed as continuous.
if [ "$MODE" = sealed ]; then
  [ "$NEST_SEALED" -ge "$BLOCK" ] || die "PINNED_BLOCK=$BLOCK is above sealed_through=$NEST_SEALED, so it is not settled history"
fi
echo "PIN mode=$MODE block=$BLOCK version=$VERSION"

# "<nuthatch_reorgs_total> <nuthatch_checkpoints_missed_total>" from the nest's /metrics.
reorg_counters() {
  local m r c
  m=$(curl -fsS -m10 -A 'nuthatch-lodestar-parity' "$NEST/metrics" || true)
  r=$(printf '%s\n' "$m" | sed -n 's/^nuthatch_reorgs_total \([0-9][0-9]*\)$/\1/p' | head -n 1)
  c=$(printf '%s\n' "$m" | sed -n 's/^nuthatch_checkpoints_missed_total \([0-9][0-9]*\)$/\1/p' | head -n 1)
  [ -n "$r" ] && [ -n "$c" ] || return 1
  echo "$r $c"
}
if [ "$MODE" = head ]; then
  REORGS_BEFORE=$(reorg_counters) \
    || die "nest /metrics has no nuthatch_reorgs_total and nuthatch_checkpoints_missed_total, so a reorg under the head pin could not be seen"
fi

nest_count() {
  local view="$1" col="$2"
  local q="SELECT count(*) AS n FROM $view WHERE $col <= $BLOCK"
  local body
  body=$(curl -fsS -m30 -A 'nuthatch-lodestar-parity' --get "$NEST/sql" --data-urlencode "q=$q" || true)
  [ -n "$body" ] || die "view $view: nest /sql did not answer"
  jq -er "$TRUTHY"'
    if .error | truthy then error("sql error: \(.error)") else . end
    | (if .rows | truthy then .rows else [] end) as $rows
    | if ($rows | length) == 0 then error("no rows") else $rows[0] end
    | (if .n | truthy then .n elif .count | truthy then .count else 0 end) | tonumber | floor
    | if . <= 0 then error("count is \(.)") else . end' <<<"$body" || die "view ${view} returned no rows"
}

echo "nest lodestar_allocations $(nest_count lodestar_allocations created_at_block)"
echo "nest lodestar_epochs $(nest_count lodestar_epochs start_block)"
echo "nest lodestar_disputes $(nest_count lodestar_disputes created_at_block)"
echo "nest lodestar_escrow_transactions $(nest_count lodestar_escrow_transactions block_number)"

EPOCH_SUMMARY=$(mktemp)
trap 'rm -f "$EPOCH_SUMMARY"' EXIT
export EPOCH_SUMMARY

if [ -z "${GRAPH_API_KEY:-}" ]; then
  die "GRAPH_API_KEY is unset: the subgraph side was not asked, so this is not a comparison"
fi

export ALLOC_N EPOCH_N DISPUTE_N ESCROW_N
ALLOC_N=$(nest_count lodestar_allocations created_at_block)
EPOCH_N=$(nest_count lodestar_epochs start_block)
DISPUTE_N=$(nest_count lodestar_disputes created_at_block)
ESCROW_N=$(nest_count lodestar_escrow_transactions block_number)

export ALLOC_N EPOCH_N DISPUTE_N ESCROW_N BLOCK NETWORK_SG GATEWAY GRAPH_API_KEY NEST EPOCH_PARITY_FROM
export PARITY_MODE=$MODE
cmp_rc=0
COMPARE=${PARITY_COMPARE:-$(dirname "$0")/lodestar-parity-compare}
[ -x "$COMPARE" ] || die "no comparison binary at $COMPARE: build tools/lodestar-parity and set PARITY_COMPARE"
"$COMPARE" || cmp_rc=$?
if [ "$cmp_rc" -eq 3 ] && [ "$MODE" = head ]; then
  echo "parity NOT MEASURED at head $BLOCK: the two sides could not be brought to the same head"
  exit 3
fi
if [ "$cmp_rc" -eq 4 ]; then
  echo "parity NOT RUN at block $BLOCK ($MODE): the subgraph side did not answer, so nothing was compared"
  exit 4
fi
[ "$cmp_rc" -eq 0 ] || die "subgraph comparison failed"

# `/sql` reads the live aggregate. The pre-read sealed boundary is immutable; this post-read check
# rejects any progress, so the comparison cannot mix that sealed snapshot with later hot state.
ready_after=$(curl -fsS -m10 -A 'nuthatch-lodestar-parity' "$NEST/ready" || true)
[ -n "$ready_after" ] || die "nest at $NEST did not answer /ready after comparison"
read -r AFTER_BLOCK AFTER_SEALED <<EOF
$(jq -r "$TRUTHY"' if (.ready | truthy | not) or .last_block == null or .sealed_through == null then error("not ready")
  else "\(.last_block | tonumber | floor) \(.sealed_through | tonumber | floor)" end' <<<"$ready_after")
EOF
[ -n "${AFTER_SEALED:-}" ] || die "nest /ready was not ready after comparison"
# Ordinary forward progress during the run is not a problem: the pin is still sealed, so every read
# above saw settled history. The boundary going *backwards* is the thing worth catching - a reorg
# past the pin, or a nest that lost sealed state - and `-ge` still catches exactly that. Under `-eq`
# this failed a run in which every comparison had already passed, purely because the nest sealed a
# batch while the escrow join was paging.
if [ "$MODE" = sealed ]; then
  [ "$AFTER_SEALED" -ge "$BLOCK" ] || die "sealed boundary went backwards during comparison: $BLOCK -> $AFTER_SEALED"
else
  # The hot window can reorg under the pin, and a rollback that re-ingests leaves heights and counts
  # as they were. The nest's reorg counter is the state identity: any reorg, or any window committed
  # without a checkpoint to detect one, means the reads may span two states.
  if [ "$AFTER_BLOCK" -lt "$BLOCK" ]; then
    echo "parity NOT MEASURED at head $BLOCK: last_block fell to $AFTER_BLOCK during the comparison"
    exit 3
  fi
  REORGS_AFTER=$(reorg_counters) || die "nest /metrics lost its reorg counters during the comparison"
  if [ "$REORGS_AFTER" != "$REORGS_BEFORE" ]; then
    echo "parity NOT MEASURED at head $BLOCK: reorgs and missed checkpoints went from $REORGS_BEFORE to $REORGS_AFTER during the comparison"
    exit 3
  fi
fi
read -r EPOCH_GATED EPOCH_KNOWN < "$EPOCH_SUMMARY"
echo "  proved: allocation counts, dispute id sets, escrow rows joined by id, and the epoch reward trio over ${EPOCH_GATED} closed epochs"
if [ "$EPOCH_KNOWN" = "-" ]; then
  echo "parity CLEAN at block $BLOCK ($MODE)"
  exit 0
fi
# Every gated comparison agreed, and three epoch fields still do not. Reporting that as OK would be
# the same fault this script was rewritten to remove, one level up: a known absence of proof reading
# as proof. Distinct exit status, so an operator can tell "agrees" from "agrees on what we check".
echo "  NOT proved: ${EPOCH_KNOWN} remain KNOWN-DIFF, see #1116, #1114, #1819 and #1983"
echo "parity NOT CLEAN at block $BLOCK ($MODE): gated comparisons agree, known differences outstanding"
exit 2
