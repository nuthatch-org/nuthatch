#!/usr/bin/env bash
# RFC-0080 second S0: the first S0's filter (JSON rows, readers woken inline) and the second's
# (Arrow columns, a delivery task) run interleaved on one box, one shape set after another, then a
# run with stalled clients.
#
#   BIN=target/release/nuthatch bash tools/rfc-0080-s0/ab.sh <out-dir> [sets...]
set -uo pipefail

OUT="${1:?usage: ab.sh <out-dir> [sets...]}"
shift
SETS=("$@")
[ ${#SETS[@]} -gt 0 ] || SETS=(none uniform skew hot:100 hot:1000 all-tables stall)
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$OUT"

run() {
  local set="$1" name="$2" filter="$3" delivery="$4"
  local shapes=10000 mode="$set" blocks=300 stall=0
  case "$set" in
    none) shapes=0 mode=uniform ;;
    hot:*) blocks=150 ;;
    stall) mode=hot:100 blocks=150 stall=50 ;;
  esac
  local dir="$OUT/${set//:/-}-$name"
  echo "== $set $name $(uptime)"
  SHAPES=$shapes MODE=$mode TIP_BLOCKS=$blocks STALL=$stall FILTER=$filter DELIVERY=$delivery \
    bash "$HERE/run.sh" "$dir" >"$dir.log" 2>&1 || echo "FAIL: $set $name, see $dir.log"
}

for set in "${SETS[@]}"; do
  if [ "$set" = stall ]; then
    run stall new columns task
    continue
  fi
  run "$set" old json inline
  run "$set" new columns task
done
echo "== done $(uptime)"
