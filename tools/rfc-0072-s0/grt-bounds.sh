#!/usr/bin/env bash
# Day boundaries from 2026-06-24 to 2026-10-07 and hour boundaries for the 14 report days, as
# first blocks on Arbitrum One, for the timestamp-free GRT supply nest. Writes grt-bounds.tsv.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
rpc=${RPC:-https://arb1.arbitrum.io/rpc}
anchor_b=512624648 anchor_t=1791389898 # a block and its timestamp, read once from $rpc

day0=$(date -u -j -f "%Y-%m-%d %H:%M:%S" "2026-06-24 00:00:00" +%s)
rep0=$(date -u -j -f "%Y-%m-%d %H:%M:%S" "2026-09-23 00:00:00" +%s)
targets=()
for ((t = day0; t <= rep0 + 14 * 86400; t += 86400)); do targets+=("$t"); done
for ((t = rep0; t < rep0 + 14 * 86400; t += 3600)); do
  [ $(((t - rep0) % 86400)) -eq 0 ] || targets+=("$t")
done

for t in "${targets[@]}"; do
  d=$((t - anchor_t))
  est=$((anchor_b + d * 4))
  m=$((${d#-} + 400000))
  echo "$t $((est - m)) $((est + m))"
done | xargs -P 8 -n 3 "$here/first-block-at.sh" "$rpc" | sort -n >"$here/grt-bounds.tsv"
wc -l <"$here/grt-bounds.tsv"
