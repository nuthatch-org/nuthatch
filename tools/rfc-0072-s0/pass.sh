#!/usr/bin/env bash
# One pass over a nest's days: a fresh `nuthatch serve` per day, so no answer comes from /sql's
# memo and each day's RSS rise is its own. Run twice and compare (compare.sh) for gate 1.
#
# Usage: pass.sh <nuthatch> <nest-dir> <port> <nest-config.json> <out-root> <first-day> <n-days> [grt-bounds.tsv]
# Writes <out-root>/<day>/ (day.sh's files) and <out-root>/pass.tsv:
#   day, statements, seconds (all), seconds (without the 90-day variants), rss before KiB, peak KiB, rise KiB
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
bin=$1 nest=$2 port=$3 cfg=$4 root=$5 first=$6 ndays=$7 btsv=${8:-}
url=http://127.0.0.1:$port
mkdir -p "$root"
: >"$root/pass.tsv"
if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then echo "port $port is taken" >&2; exit 1; fi

rss() { ps -o rss= -p "$1" | tr -d ' '; }
s0=$(jq -n --arg d "$first" '$d + "T00:00:00Z" | fromdate')
for ((i = 0; i < ndays; i++)); do
  day=$(jq -n -r --argjson t $((s0 + i * 86400)) '$t | todate | .[0:10]')
  "$bin" serve --dir "$nest" --listen "127.0.0.1:$port" >"$root/serve-$day.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 600); do curl -sf "$url/" >/dev/null 2>&1 && break; sleep 0.1; done
  curl -sf "$url/" >/dev/null || { echo "serve did not come up for $day" >&2; kill $pid; exit 1; }
  sleep 1
  before=$(rss $pid)
  if [ -w "/proc/$pid/clear_refs" ]; then
    echo 5 >"/proc/$pid/clear_refs" # resets VmHWM, so the peak read below is this day's
    "$here/day.sh" "$url" "$cfg" "$day" "$root/$day" $btsv
    peak=$(awk '/VmHWM/ { print $2 }' "/proc/$pid/status")
  else
    : >"$root/rss-$day.samples"
    (while kill -0 $pid 2>/dev/null; do rss $pid >>"$root/rss-$day.samples"; sleep 0.05; done) &
    sampler=$!
    "$here/day.sh" "$url" "$cfg" "$day" "$root/$day" $btsv
    kill $sampler 2>/dev/null || true
    wait $sampler 2>/dev/null || true
    peak=$(sort -n "$root/rss-$day.samples" | tail -1)
  fi
  kill $pid
  wait $pid 2>/dev/null || true
  awk -v d="$day" -v b="$before" -v p="$peak" '
    { n++; all += $2; if ($1 !~ /_90/) core += $2 }
    END { printf "%s\t%d\t%.3f\t%.3f\t%d\t%d\t%d\n", d, n, all, core, b, p, p - b }' \
    "$root/$day/timings.tsv" >>"$root/pass.tsv"
  tail -1 "$root/pass.tsv"
done
