#!/usr/bin/env bash
# Linux only: one day's statements replayed in order on a fresh `nuthatch serve`. Prints each
# statement's time and its own peak RSS rise (VmHWM cleared before it), then the set's: time, and
# peak over the whole replay less RSS before the first statement.
# Usage: stmt-rss.sh <nuthatch> <nest-dir> <port> <day-dir> [ids-to-skip-regex]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
bin=$1 nest=$2 port=$3 dir=$4 skip=${5:-^$}
url=http://127.0.0.1:$port
"$bin" serve --dir "$nest" --listen "127.0.0.1:$port" >/dev/null 2>&1 &
pid=$!
for _ in $(seq 1 600); do curl -sf "$url/" >/dev/null 2>&1 && break; sleep 0.1; done
sleep 1
start=$(awk '/VmRSS/ { print $2 }' "/proc/$pid/status")
set_peak=$start total=0
while read -r id; do
  before=$(awk '/VmRSS/ { print $2 }' "/proc/$pid/status")
  echo 5 >"/proc/$pid/clear_refs"
  t0=$(date +%s.%N)
  "$here/sql.sh" "$url" "$(cat "$dir/$id.sql")" >/dev/null
  t1=$(date +%s.%N)
  peak=$(awk '/VmHWM/ { print $2 }' "/proc/$pid/status")
  [ "$peak" -le "$set_peak" ] || set_peak=$peak
  s=$(echo "$t1 $t0" | awk '{ printf "%.3f", $1 - $2 }')
  total=$(echo "$total $s" | awk '{ printf "%.3f", $1 + $2 }')
  printf '%s\t%s\t%d\n' "$id" "$s" $((peak - before))
done < <(cut -f1 "$dir/timings.tsv" | grep -E -v "$skip")
printf 'SET\t%s\t%d\n' "$total" $((set_peak - start))
kill $pid
wait $pid 2>/dev/null || true
