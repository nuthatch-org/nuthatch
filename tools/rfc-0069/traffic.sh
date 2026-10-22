#!/usr/bin/env bash
#
# RFC-0069 S5: a few callers' worth of reads against a defi-pulse nest, so DISPATCH has requests to
# show: `/sql` over its own tables, `/tables`, `/entities`, and a `/table/{name}` that does not exist.
#
# usage: traffic.sh <base-url> <seconds>
base="$1"; end=$(( $(date +%s) + $2 ))
qs=(
  "SELECT count(*) FROM usdc__mint"
  "SELECT block_number, amount FROM usdc__mint ORDER BY block_number DESC LIMIT 5"
  "SELECT count(*) FROM eth_usd__answer_updated"
  "SELECT count(*) FROM steth__submitted"
)
while [ "$(date +%s)" -lt "$end" ]; do
  q="${qs[$((RANDOM % ${#qs[@]}))]}"
  curl -s -o /dev/null -G "$base/sql" --data-urlencode "q=$q"
  curl -s -o /dev/null "$base/tables"
  [ $((RANDOM % 3)) -eq 0 ] && curl -s -o /dev/null "$base/entities?limit=5"
  [ $((RANDOM % 5)) -eq 0 ] && curl -s -o /dev/null "$base/table/no_such_table"
  sleep "0.$((RANDOM % 9 + 1))"
done
