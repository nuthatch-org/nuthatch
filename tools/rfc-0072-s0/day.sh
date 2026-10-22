#!/usr/bin/env bash
# RFC-0072 S0: one UTC day's signal queries (§5.2) as plain /sql, each run once, answers kept.
#
# Usage: day.sh <base-url> <nest-config.json> <YYYY-MM-DD> <out-dir> [grt-bounds.tsv]
#
# A nest with timestamps reads its days from block_timestamp. A timestamp-free nest has no clock
# (§5.6), so its day and hour boundaries come from a file of first blocks read once from the chain.
#
# /sql refuses a statement with more than 64 UNIONs or over 16 KiB, and Lodestar has 66 tables with
# rows. So a signal runs as one statement per chunk of tables, each answer summing or merging
# exactly with the others (facts.sh merges them). Only the distinct-address record cannot be split.
#
# Writes <out-dir>/<id>.sql, <id>.json (the response), window.json and timings.tsv (id, seconds).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
url=$1 cfg=$2 day=$3 out=$4 btsv=${5:-}
mkdir -p "$out"
: >"$out/timings.tsv"

S=$(jq -n --arg d "$day" '$d + "T00:00:00Z" | fromdate')
E=$((S + 86400))
DN=$((S / 86400))
ZERO=0x0000000000000000000000000000000000000000
ts_mode=$(jq -r .timestamps "$cfg")
xfer=$(jq -r '.transfer[0] // empty' "$cfg")
all_tables=()
while IFS= read -r t; do all_tables+=("$t"); done < <(jq -r '.tables[]' "$cfg")

run() { # id sql
  printf '%s\n' "$2" >"$out/$1.sql"
  local t0 t1
  t0=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  "$here/sql.sh" "$url" "$2" >"$out/$1.json" || { echo "$day $1: $(cat "$out/$1.json")" >&2; exit 1; }
  t1=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  printf '%s\t%s\n' "$1" "$(perl -e "printf '%.3f', $t1 - $t0")" >>"$out/timings.tsv"
  if [ "$(jq -r '.degraded or .tip_unavailable or .truncated' "$out/$1.json")" != false ]; then
    echo "$day $1: degraded, truncated or tip unavailable" >&2
    exit 1
  fi
}
chunks() { # size, then items: prints one line per chunk, items space-separated
  local size=$1 n=0 line="" x
  shift
  for x in "$@"; do
    line+="${line:+ }$x" n=$((n + 1))
    if [ $n -eq "$size" ]; then echo "$line" && line="" n=0; fi
  done
  [ -z "$line" ] || echo "$line"
}

# first_of_kind: every table's first block. A table with none is left out of everything after.
i=0
while read -r -a part; do
  fk=""
  for t in "${part[@]}"; do fk+="${fk:+ UNION ALL }SELECT '$t' AS tbl, MIN(block_number) AS first_block FROM \"$t\""; done
  i=$((i + 1)) && run "first_of_kind.c$i" "SELECT tbl, first_block FROM ($fk) ORDER BY tbl"
done < <(chunks 40 "${all_tables[@]}")
tables=()
while IFS= read -r t; do tables+=("$t"); done < <(jq -s -r '[.[].rows[] | select(.first_block != null) | .tbl] | sort[]' "$out"/first_of_kind.c*.json)
active=$(printf '%s\n' "${tables[@]}" | jq -R . | jq -s .)
role_tables=()
while IFS= read -r t; do role_tables+=("$t"); done < <(jq -r --argjson act "$active" \
  '(.identifiers // []) as $id | [.roles[] | select(.col as $c | $id | index($c) | not) | .table | select(. as $t | $act | index($t))] | unique[]' "$cfg")

if [ "$ts_mode" = true ]; then
  EXTRA=", block_timestamp"
  DX="(CAST(FLOOR(CAST(block_timestamp AS DOUBLE) / 86400) AS BIGINT) - $DN)"
  HR="CAST(FLOOR(CAST(CAST(block_timestamp AS BIGINT) - $S AS DOUBLE) / 3600) AS BIGINT)"
  TS="CAST(block_timestamp AS BIGINT)"
else
  EXTRA=""
  fb() { awk -v t="$1" '$1 == t { print $2 }' "$btsv"; }
  DX="CASE"
  for k in $(seq 0 -1 -89); do DX+=" WHEN block_number >= $(fb $((S + k * 86400))) THEN $k"; done
  DX+=" ELSE -90 END"
  HR="CASE"
  for h in $(seq 23 -1 1); do HR+=" WHEN block_number >= $(fb $((S + h * 3600))) THEN $h"; done
  HR+=" ELSE 0 END"
  TS="block_number"
fi

evmap() { # id where select tail: one statement per chunk of 40 tables, over every event row
  local k=0 s t
  while read -r -a part; do
    s=""
    for t in "${part[@]}"; do s+="${s:+ UNION ALL }SELECT '$t' AS tbl, block_number, log_index$EXTRA FROM \"$t\" WHERE $2"; done
    k=$((k + 1))
    run "$1.c$k" "SELECT $3 FROM (SELECT tbl, block_number, log_index, $DX AS dx, $HR AS hr, $TS AS ts FROM ($s)) $4"
  done < <(chunks 40 "${tables[@]}")
}
roles() { # mode lo hi tables...: the union of their address parameters
  local mode=$1 lo=$2 hi=$3 tabs
  shift 3
  tabs=$(printf '%s\n' "$@" | jq -R . | jq -s .)
  jq -r --argjson tabs "$tabs" --arg mode "$mode" --arg lo "$lo" --arg hi "$hi" --arg extra "$EXTRA" -f "$here/roles.jq" "$cfg"
}
rmap() { # id mode lo hi select tail: one statement per chunk of 25 tables with address parameters
  local id=$1 mode=$2 lo=$3 hi=$4 sel=$5 tail=$6 k=0 cols
  if [ "$mode" = role ]; then cols="u['r'] AS role, u['a'] AS a"; else cols="a"; fi
  while read -r -a part; do
    k=$((k + 1))
    run "${id}.c$k" "SELECT $sel FROM (SELECT $cols, $DX AS dx FROM ($(roles "$mode" "$lo" "$hi" "${part[@]}"))) $tail"
  done < <(chunks 25 "${role_tables[@]}")
}

# Block bounds of the window.
if [ "$ts_mode" = true ]; then
  evmap bounds "block_timestamp >= $((S - 90 * 86400)) AND block_timestamp < $E" \
    "dx, MIN(block_number) AS lo, MAX(block_number) AS hi" "GROUP BY dx ORDER BY dx"
  lo_at() { jq -s -r --argjson k "$1" '[.[].rows[] | select(.dx >= $k) | .lo] | min' "$out"/bounds.c*.json; }
  b0=$(lo_at 0)
  b1=$(jq -s -r '[.[].rows[] | select(.dx == 0) | .hi] | max' "$out"/bounds.c*.json)
  lo7=$(lo_at -7) lo30=$(lo_at -30) lo90=$(lo_at -90)
else
  b0=$(fb $S) b1=$(($(fb $E) - 1))
  lo7=$(fb $((S - 7 * 86400))) lo30=$(fb $((S - 30 * 86400))) lo90=$(fb $((S - 90 * 86400)))
fi
for v in b0 b1 lo7 lo30 lo90; do [[ ${!v} =~ ^[0-9]+$ ]] || { echo "$day: no bound $v" >&2; exit 1; }; done
jq -n --arg day "$day" --argjson S $S --argjson b0 $b0 --argjson b1 $b1 --argjson lo7 $lo7 \
  --argjson lo30 $lo30 --argjson lo90 $lo90 --arg clock "$ts_mode" \
  '{day: $day, start: $S, blocks: [$b0, $b1], lo7: $lo7, lo30: $lo30, lo90: $lo90, timestamps: ($clock == "true")}' >"$out/window.json"

record() { # id, a CTE m(dx, n)
  run "$1" "WITH m AS ($2),
r AS (SELECT dx, n, ROW_NUMBER() OVER (ORDER BY n DESC, dx ASC) AS rk FROM m WHERE dx < 0)
SELECT (SELECT n FROM m WHERE dx = 0) AS day, n AS prev_max, dx AS prev_max_dx,
  (SELECT COUNT(*) FROM m WHERE dx < 0) AS days_before FROM r WHERE rk = 1"
}

# activity and unusual: rows per table per day over D-30..D (medians are taken in facts.sh).
evmap daily_counts "block_number BETWEEN $lo30 AND $b1" "tbl, dx, COUNT(*) AS n" "GROUP BY tbl, dx ORDER BY tbl, dx"
evmap busiest_hour "block_number BETWEEN $b0 AND $b1" "hr, COUNT(*) AS n" "GROUP BY hr ORDER BY hr"
evmap quiet_spell "block_number BETWEEN $b0 AND $b1" \
  "ts, MIN(block_number) AS first_block, MAX(block_number) AS last_block" "GROUP BY ts ORDER BY ts"
# A full-history record needs every earlier day, and a timestamp-free nest has no clock to bucket them by.
[ "$ts_mode" != true ] || evmap records_events "block_number BETWEEN 0 AND $b1" "dx, COUNT(*) AS n" "GROUP BY dx ORDER BY dx"
evmap records_events_90 "block_number BETWEEN $lo90 AND $b1" "dx, COUNT(*) AS n" "GROUP BY dx ORDER BY dx"

if [ -n "$xfer" ]; then
  X="\"$xfer\""
  run biggest_mover "WITH m AS (
  SELECT address AS token, \"from\" AS a, -value_dec AS v FROM $X WHERE block_number BETWEEN $b0 AND $b1
  UNION ALL SELECT address AS token, \"to\" AS a, value_dec AS v FROM $X WHERE block_number BETWEEN $b0 AND $b1)
SELECT token, a, SUM(v) AS net, COUNT(*) AS legs FROM m WHERE a <> '$ZERO' GROUP BY token, a
ORDER BY ABS(SUM(v)) DESC, a ASC LIMIT 1"

  run largest_transfer "SELECT address AS token, value_dec, \"from\", \"to\", tx_hash, block_number, log_index FROM $X
WHERE block_number BETWEEN $b0 AND $b1 ORDER BY value_dec DESC, block_number ASC, log_index ASC LIMIT 1"

  newcp() { # id history-lo
    # An address is new when its first appearance in the window falls in D: a GROUP BY, not a join,
    # since a hash join of the day against 13 million history rows ran out of the 512 MB budget.
    run "$1" "WITH d AS (SELECT a, SUM(v) AS volume, COUNT(*) AS legs FROM (
    SELECT \"from\" AS a, value_dec AS v FROM $X WHERE block_number BETWEEN $b0 AND $b1
    UNION ALL SELECT \"to\" AS a, value_dec AS v FROM $X WHERE block_number BETWEEN $b0 AND $b1) WHERE a <> '$ZERO' GROUP BY a),
f AS (SELECT a FROM (SELECT \"from\" AS a, block_number FROM $X WHERE block_number BETWEEN $2 AND $b1
    UNION ALL SELECT \"to\" AS a, block_number FROM $X WHERE block_number BETWEEN $2 AND $b1)
  GROUP BY a HAVING MIN(block_number) >= $b0),
nw AS (SELECT d.a, d.volume, d.legs FROM d JOIN f ON d.a = f.a)
SELECT a, volume, legs, (SELECT COUNT(*) FROM nw) AS new, (SELECT COUNT(*) FROM d) AS active
FROM nw ORDER BY volume DESC, a ASC LIMIT 3"
  }
  newcp new_counterparties 0
  newcp new_counterparties_90 "$lo90"
  xv() { echo "SELECT $DX AS dx, value_dec AS v, \"from\" AS f, \"to\" AS t FROM $X WHERE block_number BETWEEN $1 AND $b1"; }
  recs() { # suffix history-lo
    record "records_volume$1" "SELECT dx, SUM(v) AS n FROM ($(xv "$2")) GROUP BY dx"
    record "records_addresses$1" "SELECT dx, COUNT(DISTINCT a) AS n FROM (SELECT dx, f AS a FROM ($(xv "$2")) UNION ALL SELECT dx, t AS a FROM ($(xv "$2"))) WHERE a <> '$ZERO' GROUP BY dx"
  }
  [ "$ts_mode" != true ] || recs "" 0
  recs _90 "$lo90"
else
  rmap new_counterparties_day role "$b0" "$b1" "role, a, COUNT(*) AS n" "WHERE a <> '$ZERO' GROUP BY role, a ORDER BY role, a"
  ncp_seen() { # suffix history-lo: which of the day's addresses appear before it
    local k=0 list a
    while read -r -a part; do
      list=""
      for a in "${part[@]}"; do list+="${list:+, }'$a'"; done
      k=$((k + 1))
      rmap "new_counterparties_seen$1.a$k" plain "$2" "$((b0 - 1))" "DISTINCT a" "WHERE a IN ($list) ORDER BY a"
    done < <(chunks 150 $(jq -s -r '[.[].rows[].a] | unique[]' "$out"/new_counterparties_day.c*.json))
  }
  ncp_seen "" 0
  ncp_seen _90 "$lo90"
  record records_addresses "SELECT dx, COUNT(DISTINCT a) AS n FROM (SELECT a, $DX AS dx FROM ($(roles plain 0 "$b1" "${role_tables[@]}"))) WHERE a <> '$ZERO' GROUP BY dx"
  record records_addresses_90 "SELECT dx, COUNT(DISTINCT a) AS n FROM (SELECT a, $DX AS dx FROM ($(roles plain "$lo90" "$b1" "${role_tables[@]}"))) WHERE a <> '$ZERO' GROUP BY dx"
fi
