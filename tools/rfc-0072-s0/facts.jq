# RFC-0072 S0: merge one day's answers into a facts document (§5.3), deterministically.
# Input: an array of {id, rows} from day.sh. Args: $w (window.json), $nest, $meta (metadata.json),
# $shas ({statement id: sha256 of its SQL}), $hb (sorted [unix time, first block] pairs, or null).

def sig($name): [.[] | select(.id | sub("(\\.a[0-9]+)?\\.c[0-9]+$"; "") == $name) | .rows[]];
def present($name): any(.[]; .id | sub("(\\.a[0-9]+)?\\.c[0-9]+$"; "") == $name);
def median: sort | length as $n | if $n == 0 then null
  elif $n % 2 == 1 then .[($n - 1) / 2] else (.[$n / 2 - 1] + .[$n / 2]) / 2 end;
def day_of($dx): ($w.start + $dx * 86400) | todate | .[0:10];

# An integer string in base units as a decimal string with thousands separators. Exact: strings only.
def group3: (. | explode | reverse | implode) as $r
  | [range(0; $r | length; 3) as $i | $r[$i:$i + 3]] | join(",") | explode | reverse | implode;
def units($dec):
  tostring | (if startswith("-") then "-" else "" end) as $sign | ltrimstr("-")
  | (("0" * ([0, $dec + 1 - length] | max)) + .) as $p
  | ($p[0:($p | length) - $dec]) as $int | ($p[($p | length) - $dec:] | sub("0+$"; "")) as $frac
  | $sign + ($int | group3) + (if $frac == "" then "" else "." + $frac end);
def amount($token): ($meta[$token] // {}) as $m
  | if $m.decimals then (units($m.decimals) + " " + ($m.symbol // "tokens"))
    else (units(0) + " base units (decimals unknown)") end;
# Exact comparison of two non-negative integers that may be decimal strings past 2^53.
def gt($a; $b): ($a | tostring) as $x | ($b | tostring) as $y
  | ($x | length) > ($y | length) or (($x | length) == ($y | length) and $x > $y);
def thou:tostring | if test("^[0-9]+$") then group3 else . end;

# A timestamp-free nest's block, placed in time between the two hour boundaries around it.
def block_time($b): if $hb == null then $b else
  ([$hb[] | select(.[1] <= $b)] | last) as $lo | ([$hb[] | select(.[1] > $b)] | first) as $hi
  | if $lo == null or $hi == null then null
    else ($lo[0] + (($b - $lo[1]) * ($hi[0] - $lo[0]) / ($hi[1] - $lo[1])) | floor) end end;
def clock($t): $t | todate | .[11:19];

. as $in
| $w.blocks as [$b0, $b1]
| ($in | sig("daily_counts")) as $dc
| ([$dc[].tbl] | unique) as $tabs
| ($tabs | map(. as $t | {key: $t, value: ($dc | map(select(.tbl == $t)))}) | from_entries) as $bytab
| def daily($rows; $dx): ([$rows[] | select(.dx == $dx) | .n] | add // 0);
  def stats($rows): {
    day: daily($rows; 0),
    median_7: ([range(-7; 0) as $k | daily($rows; $k)] | median),
    median_30: ([range(-30; 0) as $k | daily($rows; $k)] | median)
  };
  ([$tabs[] | {table: .} + stats($bytab[.])]) as $per
| (stats($dc)) as $all

| ($in | sig("busiest_hour") | group_by(.hr) | map({hr: .[0].hr, n: (map(.n) | add)})) as $hours
| ($hours | sort_by(-.n, .hr) | first) as $bh

| ($in | sig("quiet_spell") | group_by(.ts)
   | map({ts: .[0].ts, first: (map(.first_block) | min), last: (map(.last_block) | max)})) as $pts
| (if $w.timestamps then [$w.start, $w.start + 86400] else [$b0, $b1 + 1] end) as [$edge0, $edge1]
| ([{ts: $edge0, first: null, last: null}] + $pts + [{ts: $edge1, first: null, last: null}]) as $seq
| ([range(1; $seq | length) as $i
    | {start: $seq[$i - 1].ts, end: $seq[$i].ts, after_block: $seq[$i - 1].last, before_block: $seq[$i].first,
       gap: ($seq[$i].ts - $seq[$i - 1].ts)}]) as $gaps
| ($gaps | map(.gap) | median) as $medgap
| ($gaps | sort_by(-.gap, .start) | first) as $qs

| ([$in | sig("first_of_kind")[] | select(.first_block != null and .first_block >= $b0 and .first_block <= $b1)]
   | sort_by(.tbl)) as $fok

| def recmerge($name): ($in | sig($name) | group_by(.dx) | map({dx: .[0].dx, n: (map(.n) | add)})) as $d
    | ([$d[] | select(.dx == 0) | .n] | first) as $day
    | ([$d[] | select(.dx < 0)] | sort_by(-.n, .dx) | first) as $pm
    | {day: $day, prev_max: $pm.n, prev_max_day: (if $pm then day_of($pm.dx) else null end),
       days_before: ([$d[] | select(.dx < 0)] | length), fired: ($day != null and ($pm == null or $day > $pm.n))};
  def recone($name): ($in | sig($name) | first) as $r
    | if $r == null then null else
      {day: $r.day, prev_max: $r.prev_max, prev_max_day: day_of($r.prev_max_dx), days_before: $r.days_before,
       fired: gt($r.day; $r.prev_max)} end;

  {
    nest: $nest, day: $w.day, blocks: $w.blocks, timestamps: $w.timestamps, weather_version: 0,
    facts: ({
      activity: {all: $all, tables: ($per | map(select(.day > 0)) | sort_by(-.day, .table))},
      unusual: [($per + [{table: "(all events)"} + $all])[]
                | select(.median_7 >= 20 and (.day >= 3 * .median_7 or 3 * .day <= .median_7))],
      busiest_hour: (if $bh then {hour: $bh.hr, events: $bh.n, total: ($hours | map(.n) | add),
                       share: ((($bh.n * 1000 / ($hours | map(.n) | add)) | round) / 10)} else null end),
      quiet_spell: ($qs + {median_gap: $medgap, unit: (if $w.timestamps then "seconds" else "blocks" end),
                    fired: ($medgap != null and $qs.gap > 4 * $medgap)}
                    + (if $w.timestamps then {} else {start_time: block_time($qs.start), end_time: block_time($qs.end)} end)),
      first_of_kind: $fok,
      records: ([("events", "addresses", "volume") as $m
        | (if $m == "events" then (if present("records_events") then recmerge("records_events") else null end)
           else recone("records_" + $m) end) as $full
        | (if $m == "events" then recmerge("records_events_90") else recone("records_" + $m + "_90") end) as $r90
        | select($r90 != null)
        | {metric: $m, history: $full, last_90_days: $r90}])
    } + (if present("new_counterparties_day") then
      ($in | sig("new_counterparties_day") | group_by([.role, .a]) | map({role: .[0].role, a: .[0].a, n: (map(.n) | add)})) as $cand
      | def newcp($seenname): ([$in | sig($seenname)[] | .a] | unique) as $seen
          | ([$cand[] | select(.a as $a | $seen | index($a) | not)]) as $nw
          | {active: ([$cand[].a] | unique | length), new: ([$nw[].a] | unique | length),
             by_role: ($cand | group_by(.role) | map({role: .[0].role, active: length,
               new: ([.[] | select(.a as $a | $nw | map(.a) | index($a))] | length),
               top: ([.[] | select(.a as $a | $nw | map(.a) | index($a))] | sort_by(-.n, .a) | .[0:3])}))};
        {new_counterparties: {history: newcp("new_counterparties_seen"), last_90_days: newcp("new_counterparties_seen_90")}}
    else
      def newcp($name): ($in | sig($name)) as $r
        | {active: ($r[0].active // null), new: ($r[0].new // 0),
           top: [$r[] | {a, volume: (.volume | amount($token)), legs}]};
      {new_counterparties: {history: newcp("new_counterparties"), last_90_days: newcp("new_counterparties_90")},
       biggest_mover: ($in | sig("biggest_mover") | first | if . then .token as $t | {address: .a, token: .token,
         net: (.net | amount($t)), legs} else null end),
       largest_transfer: ($in | sig("largest_transfer") | first | if . then .token as $t | {token, value: (.value_dec | amount($t)),
         from, to, tx_hash, block_number, log_index} else null end)}
    end)),
    queries: $shas
  }
