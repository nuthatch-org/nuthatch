#!/usr/bin/env bash
#
# RFC-0069 S1 gate: every number on `nuthatch watch`'s line equals the endpoint's value in the same
# read. Runs the line mode against a live nest or runtime with --record, then recomputes each field
# from the recorded bodies with jq, independently of the client's own parser, and compares.
#
# usage: line-gate.sh <nuthatch-binary> <url> <seconds> <out-dir>
set -euo pipefail

BIN="$1"
URL="$2"
SECS="$3"
OUT="$4"
mkdir -p "$OUT"
rm -f "$OUT/reads.jsonl" "$OUT/lines.txt"

"$BIN" watch --url "$URL" --interval 2 --record "$OUT/reads.jsonl" --exit-after "$SECS" \
  | cat > "$OUT/lines.txt"

jq -n -r --slurpfile reads "$OUT/reads.jsonl" --rawfile lines "$OUT/lines.txt" '
  def ns: "\"NO SIGNAL\"";
  def logfmt: [scan("([a-z_]+)=(\"[^\"]*\"|[^ ]+)") | {(.[0]): .[1]}] | add;
  def metric($body; $series):
    [$body | split("\n")[] | select(startswith($series + " ")) | split(" ") | .[-1] | tonumber]
    | first // null;
  def labelled($body; $name):
    [$body | split("\n")[] | select(startswith($name + "{"))
      | capture("^[a-z_]+\\{(?<labels>[^}]*)\\} (?<v>[^ ]+)$")
      | {labels: .labels, v: (.v | tonumber)}];
  def endpoints($body):
    [labelled($body; "nuthatch_rpc_endpoint_requests_total")[]
      | .labels | capture("endpoint=\"(?<e>[^\"]*)\"").e] | unique;
  def failures($body; $e):
    [labelled($body; "nuthatch_rpc_endpoint_failures_total")[]
      | select(.labels | contains("endpoint=\"" + $e + "\"")) | .v] | first // null;
  def num($v): if $v == null then ns else ($v | tostring) end;
  def fmt_name($v): if $v == null or $v == "" then ns else $v end;

  ($reads | group_by(.cycle) | map({key: (.[0].cycle | tostring), value: .}) | from_entries) as $by
  | ($lines | split("\n") | map(select(length > 0)) | map(logfmt)) as $parsed
  | [ range(1; ($by | length) + 1) as $c
      | $by[$c | tostring] as $rs
      | ($rs | map({key: .route, value: .}) | from_entries) as $r
      | ($by[($c - 1) | tostring] // [] | map(select(.route == "/metrics")) | first) as $pm
      | $r["/metrics"] as $m
      | (if $r["/nests"].status == 200 then ($r["/nests"].body | fromjson | .nests) else null end) as $mounts
      | (if $mounts == null then
           [{ nest: ([labelled($m.body; "nuthatch_nest_last_block")[]
                      | .labels | capture("nest=\"(?<n>[^\"]*)\"").n] | first),
              chain: (if $r["/"].status == 200 then ($r["/"].body | fromjson | .chain) else null end),
              ready: $r["/ready"] }]
         else
           [$mounts[] | . as $mt | { nest: .name, chain: .chain, ready: $r["/" + $mt.name + "/ready"] }]
         end) as $want
      | (if $r["/nests"].status == 404 then 1
         elif $mounts != null then ([$mounts[].chain] | unique | length)
         else null end) as $cursors
      | (if $m.status == 200 then metric($m.body; "nuthatch_rss_bytes") else null end) as $rss
      | (if $pm != null and $pm.status == 200 and $m.status == 200 then
           (metric($m.body; "nuthatch_http_requests_total") - metric($pm.body; "nuthatch_http_requests_total"))
           / (($m.t_ms - $pm.t_ms) / 1000)
         else null end) as $rps
      | (if $pm != null and $pm.status == 200 and $m.status == 200 then
           endpoints($m.body) as $es
           | if ($es | length) == 0 then null else
               ([$es[] | select(failures($m.body; .) > failures($pm.body; .))] | length) as $failing
               | "\(($es | length) - $failing)/\($es | length)"
             end
         else null end) as $rpc
      | $want | to_entries[]
      | .value as $w
      | ($w.ready.body | try fromjson catch null) as $rb
      | { cycle: $c,
          expect: {
            nest: fmt_name($w.nest),
            chain: fmt_name($w.chain),
            tip: num(if ($rb.tip // 0) > 0 then $rb.tip else null end),
            lag: num(if ($rb.tip // 0) > 0 then $rb.lag_blocks else null end),
            sealed: num(if ($rb.sealed_through // 0) > 0 then $rb.sealed_through else null end),
            rss: num($rss),
            budget: num(if $cursors == null then null else $cursors * 2147483648 end),
            rpc_up: (if $rpc == null then ns else $rpc end),
            http_rps: $rps } }
    ] as $expected
  | if ($expected | length) != ($parsed | length) then
      "FAIL: \($expected | length) expected lines, \($parsed | length) printed\n" | halt_error(1)
    else . end
  | [ range(0; $expected | length) as $i
      | $expected[$i] as $e | $parsed[$i] as $p
      | $e.expect | to_entries[]
      | .key as $k
      | if $k == "http_rps" then
          (if .value == null then ($p[$k] == ns)
           else (($p[$k] | tonumber? // -1) - .value | fabs) <= 0.05 end) as $ok
          | {k: $k, ok: $ok, want: (.value // ns), got: $p[$k], cycle: $e.cycle}
        else {k: $k, ok: ($p[$k] == .value), want: .value, got: $p[$k], cycle: $e.cycle} end
    ] as $checks
  | ($checks | map(select(.ok | not))) as $bad
  | "cycles \($by | length), lines \($parsed | length), fields compared \($checks | length), " +
    "of them NO SIGNAL \($checks | map(select(.got == ns)) | length), mismatches \($bad | length)",
    ($bad[] | "  MISMATCH cycle \(.cycle) \(.k): line \(.got), endpoint \(.want)"),
    (if ($bad | length) > 0 then "FAIL\n" | halt_error(1) else "PASS" end)
'
