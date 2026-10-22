# RFC-0072 S0: throwaway templates. Input: an array of facts documents for one nest, in day order.
# Output: an HTML fragment, one <article> per day. Every number is copied from the facts.

def thou: tostring | if test("^-?[0-9]+$") then
  (if startswith("-") then "-" else "" end) as $s | ltrimstr("-")
  | (explode | reverse | implode) as $r
  | $s + ([range(0; $r | length; 3) as $i | $r[$i:$i + 3]] | join(",") | explode | reverse | implode)
  else . end;
def num: if type == "number" and . != floor then tostring else thou end;
def code: "<code>" + (tostring | @html) + "</code>";
def tbl: code;
def dur: if . >= 3600 then "\(. / 3600 | floor) h \((. % 3600) / 60 | floor) min"
  elif . >= 60 then "\(. / 60 | floor) min \(. % 60) s" else "\(.) s" end;
def clock: todate | .[11:19];
def pct($a; $b): if $b == 0 then "n/a" else "\(($a * 100 / $b) | round)%" end;
def list: if length <= 1 then (.[0] // "")
  else (.[0:-1] | join(", ")) + " and " + .[-1] end;

def report:
  .facts as $f | .timestamps as $clock
  | ($f.activity.all) as $a
  | [
      # activity: always said, as the headline
      "<p class=lead>\($a.day | thou) events, \(pct($a.day; $a.median_30)) of the 30-day median of \($a.median_30 | num) a day (7-day median \($a.median_7 | num)).</p>",
      (if ($f.activity.tables | length) > 0 then
        "<li>Most rows: " + ([$f.activity.tables[0:3][] | "\(.table | tbl) \(.day | thou)"] | list) + ".</li>" else empty end),
      ([$f.unusual[] | select(.table != "(all events)" and .day >= 3 * .median_7)] as $up
       | if ($up | length) > 0 then "<li>Well above its 7-day median: "
           + ([$up[] | "\(.table | tbl) \(.day | thou) (median \(.median_7 | num))"] | list) + ".</li>" else empty end),
      ([$f.unusual[] | select(.table != "(all events)" and .day < 3 * .median_7)] as $dn
       | if ($dn | length) > 0 then "<li>Well below its 7-day median: "
           + ([$dn[] | "\(.table | tbl) \(.day | thou) (median \(.median_7 | num))"] | list) + ".</li>" else empty end),
      ($f.busiest_hour | if . then
        "<li>Busiest hour \(.hour | tostring | if length == 1 then "0" + . else . end):00 UTC: \(.events | thou) of \(.total | thou) events (\(.share)%).</li>"
        else empty end),
      ($f.quiet_spell | if .fired then
        if $clock then
          "<li>Silent for \(.gap | dur), \(.start | clock) to \(.end | clock) UTC"
          + (if .after_block then " (after block \(.after_block | thou)" else " (from midnight" end)
          + (if .before_block then ", before block \(.before_block | thou))" else ", to midnight)" end)
          + "; the day's median gap between events was \(.median_gap | dur).</li>"
        else
          "<li>Silent for \(.gap | thou) blocks, about \(.start_time | clock) to \(.end_time | clock) UTC"
          + (if .after_block then " (after block \(.after_block | thou)" else " (from the day's first block" end)
          + (if .before_block then ", before block \(.before_block | thou))" else ", to its last)" end)
          + "; the median gap between events was \(.median_gap | num) blocks. Times are placed between hour boundaries, since this nest has no clock.</li>"
        end else empty end),
      (if ($f.first_of_kind | length) > 0 then
        "<li>First rows ever: " + ([$f.first_of_kind[] | "\(.tbl | tbl) (block \(.first_block | thou))"] | list) + ".</li>" else empty end),
      ($f.new_counterparties | if .history.by_role then
        (.history) as $h | (.last_90_days) as $n
        | if $h.new > 0 then
            "<li>\($h.new) of the day's \($h.active) addresses appeared for the first time in the nest's history ("
            + ([$h.by_role[] | select(.new > 0) | "\(.new) as \(.role)"] | list) + "): "
            + ([$h.by_role[] | select(.new > 0) | .top[] | "\(.a | code) as \(.role) (\(.n) row\(if .n == 1 then "" else "s" end))"] | .[0:5] | list)
            + (if $n.new != $h.new then ". \($n.new) were new to the last 90 days." else "." end) + "</li>"
          elif $n.new > 0 then "<li>No address was new to the nest, but \($n.new) were new to the last 90 days.</li>"
          else empty end
        else
          (.history) as $h | (.last_90_days) as $n
          | if $h.new > 0 then
              "<li>\($h.new | thou) of the day's \($h.active | thou) addresses moved the token for the first time; the largest newcomer, \($h.top[0].a | code), moved \($h.top[0].volume) in \($h.top[0].legs) transfer\(if $h.top[0].legs == 1 then "" else "s" end)"
              + (if $n.new != $h.new then ". \($n.new | thou) were new to the last 90 days." else "." end) + "</li>"
            else empty end
        end),
      ($f.biggest_mover | if . then
        "<li>Largest net change: \(.address | code), \(if (.net | startswith("-")) then "" else "+" end)\(.net) over \(.legs) transfer\(if .legs == 1 then "" else "s" end).</li>" else empty end),
      ($f.largest_transfer | if . then
        "<li>Largest single transfer: \(.value) from \(.from | code) to \(.to | code), block \(.block_number | thou), tx \(.tx_hash | code).</li>" else empty end),
      ($f.records[] | . as $r
        | (if $r.metric == "events" then "events" elif $r.metric == "addresses" then "distinct active addresses" else "transfer volume (base units)" end) as $what
        | if ($r.history // {}).fired then
            "<li>Record: \($r.history.day | thou) \($what), above every earlier day (previous high \($r.history.prev_max | thou) on \($r.history.prev_max_day)).</li>"
          elif $r.last_90_days.fired then
            "<li>Highest in 90 days: \($r.last_90_days.day | thou) \($what) (previous high \($r.last_90_days.prev_max | thou) on \($r.last_90_days.prev_max_day)).</li>"
          else empty end)
    ] as $lines
  | ([
      "activity",
      (if ($f.unusual | map(select(.table != "(all events)")) | length) > 0 then "unusual (\($f.unusual | map(select(.table != "(all events)")) | length))" else empty end),
      (if ($f.unusual | map(select(.table == "(all events)")) | length) > 0 then "unusual (all events)" else empty end),
      (if $f.busiest_hour then "busiest_hour" else empty end),
      (if $f.quiet_spell.fired then "quiet_spell" else empty end),
      (if ($f.first_of_kind | length) > 0 then "first_of_kind (\($f.first_of_kind | length))" else empty end),
      (if $f.new_counterparties.history.new > 0 then "new_counterparties (\($f.new_counterparties.history.new))"
       elif $f.new_counterparties.last_90_days.new > 0 then "new_counterparties, 90 days only (\($f.new_counterparties.last_90_days.new))" else empty end),
      (if $f.biggest_mover then "biggest_mover" else empty end),
      (if $f.largest_transfer then "largest_transfer" else empty end),
      ($f.records[] | if (.history // {}).fired then "records (\(.metric))" elif .last_90_days.fired then "records, 90 days (\(.metric))" else empty end)
    ] | join(", ")) as $note
  | "<article id=\"\(.nest)-\(.day)\"><h3>\(.day)</h3>"
    + "<p class=note>Signals: \($note | @html). Blocks \(.blocks[0] | thou) to \(.blocks[1] | thou).</p>"
    + $lines[0] + "<ul>" + ($lines[1:] | join("")) + "</ul></article>";

map(report) | join("\n")
