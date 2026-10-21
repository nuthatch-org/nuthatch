#!/usr/bin/env bash
# RFC-0071 S0(b). For one nest, counts the rows in its last week of data and the share the fixed
# catalogue of RFC-0071 §4 covers, keyed on topic0 and the indexed pattern of the parameters, never on
# a table's name. Counts come from `nuthatch sql`, so they are the rows S1 will see.
#
#   coverage.sh <nuthatch> <tables.json> <--dir DIR | --url URL> <ts | blocks:N | all>
#
# tables.json is the nest's schema.json or its running instance's /tables (both carry `.tables`).
# `ts` takes the week as 604,800 s back from the newest block_timestamp; `blocks:N` takes the last N
# blocks, for a timestamp-free nest; `all` takes every row. One TSV line per table on stdout, the
# totals on stderr.
set -euo pipefail
BIN=$1 TABLES=$2 SRC_FLAG=$3 SRC=$4 WINDOW=$5

sig() { cast keccak "$1"; }
# kind, topic0, indexed pattern of the non-implicit parameters (t = indexed, f = not)
CATALOGUE=$(cat <<EOF
erc20_transfer	$(sig 'Transfer(address,address,uint256)')	ttf
erc721_transfer	$(sig 'Transfer(address,address,uint256)')	ttt
erc20_approval	$(sig 'Approval(address,address,uint256)')	ttf
erc721_approval	$(sig 'Approval(address,address,uint256)')	ttt
approval_for_all	$(sig 'ApprovalForAll(address,address,bool)')	ttf
erc1155_transfer_single	$(sig 'TransferSingle(address,address,address,uint256,uint256)')	tttff
erc1155_transfer_batch	$(sig 'TransferBatch(address,address,address,uint256[],uint256[])')	tttff
ownership_transferred	$(sig 'OwnershipTransferred(address,address)')	tt
paused	$(sig 'Paused(address)')	f
unpaused	$(sig 'Unpaused(address)')	f
upgraded	$(sig 'Upgraded(address)')	t
admin_changed	$(sig 'AdminChanged(address,address)')	ff
beacon_upgraded	$(sig 'BeaconUpgraded(address)')	t
role_granted	$(sig 'RoleGranted(bytes32,address,address)')	ttt
role_revoked	$(sig 'RoleRevoked(bytes32,address,address)')	ttt
role_admin_changed	$(sig 'RoleAdminChanged(bytes32,bytes32,bytes32)')	ttt
EOF
)
# Not in §4's catalogue; reported apart, to show what adding WETH9's mint and burn would change.
WETH9=$(printf '%s\tt f\n%s\tt f\n' "$(sig 'Deposit(address,uint256)')" "$(sig 'Withdrawal(address,uint256)')")

q() { "$BIN" sql --json "$SRC_FLAG" "$SRC" "$1"; }
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Every table the nest serves, with its kind, topic0 and indexed pattern.
jq -r '.tables[] | [.table, (.kind // "event"), (.topic0 // ""),
        ([.columns[] | select(.sol_type != "implicit") | if .indexed then "t" else "f" end] | join(""))] | @tsv' \
  "$TABLES" > "$WORK/tables"

# One query per table: the planner refuses a union of more than 64. The week's upper bound is the
# newest row the nest holds.
case $WINDOW in ts) COL=block_timestamp ;; *) COL=block_number ;; esac
HEAD=0
while IFS=$'\t' read -r t _ _ _; do
  m=$(q "SELECT coalesce(max($COL), 0) AS m FROM \"$t\"" < /dev/null | jq -r .m)
  [ "$m" -gt "$HEAD" ] && HEAD=$m
done < "$WORK/tables"
case $WINDOW in
  ts) FROM=$((HEAD - 604800)) ;;
  blocks:*) FROM=$((HEAD - ${WINDOW#blocks:} + 1)) ;;
  all) FROM=0 ;;
esac

: > "$WORK/counts"
while IFS=$'\t' read -r t _ _ _; do
  n=$(q "SELECT count(*) AS n FROM \"$t\" WHERE $COL >= $FROM AND $COL <= $HEAD" < /dev/null | jq -r .n)
  printf '%s\t%s\n' "$t" "$n" >> "$WORK/counts"
done < "$WORK/tables"

total=0 covered=0 events=0 covered_events=0 weth=0
while IFS=$'\t' read -r t kind topic0 pattern; do
  n=$(awk -F'\t' -v t="$t" '$1 == t {print $2}' "$WORK/counts")
  n=${n:-0}
  class=fallback
  if [ "$kind" = event ]; then
    hit=$(awk -F'\t' -v k="$topic0" -v p="$pattern" '$2 == k && $3 == p {print $1; exit}' <<< "$CATALOGUE")
    [ -n "$hit" ] && class=$hit
    if [ "$class" = fallback ] && [ "$pattern" = tf ] && grep -q "^$topic0" <<< "$WETH9"; then
      class=weth9_not_in_catalogue; weth=$((weth + n))
    fi
    events=$((events + n))
    [ "$class" != fallback ] && [ "$class" != weth9_not_in_catalogue ] && covered_events=$((covered_events + n))
  else
    class="fallback($kind)"
  fi
  total=$((total + n))
  case $class in fallback*|weth9_not_in_catalogue) ;; *) covered=$((covered + n)) ;; esac
  printf '%s\t%s\t%s\n' "$t" "$n" "$class"
done < "$WORK/tables"

pct() { if [ "$2" -eq 0 ]; then echo "n/a"; else awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f%%", 100*a/b}'; fi; }
{
  echo "window: $COL $FROM..=$HEAD"
  echo "rows: $total (event rows $events); covered by the catalogue: $covered ($(pct "$covered" "$total") of all rows, $(pct "$covered_events" "$events") of event rows)"
  echo "WETH9 Deposit/Withdrawal rows (not in the catalogue): $weth"
} >&2
